use chrono::{Local, Utc};
use rdkafka::{
    consumer::{CommitMode, Consumer},
    message::Message,
    producer::Producer,
};
use spike_detector::{
    config::Settings,
    detector::Detector,
    kafka::{self, Parsed, WarmUp},
    metrics::Metrics,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::from_env()?;
    eprintln!("{} spike-detector starting: {settings:?}", Local::now());

    let (stop_tx, mut stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_signal().await;
        let _ = stop_tx.send(true);
        // A second signal forces an immediate (non-zero) exit. Nothing unsafe is lost:
        // offsets are only stored after delivery, so the worst case is a replay.
        wait_for_signal().await;
        eprintln!("{} second signal: forcing exit", Local::now());
        std::process::exit(130);
    });

    let consumer = Arc::new(kafka::build_consumer(&settings)?);
    let producer = kafka::build_producer(&settings)?;
    let mut detector = Detector::new(settings.detector_config());
    let mut metrics = Metrics::default();

    match kafka::assign_and_warm_up(
        &consumer,
        &settings,
        &mut detector,
        &mut metrics.next_offsets,
        &mut stop_rx,
    )
    .await
    {
        Ok(WarmUp::Ready { since_ms }) => {
            let s2 = settings.clone();
            match tokio::task::spawn_blocking(move || kafka::read_cooldown_seeds(&s2, since_ms))
                .await
            {
                Ok(Ok(read)) => {
                    // Seeds from the future (bogus or foreign events) would suppress real
                    // spikes: ignore anything beyond the replay's newest event time + skew.
                    let reference = detector.max_event_time().unwrap_or_else(Utc::now);
                    let (kept, ignored) = kafka::filter_seeds(
                        read.seeds,
                        reference,
                        settings.max_future_skew_seconds,
                    );
                    for (sym, dir, at) in &ignored {
                        eprintln!(
                            "{} WARNING ignoring cooldown seed {} {} at {at}: later than replay time {reference} + {}s",
                            Local::now(),
                            kafka::trunc(sym, 40),
                            dir.as_str(),
                            settings.max_future_skew_seconds
                        );
                    }
                    if read.complete {
                        // Exact record of what was published: authoritative.
                        detector.reset_cooldowns();
                    }
                    for (symbol, dir, at) in &kept {
                        if read.reached_end {
                            // Newest published event of this pair: exact, beats the replay phase.
                            detector.override_cooldown(symbol, *dir, *at);
                        } else {
                            // Budget hit before the end: the newest events may be unread.
                            detector.seed_cooldown(symbol, *dir, *at);
                        }
                    }
                    eprintln!(
                        "{} cooldown seeded: {} pair(s) used, {} ignored (future), {} foreign-schema events skipped, mode={}",
                        Local::now(),
                        kept.len(),
                        ignored.len(),
                        read.foreign_schema,
                        if read.complete {
                            "authoritative"
                        } else if read.reached_end {
                            "per-pair override, others keep replay-derived (older records skipped)"
                        } else {
                            "max-merge with replay-derived (budget hit before the end)"
                        }
                    );
                }
                Ok(Err(e)) => eprintln!(
                    "{} WARNING cooldown not seeded from {}: {e}; using replay-derived cooldowns",
                    Local::now(),
                    settings.output_topic
                ),
                Err(e) => eprintln!("{} WARNING cooldown seeding task failed: {e}", Local::now()),
            }
        }
        Ok(WarmUp::Shutdown) => {
            eprintln!("{} shutdown requested during startup", Local::now());
            return Ok(());
        }
        Err(e) => {
            eprintln!("{} FATAL startup failure: {e}", Local::now());
            std::process::exit(1);
        }
    }
    eprintln!(
        "{} spike-detector consuming {}",
        Local::now(),
        settings.source_topic
    );

    let mut tick = tokio::time::interval(Duration::from_secs(settings.metrics_interval_seconds));
    tick.tick().await;

    let mut failed = false;
    let mut future_warn = spike_detector::metrics::FutureWarn::default();
    'main: loop {
        tokio::select! {
            _ = stop_rx.changed() => break,
            _ = tick.tick() => {
                let lag = consumer_lag(&consumer, &settings.source_topic, &metrics);
                metrics.report(&detector.stats(), detector.symbol_count(), detector.retained_trades(), lag);
            }
            received = consumer.recv() => {
                let msg = match received {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("{} consumer error: {e}", Local::now());
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let started = Instant::now();
                metrics.consumed += 1;
                let now = Utc::now();
                match kafka::parse_trade(
                    msg.payload(),
                    now,
                    settings.max_future_skew_seconds,
                ) {
                    Parsed::Trade(trade) => {
                        for event in detector.observe(&trade, now) {
                            eprintln!(
                                "{} SPIKE symbol={} dir={} change_pct={} event_id={}",
                                Local::now(), event.symbol, event.direction.as_str(),
                                event.percentage_change, event.event_id
                            );
                            match kafka::publish(&producer, &settings.output_topic, &event, &mut stop_rx, &mut metrics.publish_errors).await {
                                Ok(true) => metrics.published += 1,
                                // Shutdown mid-publish: leave the offset unstored so the record replays.
                                Ok(false) => break 'main,
                                Err(e) => {
                                    // Never skip: leave the offset unstored so a restart replays the record.
                                    eprintln!("{} FATAL {e}; exiting without storing offset", Local::now());
                                    failed = true;
                                    break 'main;
                                }
                            }
                        }
                    }
                    Parsed::Malformed => {
                        metrics.malformed += 1;
                        eprintln!("{} Invalid trade partition={} offset={}", Local::now(), msg.partition(), msg.offset());
                    }
                    Parsed::Future => {
                        metrics.rejected_future += 1;
                        let drift = msg
                            .payload()
                            .and_then(|p| serde_json::from_slice::<market_events::Trade>(p).ok())
                            .map(|t| (t.timestamp - now).num_seconds());
                        future_warn.record(
                            drift,
                            settings.max_future_skew_seconds,
                            msg.partition(),
                            msg.offset(),
                        );
                    }
                }
                // Only now is the record fully handled (any spikes are acknowledged).
                if let Err(e) = consumer.store_offset_from_message(&msg) {
                    eprintln!("{} store_offset failed: {e}", Local::now());
                }
                metrics.next_offsets.insert(msg.partition(), msg.offset() + 1);
                metrics.record_latency(started.elapsed());
            }
        }
    }

    eprintln!("{} shutting down", Local::now());
    // Bounded: a hung broker must not block shutdown. If the commit is lost the records simply replay.
    let c2 = Arc::clone(&consumer);
    let commit = tokio::task::spawn_blocking(move || c2.commit_consumer_state(CommitMode::Sync));
    match tokio::time::timeout(Duration::from_secs(10), commit).await {
        Ok(Ok(Ok(()))) => {}
        // Nothing stored since the last commit: not an error.
        Ok(Ok(Err(e)))
            if e.rdkafka_error_code() == Some(rdkafka::error::RDKafkaErrorCode::NoOffset) => {}
        Ok(Ok(Err(e))) => eprintln!("{} final commit: {e}", Local::now()),
        Ok(Err(e)) => eprintln!("{} final commit task failed: {e}", Local::now()),
        Err(_) => eprintln!(
            "{} WARNING final commit timed out after 10s; exiting (records will replay)",
            Local::now()
        ),
    }
    let _ = tokio::task::block_in_place(|| producer.flush(Duration::from_secs(5)));
    metrics.report(
        &detector.stats(),
        detector.symbol_count(),
        detector.retained_trades(),
        None,
    );
    // Explicit exit: a timed-out blocking commit must not keep the runtime alive.
    std::process::exit(if failed { 1 } else { 0 });
}

fn consumer_lag(
    consumer: &rdkafka::consumer::StreamConsumer,
    topic: &str,
    metrics: &Metrics,
) -> Option<i64> {
    if metrics.next_offsets.is_empty() {
        return None;
    }
    tokio::task::block_in_place(|| {
        let mut total = 0;
        for (&p, &next) in &metrics.next_offsets {
            let (_, high) = consumer
                .fetch_watermarks(topic, p, Duration::from_secs(2))
                .ok()?;
            total += (high - next).max(0);
        }
        Some(total)
    })
}

async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
