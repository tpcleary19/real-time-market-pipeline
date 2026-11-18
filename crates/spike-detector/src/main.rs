use chrono::{Local, Utc};
use rdkafka::{
    consumer::{CommitMode, Consumer},
    message::Message,
    producer::Producer,
};
use spike_detector::{
    config::Settings,
    detector::Detector,
    kafka::{self, Parsed},
    metrics::Metrics,
};
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
    });

    let consumer = kafka::build_consumer(&settings)?;
    let producer = kafka::build_producer(&settings)?;
    let mut detector = Detector::new(settings.detector_config());
    let mut metrics = Metrics::default();

    kafka::assign_and_warm_up(
        &consumer,
        &settings,
        &mut detector,
        &mut metrics.next_offsets,
    )
    .await?;
    eprintln!(
        "{} spike-detector consuming {}",
        Local::now(),
        settings.source_topic
    );

    let mut tick = tokio::time::interval(Duration::from_secs(settings.metrics_interval_seconds));
    tick.tick().await;

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
                match kafka::parse_trade(msg.payload(), now, settings.max_future_skew_seconds) {
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
                                    // Serialization of a finite-valued event should not fail; never skip silently.
                                    eprintln!("{} cannot serialize event {}: {e}", Local::now(), event.event_id);
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
                        eprintln!("{} Future-dated trade rejected partition={} offset={}", Local::now(), msg.partition(), msg.offset());
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
    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) => {}
        Err(e) => eprintln!("{} final commit: {e}", Local::now()),
    }
    let _ = producer.flush(Duration::from_secs(10));
    metrics.report(
        &detector.stats(),
        detector.symbol_count(),
        detector.retained_trades(),
        None,
    );
    Ok(())
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
