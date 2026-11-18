//! Kafka I/O: consumer/producer construction, publish-with-retry, parsing, restart warm-up.
//!
//! Delivery model (at-least-once): a source offset is stored only after every spike derived
//! from that record has been acknowledged by the broker (acks=all, idempotent producer).
use crate::config::Settings;
use crate::detector::Detector;
use crate::event::SpikeEvent;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use market_events::Trade;
use rdkafka::{
    Offset, TopicPartitionList,
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

pub fn build_consumer(s: &Settings) -> Result<StreamConsumer, KafkaError> {
    ClientConfig::new()
        .set("group.id", &s.group_id)
        .set("bootstrap.servers", &s.brokers)
        .set("auto.offset.reset", &s.auto_offset_reset)
        // Offsets are stored explicitly, only after downstream publication succeeds.
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.commit.interval.ms", "5000")
        .create()
}

pub fn build_producer(s: &Settings) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &s.brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "30000")
        .create()
}

pub enum Parsed {
    Trade(Trade),
    Malformed,
    /// Timestamp too far in the future; accepting it would poison the symbol's window.
    Future,
}

pub fn parse_trade(payload: Option<&[u8]>, now: DateTime<Utc>, max_skew_secs: i64) -> Parsed {
    let Some(bytes) = payload else {
        return Parsed::Malformed;
    };
    match serde_json::from_slice::<Trade>(bytes) {
        Ok(t) if t.timestamp > now + ChronoDuration::seconds(max_skew_secs) => Parsed::Future,
        Ok(t) => Parsed::Trade(t),
        Err(_) => Parsed::Malformed,
    }
}

/// Publish one event, retrying transient failures until acknowledged.
/// Returns `Ok(true)` when delivered, `Ok(false)` if shutdown was requested first (the caller
/// must NOT store the source offset in that case). `errors` counts failed attempts.
pub async fn publish(
    producer: &FutureProducer,
    topic: &str,
    event: &SpikeEvent,
    shutdown: &mut watch::Receiver<bool>,
    errors: &mut u64,
) -> Result<bool, serde_json::Error> {
    let payload = serde_json::to_vec(event)?;
    loop {
        let record = FutureRecord::to(topic)
            .key(event.symbol.as_str())
            .payload(&payload);
        tokio::select! {
            _ = shutdown.changed() => return Ok(false),
            res = producer.send(record, Duration::from_secs(5)) => match res {
                Ok(_) => return Ok(true),
                Err((e, _)) => {
                    *errors += 1;
                    eprintln!("{} publish failed event_id={} err={e}; retrying in 2s",
                        chrono::Local::now(), event.event_id);
                }
            }
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(false),
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
        if *shutdown.borrow() {
            return Ok(false);
        }
    }
}

/// Assign all partitions of the source topic (single-instance deployment) and rebuild
/// detector state after a restart.
///
/// For each partition with a committed offset `C`, replay records from roughly
/// `ts(C-1) - (window + cooldown) - 60s` up to (not including) `C` into the detector and
/// DISCARD any spikes: every record below `C` was fully handled before the offset was stored,
/// so its spikes were already delivered. Records from `C` on are then processed normally.
/// Partitions without a committed offset start per `auto_offset_reset` with empty state.
pub async fn assign_and_warm_up(
    consumer: &StreamConsumer,
    s: &Settings,
    detector: &mut Detector,
    next_offsets: &mut std::collections::BTreeMap<i32, i64>,
) -> Result<(), KafkaError> {
    let topic = s.source_topic.as_str();
    let t10 = Duration::from_secs(10);
    let meta = consumer.fetch_metadata(Some(topic), t10)?;
    let partitions: Vec<i32> = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().iter().map(|p| p.id()).collect())
        .unwrap_or_default();
    if partitions.is_empty() {
        eprintln!(
            "{} source topic {topic} has no partitions yet; starting without warm-up",
            chrono::Local::now()
        );
    }

    let mut all = TopicPartitionList::new();
    for p in &partitions {
        all.add_partition(topic, *p);
    }
    let committed = consumer.committed_offsets(all, t10)?;

    // partition -> committed offset C (exclusive end of warm-up range)
    let mut pending: HashMap<i32, i64> = HashMap::new();
    let mut assign = TopicPartitionList::new();
    let lookback_ms = ((s.window_seconds + s.cooldown_seconds + 60) * 1000) as i64;
    for e in committed.elements() {
        let p = e.partition();
        let Offset::Offset(c) = e.offset() else {
            let start = if s.auto_offset_reset == "earliest" {
                Offset::Beginning
            } else {
                Offset::End
            };
            assign.add_partition_offset(topic, p, start)?;
            continue;
        };
        let warm_start = if c > 0 {
            warm_start_offset(consumer, topic, p, c, lookback_ms).await
        } else {
            None
        };
        match warm_start {
            Some(w) if w < c => {
                pending.insert(p, c);
                assign.add_partition_offset(topic, p, Offset::Offset(w))?;
            }
            _ => assign.add_partition_offset(topic, p, Offset::Offset(c))?,
        }
    }
    consumer.assign(&assign)?;
    if pending.is_empty() {
        return Ok(());
    }
    eprintln!(
        "{} warm-up: replaying {} partition(s) before resuming {:?}",
        chrono::Local::now(),
        pending.len(),
        pending
    );

    let mut replayed = 0u64;
    let mut paused = TopicPartitionList::new();
    while !pending.is_empty() {
        let msg = match tokio::time::timeout(Duration::from_secs(15), consumer.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => {
                eprintln!("{} warm-up recv error: {e}", chrono::Local::now());
                continue;
            }
            Err(_) => {
                eprintln!(
                    "{} warm-up timed out with partitions pending {:?}; continuing with partial state",
                    chrono::Local::now(),
                    pending
                );
                break;
            }
        };
        let (p, off) = (msg.partition(), msg.offset());
        let Some(&c) = pending.get(&p) else {
            // Straggler from a partition that already finished warm-up: rewind to C.
            continue;
        };
        if off < c {
            if let Parsed::Trade(t) =
                parse_trade(msg.payload(), Utc::now(), s.max_future_skew_seconds)
            {
                detector.observe(&t, Utc::now()); // events intentionally discarded
            }
            replayed += 1;
        }
        if off + 1 >= c {
            pending.remove(&p);
            paused.add_partition(topic, p);
            consumer.pause(&paused)?;
            if off >= c {
                consumer.seek(topic, p, Offset::Offset(c), t10)?;
            }
        }
    }
    consumer.resume(&paused)?;
    // Records >= C buffered before pause are discarded by pause; re-seek to be certain.
    for e in committed.elements() {
        if let Offset::Offset(c) = e.offset() {
            let _ = consumer.seek(topic, e.partition(), Offset::Offset(c), t10);
        }
    }
    let _ = next_offsets; // positions are tracked as live records are handled
    eprintln!(
        "{} warm-up complete: replayed={replayed} symbols={} retained_trades={}",
        chrono::Local::now(),
        detector.symbol_count(),
        detector.retained_trades()
    );
    Ok(())
}

/// Offset to start replaying from for partition `p` whose committed offset is `c`.
async fn warm_start_offset(
    consumer: &StreamConsumer,
    topic: &str,
    p: i32,
    c: i64,
    lookback_ms: i64,
) -> Option<i64> {
    let mut one = TopicPartitionList::new();
    one.add_partition_offset(topic, p, Offset::Offset(c - 1))
        .ok()?;
    consumer.assign(&one).ok()?;
    let msg = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .ok()?
        .ok()?;
    let ts = msg.timestamp().to_millis()?;
    let mut q = TopicPartitionList::new();
    q.add_partition_offset(topic, p, Offset::Offset(ts - lookback_ms))
        .ok()?;
    let res = consumer
        .offsets_for_times(q, Duration::from_secs(10))
        .ok()?;
    match res.elements().first()?.offset() {
        Offset::Offset(w) => Some(w.min(c)),
        // No record at/after the target time: nothing earlier to replay beyond the last one.
        Offset::End => Some(c - 1),
        _ => None,
    }
}
