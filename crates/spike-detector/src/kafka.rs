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
    error::{KafkaError, RDKafkaErrorCode},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use std::time::Instant;
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

/// Why a publish was abandoned without acknowledgement.
#[derive(Debug)]
pub enum PublishFailure {
    Serialize(serde_json::Error),
    /// Retrying cannot succeed (size, auth, fatal producer state, topic never appears).
    NonRetriable(String),
}

impl std::fmt::Display for PublishFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize(e) => write!(f, "serialization failed: {e}"),
            Self::NonRetriable(m) => write!(f, "non-retriable publish error: {m}"),
        }
    }
}

impl std::error::Error for PublishFailure {}

fn non_retriable(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::*;
    matches!(
        code,
        MessageSizeTooLarge
            | InvalidMessageSize
            | TopicAuthorizationFailed
            | ClusterAuthorizationFailed
            | Authentication
            | SaslAuthenticationFailed
            | InvalidTopic
            | Fatal
            | ProducerFenced
            | TransactionCoordinatorFenced
    )
}

/// An unknown topic is retried this many consecutive times (2 s apart) before giving up.
const MAX_UNKNOWN_TOPIC_RETRIES: u32 = 30;

/// Publish one event, retrying transient failures until acknowledged.
/// Returns `Ok(true)` when delivered, `Ok(false)` if shutdown was requested first (the caller
/// must NOT store the source offset in that case) and `Err` for failures that retrying cannot
/// fix (the caller must exit non-zero without storing the offset). `errors` counts failed attempts.
pub async fn publish(
    producer: &FutureProducer,
    topic: &str,
    event: &SpikeEvent,
    shutdown: &mut watch::Receiver<bool>,
    errors: &mut u64,
) -> Result<bool, PublishFailure> {
    let payload = serde_json::to_vec(event).map_err(PublishFailure::Serialize)?;
    let mut unknown_topic = 0u32;
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
                    if let Some((code, msg)) = producer.client().fatal_error() {
                        return Err(PublishFailure::NonRetriable(format!("fatal producer error {code}: {msg}")));
                    }
                    let code = e.rdkafka_error_code();
                    if code.is_some_and(non_retriable) {
                        return Err(PublishFailure::NonRetriable(format!("{e} (event_id={})", event.event_id)));
                    }
                    if matches!(code, Some(RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::UnknownTopicOrPartition)) {
                        unknown_topic += 1;
                        if unknown_topic > MAX_UNKNOWN_TOPIC_RETRIES {
                            return Err(PublishFailure::NonRetriable(format!("topic {topic} still unknown after {MAX_UNKNOWN_TOPIC_RETRIES} retries: {e}")));
                        }
                    } else {
                        unknown_topic = 0;
                    }
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

/// Outcome of startup: either ready to consume or a shutdown signal arrived first.
pub enum WarmUp {
    Ready,
    Shutdown,
}

/// How long to wait for the source topic to have partitions before giving up.
const PARTITION_DISCOVERY_DEADLINE: Duration = Duration::from_secs(60);
/// Warm-up gives up (continuing with partial state) after this long without progress.
const WARM_UP_STALL: Duration = Duration::from_secs(15);

async fn discover_partitions(
    consumer: &StreamConsumer,
    topic: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Vec<i32>>, String> {
    let started = Instant::now();
    loop {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(5)) {
            Ok(meta) => {
                let parts: Vec<i32> = meta
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map(|t| t.partitions().iter().map(|p| p.id()).collect())
                    .unwrap_or_default();
                if !parts.is_empty() {
                    return Ok(Some(parts));
                }
                eprintln!(
                    "{} source topic {topic} has no partitions yet; retrying",
                    chrono::Local::now()
                );
            }
            Err(e) => eprintln!(
                "{} metadata fetch failed: {e}; retrying",
                chrono::Local::now()
            ),
        }
        if started.elapsed() > PARTITION_DISCOVERY_DEADLINE {
            return Err(format!(
                "source topic {topic} has no partitions (or broker unreachable) after {}s",
                PARTITION_DISCOVERY_DEADLINE.as_secs()
            ));
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(None),
            _ = tokio::time::sleep(Duration::from_secs(3)) => {}
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
/// Partitions without a committed offset resolve to a concrete start (`auto_offset_reset`)
/// with empty state. Partitions not being replayed are paused during warm-up, and at the end
/// EVERY partition is re-seeked to its resume offset, so nothing read during warm-up is lost.
/// Errors if the topic has no partitions after a bounded wait.
pub async fn assign_and_warm_up(
    consumer: &StreamConsumer,
    s: &Settings,
    detector: &mut Detector,
    next_offsets: &mut BTreeMap<i32, i64>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<WarmUp, String> {
    let topic = s.source_topic.as_str();
    let t10 = Duration::from_secs(10);
    let e = |what: &str, err: KafkaError| format!("{what}: {err}");

    let Some(partitions) = discover_partitions(consumer, topic, shutdown).await? else {
        return Ok(WarmUp::Shutdown);
    };
    let mut all = TopicPartitionList::new();
    for p in &partitions {
        all.add_partition(topic, *p);
    }
    let committed = consumer
        .committed_offsets(all, t10)
        .map_err(|x| e("committed offsets", x))?;

    // partition -> offset at which normal processing resumes
    let mut resume: BTreeMap<i32, i64> = BTreeMap::new();
    // partition -> committed offset C (exclusive end of the replay range)
    let mut pending: HashMap<i32, i64> = HashMap::new();
    let mut assign = TopicPartitionList::new();
    let lookback_ms = ((s.window_seconds + s.cooldown_seconds + 60) * 1000) as i64;
    for el in committed.elements() {
        let p = el.partition();
        let Offset::Offset(c) = el.offset() else {
            let (low, high) = consumer
                .fetch_watermarks(topic, p, t10)
                .map_err(|x| e("watermarks", x))?;
            let start = if s.auto_offset_reset == "earliest" {
                low
            } else {
                high
            };
            resume.insert(p, start);
            assign
                .add_partition_offset(topic, p, Offset::Offset(start))
                .map_err(|x| e("assign", x))?;
            continue;
        };
        resume.insert(p, c);
        let warm_start = if c > 0 {
            warm_start_offset(consumer, topic, p, c, lookback_ms).await
        } else {
            None
        };
        match warm_start {
            Some(w) if w < c => {
                pending.insert(p, c);
                assign.add_partition_offset(topic, p, Offset::Offset(w))
            }
            _ => assign.add_partition_offset(topic, p, Offset::Offset(c)),
        }
        .map_err(|x| e("assign", x))?;
    }
    consumer.assign(&assign).map_err(|x| e("assign", x))?;
    *next_offsets = resume.clone();
    if pending.is_empty() {
        return Ok(WarmUp::Ready);
    }
    // Only replayed partitions should deliver records during warm-up.
    let mut idle = TopicPartitionList::new();
    for p in resume.keys().filter(|p| !pending.contains_key(p)) {
        idle.add_partition(topic, *p);
    }
    if idle.count() > 0 {
        consumer.pause(&idle).map_err(|x| e("pause", x))?;
    }
    eprintln!(
        "{} warm-up: replaying {} partition(s) up to {:?}",
        chrono::Local::now(),
        pending.len(),
        pending
    );

    let mut replayed = 0u64;
    let mut last_progress = Instant::now();
    while !pending.is_empty() {
        let received = tokio::select! {
            _ = shutdown.changed() => return Ok(WarmUp::Shutdown),
            r = tokio::time::timeout(WARM_UP_STALL, consumer.recv()) => r,
        };
        let msg = match received {
            Ok(Ok(m)) => {
                last_progress = Instant::now();
                m
            }
            Ok(Err(err)) => {
                eprintln!("{} warm-up recv error: {err}", chrono::Local::now());
                if last_progress.elapsed() > WARM_UP_STALL {
                    eprintln!(
                        "{} WARNING warm-up stalled with partitions pending {pending:?}; continuing with partial state",
                        chrono::Local::now()
                    );
                    break;
                }
                tokio::select! {
                    _ = shutdown.changed() => return Ok(WarmUp::Shutdown),
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
                continue;
            }
            Err(_) => {
                eprintln!(
                    "{} WARNING warm-up timed out with partitions pending {pending:?}; continuing with partial state",
                    chrono::Local::now()
                );
                break;
            }
        };
        let (p, off) = (msg.partition(), msg.offset());
        let Some(&c) = pending.get(&p) else {
            continue; // re-seeked below
        };
        if off < c {
            // Use the record's own time as "now" so the future-skew check matches the original run.
            let reference = msg
                .timestamp()
                .to_millis()
                .and_then(DateTime::from_timestamp_millis)
                .unwrap_or_else(Utc::now);
            if let Parsed::Trade(t) =
                parse_trade(msg.payload(), reference, s.max_future_skew_seconds)
            {
                detector.observe(&t, Utc::now()); // events intentionally discarded
            }
            replayed += 1;
        }
        if off + 1 >= c {
            pending.remove(&p);
            let mut one = TopicPartitionList::new();
            one.add_partition(topic, p);
            consumer.pause(&one).map_err(|x| e("pause", x))?;
        }
    }
    // Put EVERY partition back at its resume offset and unpause, whatever was read meanwhile.
    let mut every = TopicPartitionList::new();
    for (&p, &r) in &resume {
        consumer
            .seek(topic, p, Offset::Offset(r), t10)
            .map_err(|x| e("seek", x))?;
        every.add_partition(topic, p);
    }
    consumer.resume(&every).map_err(|x| e("resume", x))?;
    eprintln!(
        "{} warm-up complete: replayed={replayed} symbols={} retained_trades={}",
        chrono::Local::now(),
        detector.symbol_count(),
        detector.retained_trades()
    );
    Ok(WarmUp::Ready)
}

/// Offset to start replaying from for partition `p` whose committed offset is `c`.
/// Logs a warning and returns `None` (start at `c` with empty state) on any failure.
async fn warm_start_offset(
    consumer: &StreamConsumer,
    topic: &str,
    p: i32,
    c: i64,
    lookback_ms: i64,
) -> Option<i64> {
    let warn = |why: String| {
        eprintln!(
            "{} WARNING warm-up skipped for partition {p} (committed {c}): {why}; starting with empty state",
            chrono::Local::now()
        );
    };
    let mut one = TopicPartitionList::new();
    if let Err(x) = one.add_partition_offset(topic, p, Offset::Offset(c - 1)) {
        warn(x.to_string());
        return None;
    }
    if let Err(x) = consumer.assign(&one) {
        warn(format!("assign: {x}"));
        return None;
    }
    let msg = match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
        Ok(Ok(m)) => m,
        Ok(Err(x)) => {
            warn(format!("cannot read record {}: {x}", c - 1));
            return None;
        }
        Err(_) => {
            warn(format!("timed out reading record {}", c - 1));
            return None;
        }
    };
    let Some(ts) = msg.timestamp().to_millis() else {
        warn("record has no timestamp".into());
        return None;
    };
    let mut q = TopicPartitionList::new();
    if let Err(x) = q.add_partition_offset(topic, p, Offset::Offset(ts - lookback_ms)) {
        warn(x.to_string());
        return None;
    }
    let res = match consumer.offsets_for_times(q, Duration::from_secs(10)) {
        Ok(r) => r,
        Err(x) => {
            warn(format!("offsets_for_times: {x}"));
            return None;
        }
    };
    match res.elements().first().map(|el| el.offset()) {
        Some(Offset::Offset(w)) => Some(w.min(c)),
        // No record at/after the target time: nothing earlier to replay beyond the last one.
        Some(Offset::End) => Some(c - 1),
        other => {
            warn(format!("unexpected offsets_for_times result {other:?}"));
            None
        }
    }
}
