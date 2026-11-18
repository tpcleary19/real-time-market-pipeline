//! Broker-backed integration tests. They run the real `spike-detector` binary against a live
//! Kafka, so they exercise main.rs (consume -> publish -> store offset) and kafka.rs warm-up.
//!
//! Run (broker on localhost:19092, or set SPIKE_IT_BROKERS):
//!   SPIKE_IT_BROKERS=localhost:19092 cargo test -p spike-detector --test kafka_integration -- --ignored --test-threads=4
//! Each test uses unique topics and consumer group and deletes its topics afterwards.
mod common;
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use spike_detector::{Direction, SpikeEvent};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn brokers() -> String {
    std::env::var("SPIKE_IT_BROKERS").unwrap_or_else(|_| "localhost:19092".into())
}

static N: AtomicU32 = AtomicU32::new(0);

struct Env {
    src: String,
    out: String,
    group: String,
}

impl Env {
    async fn new(tag: &str) -> Env {
        Self::with_partitions(tag, 1).await
    }
    async fn with_partitions(tag: &str, parts: i32) -> Env {
        let id = format!(
            "{tag}-{}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst),
            chrono::Utc::now().timestamp_millis()
        );
        let e = Env { src: format!("it-src-{id}"), out: format!("it-out-{id}"), group: format!("it-grp-{id}") };
        let admin: AdminClient<DefaultClientContext> =
            ClientConfig::new().set("bootstrap.servers", brokers()).create().unwrap();
        for (t, n) in [(&e.src, parts), (&e.out, 1)] {
            let nt = NewTopic::new(t, n, TopicReplication::Fixed(1));
            let r = admin.create_topics([&nt], &AdminOptions::new()).await.expect("create topic");
            r[0].as_ref().unwrap_or_else(|x| panic!("create {t}: {x:?}"));
        }
        e
    }
    async fn teardown(&self) {
        let admin: AdminClient<DefaultClientContext> =
            ClientConfig::new().set("bootstrap.servers", brokers()).create().unwrap();
        let _ = admin.delete_topics(&[self.src.as_str(), self.out.as_str()], &AdminOptions::new()).await;
    }
    fn spawn(&self, out_topic: &str) -> Child {
        Command::new(env!("CARGO_BIN_EXE_spike-detector"))
            .env("KAFKA_BROKERS", brokers())
            .env("SPIKE_SOURCE_TOPIC", &self.src)
            .env("SPIKE_OUTPUT_TOPIC", out_topic)
            .env("SPIKE_CONSUMER_GROUP", &self.group)
            .env("SPIKE_AUTO_OFFSET_RESET", "earliest")
            .env("SPIKE_METRICS_INTERVAL_SECONDS", "3600")
            .stdout(Stdio::null())
            .stderr(if std::env::var("SPIKE_IT_LOG").is_ok() { Stdio::inherit() } else { Stdio::null() })
            .spawn()
            .expect("spawn spike-detector")
    }
    fn committed(&self) -> Option<i64> {
        self.committed_p(0)
    }
    fn committed_p(&self, part: i32) -> Option<i64> {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers())
            .set("group.id", &self.group)
            .create()
            .unwrap();
        let mut l = TopicPartitionList::new();
        l.add_partition(&self.src, part);
        match c.committed_offsets(l, Duration::from_secs(10)).unwrap().elements()[0].offset() {
            Offset::Offset(o) => Some(o),
            _ => None,
        }
    }
}

/// Graceful stop (SIGTERM) so the service commits stored offsets; returns exit success.
fn stop(mut c: Child) -> bool {
    let _ = Command::new("kill").arg("-TERM").arg(c.id().to_string()).status();
    let start = Instant::now();
    loop {
        if let Some(s) = c.try_wait().unwrap() {
            return s.success();
        }
        if start.elapsed() > Duration::from_secs(40) {
            let _ = c.kill();
            let _ = c.wait();
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn kill9(mut c: Child) {
    let _ = c.kill();
    let _ = c.wait();
}

fn producer() -> FutureProducer {
    ClientConfig::new().set("bootstrap.servers", brokers()).set("acks", "all").create().unwrap()
}

async fn send_raw(topic: &str, payload: Option<&[u8]>) {
    send_raw_p(topic, 0, payload).await
}

async fn send_raw_p(topic: &str, part: i32, payload: Option<&[u8]>) {
    let p = producer();
    let mut r: FutureRecord<'_, str, [u8]> = FutureRecord::to(topic).key("k").partition(part);
    if let Some(b) = payload {
        r = r.payload(b);
    }
    p.send(r, Duration::from_secs(10)).await.map_err(|e| e.0).expect("produce");
}

/// Bulk-produce `n` flat-price trades for `sym` to one partition.
async fn send_many(topic: &str, part: i32, sym: &str, n: i64, base_off: i64) {
    let p = producer();
    let mut futs = Vec::new();
    for i in 0..n {
        let payload = trade_json(sym, i + 1, 100.0, base_off + i % 30);
        let rec = FutureRecord::to(topic).key("k").partition(part).payload(payload.as_bytes());
        futs.push(p.send_result(rec).map_err(|x| x.0).expect("enqueue"));
    }
    for f in futs {
        f.await.expect("delivery").map_err(|x| x.0).expect("delivered");
    }
}

fn trade_json(sym: &str, id: i64, price: f64, offset_secs: i64) -> String {
    // Event times are in the past relative to now (never trips the future-skew guard).
    let ts = chrono::Utc::now() - chrono::Duration::seconds(300) + chrono::Duration::seconds(offset_secs);
    format!(r#"{{"i":{id},"S":"{sym}","p":{price},"s":100,"t":"{}","x":"V","z":"C"}}"#, ts.to_rfc3339())
}

async fn send_trade(topic: &str, sym: &str, id: i64, price: f64, offset_secs: i64) {
    send_trade_p(topic, 0, sym, id, price, offset_secs).await
}

async fn send_trade_p(topic: &str, part: i32, sym: &str, id: i64, price: f64, offset_secs: i64) {
    send_raw_p(topic, part, Some(trade_json(sym, id, price, offset_secs).as_bytes())).await;
}

/// Read price-spikes from the beginning; wait until `min` messages are seen (or timeout),
/// then keep listening `settle` longer to catch unexpected extras.
fn read_out(topic: &str, min: usize, timeout: Duration, settle: Duration) -> Vec<(Option<String>, Vec<u8>)> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", format!("reader-{}", uuid_like()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[topic]).unwrap();
    let mut got = Vec::new();
    let start = Instant::now();
    let mut reached: Option<Instant> = None;
    loop {
        if let Some(r) = reached {
            if r.elapsed() > settle {
                break;
            }
        } else if got.len() >= min && min > 0 {
            reached = Some(Instant::now());
        } else if start.elapsed() > timeout {
            if min == 0 {
                reached = Some(Instant::now());
            } else {
                break;
            }
        }
        if let Some(Ok(m)) = c.poll(Duration::from_millis(200)) {
            got.push((m.key().map(|k| String::from_utf8_lossy(k).into_owned()), m.payload().unwrap_or_default().to_vec()));
        }
    }
    got
}

fn uuid_like() -> String {
    format!("{}-{}", std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
}

fn events(raw: &[(Option<String>, Vec<u8>)]) -> Vec<SpikeEvent> {
    raw.iter().map(|(_, p)| serde_json::from_slice(p).expect("output matches SpikeEvent contract")).collect()
}

async fn wait_committed(e: &Env, at_least: i64, timeout: Duration) -> Option<i64> {
    let start = Instant::now();
    loop {
        let c = e.committed();
        if c.is_some_and(|v| v >= at_least) || start.elapsed() > timeout {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

const IGNORE: &str = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)";

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn consume_trades_publish_spike_keyed_by_symbol_then_store_offset() {
    let _ = IGNORE;
    let e = Env::new("basic").await;
    send_trade(&e.src, "AAPL", 1, 100.0, 0).await;
    send_trade(&e.src, "MSFT", 1, 200.0, 1).await;
    send_trade(&e.src, "AAPL", 2, 106.0, 10).await;
    send_trade(&e.src, "MSFT", 2, 201.0, 11).await; // below threshold
    let child = e.spawn(&e.out);
    let raw = read_out(&e.out, 1, Duration::from_secs(90), Duration::from_secs(3));
    let ok = stop(child);
    assert_eq!(raw.len(), 1, "exactly one spike expected");
    assert_eq!(raw[0].0.as_deref(), Some("AAPL"), "message key = symbol");
    let ev = &events(&raw)[0];
    assert_eq!(ev.direction, Direction::Up);
    assert_eq!(ev.symbol, "AAPL");
    assert_eq!(ev.schema_version, 1);
    assert_eq!(ev.baseline_price, 100.0);
    assert_eq!(ev.current_price, 106.0);
    assert!((ev.percentage_change - 6.0).abs() < 1e-9);
    assert_eq!(ev.window_seconds, 600);
    assert!(ev.event_id.starts_with("AAPL-UP-2-"));
    assert!(ok, "graceful SIGTERM shutdown should exit 0");
    assert_eq!(e.committed(), Some(4), "all 4 records handled and committed on shutdown");
    e.teardown().await;
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn producer_failure_does_not_advance_offset_and_record_is_reprocessed() {
    let e = Env::new("pfail").await;
    send_trade(&e.src, "AAPL", 1, 100.0, 0).await; // offset 0
    send_trade(&e.src, "AAPL", 2, 106.0, 10).await; // offset 1 -> spike
    send_trade(&e.src, "AAPL", 3, 107.0, 20).await; // offset 2
    // Publish to an invalid topic name: every delivery attempt fails, service retries forever.
    let bad = "invalid topic name!";
    let child = e.spawn(bad);
    tokio::time::sleep(Duration::from_secs(12)).await;
    stop(child);
    let c = e.committed();
    assert!(c.is_none_or(|v| v <= 1), "offset of undelivered spike record (1) must not be stored; committed={c:?}");
    // Healthy restart: the record is reprocessed and the spike delivered.
    let child = e.spawn(&e.out);
    let raw = read_out(&e.out, 1, Duration::from_secs(60), Duration::from_secs(3));
    stop(child);
    let evs = events(&raw);
    assert_eq!(evs.len(), 1, "spike delivered exactly once after recovery");
    assert_eq!(evs[0].current_price, 106.0);
    assert_eq!(e.committed(), Some(3));
    e.teardown().await;
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn restart_warm_up_restores_window_and_cooldown_without_duplicates() {
    let e = Env::new("restart").await;
    send_trade(&e.src, "AAPL", 1, 100.0, 0).await;
    send_trade(&e.src, "AAPL", 2, 106.0, 10).await; // UP spike
    let child = e.spawn(&e.out);
    let first = read_out(&e.out, 1, Duration::from_secs(60), Duration::from_secs(1));
    assert_eq!(first.len(), 1);
    stop(child);
    assert_eq!(e.committed(), Some(2));
    // New data while the service is down.
    send_trade(&e.src, "AAPL", 3, 108.0, 30).await; // UP again: within cooldown -> suppressed
    send_trade(&e.src, "AAPL", 4, 93.0, 40).await; // DOWN vs baseline 100: only possible if window was rebuilt
    let child = e.spawn(&e.out);
    let raw = read_out(&e.out, 2, Duration::from_secs(60), Duration::from_secs(5));
    stop(child);
    let evs = events(&raw);
    assert_eq!(evs.len(), 2, "UP not republished, DOWN emitted: {evs:?}");
    assert_eq!(evs.iter().filter(|x| x.direction == Direction::Up).count(), 1, "no duplicate UP");
    let down: Vec<_> = evs.iter().filter(|x| x.direction == Direction::Down).collect();
    assert_eq!(down.len(), 1, "warm-up must restore baseline so DOWN is detected");
    assert_eq!(down[0].baseline_price, 100.0);
    e.teardown().await;
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn hard_kill_mid_run_loses_no_spike_and_restores_cooldown() {
    let e = Env::new("crash").await;
    send_trade(&e.src, "AAPL", 1, 100.0, 0).await;
    send_trade(&e.src, "AAPL", 2, 106.0, 10).await; // UP
    let child = e.spawn(&e.out);
    let first = read_out(&e.out, 1, Duration::from_secs(90), Duration::ZERO);
    kill9(child); // before the 5s auto-commit
    assert_eq!(first.len(), 1);
    let up_id = events(&first)[0].event_id.clone();
    send_trade(&e.src, "AAPL", 3, 108.0, 30).await; // UP within cooldown: must stay suppressed
    send_trade(&e.src, "AAPL", 4, 93.0, 40).await; // DOWN vs baseline 100
    let child = e.spawn(&e.out);
    // Out topic now holds first UP (+ possibly a redelivered copy) + DOWN.
    let raw = read_out(&e.out, 2, Duration::from_secs(60), Duration::from_secs(8));
    stop(child);
    let evs = events(&raw);
    let ups: Vec<_> = evs.iter().filter(|x| x.direction == Direction::Up).collect();
    let downs: Vec<_> = evs.iter().filter(|x| x.direction == Direction::Down).collect();
    assert!(!ups.is_empty(), "UP spike lost");
    assert!(ups.iter().all(|x| x.event_id == up_id), "redelivery must reuse event_id; a different UP id means cooldown was not restored: {ups:?}");
    assert_eq!(downs.len(), 1, "DOWN spike lost or duplicated: {downs:?}");
    e.teardown().await;
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn partition_without_committed_offset_is_still_processed_after_restart() {
    let e = Env::with_partitions("twopart", 2).await;
    // Run 1: only partition 0 has data, so only p0 gets a committed offset.
    // Long p0 replay on restart, so p1's backlog is fetched while warm-up is still running.
    send_many(&e.src, 0, "AAA", 50_000, 0).await;
    send_trade_p(&e.src, 0, "AAA", 100_000, 106.0, 40).await; // spike (UP)
    let child = e.spawn(&e.out);
    let first = read_out(&e.out, 1, Duration::from_secs(120), Duration::from_secs(1));
    assert_eq!(first.len(), 1);
    stop(child);
    assert_eq!(e.committed_p(0), Some(50_001));
    assert_eq!(e.committed_p(1), None, "precondition: p1 has no committed offset");
    // While down: p1 gets its first records. Spike is EARLY in the backlog, then filler.
    send_trade_p(&e.src, 1, "ZZZ", 1, 50.0, 50).await;
    send_trade_p(&e.src, 1, "ZZZ", 2, 60.0, 60).await; // +20%
    send_many(&e.src, 1, "YYY", 3000, 100).await;
    let child = e.spawn(&e.out);
    let raw = read_out(&e.out, 2, Duration::from_secs(120), Duration::from_secs(3));
    stop(child);
    let evs = events(&raw);
    assert_eq!(e.committed_p(1), Some(3002), "every p1 record must be processed and its offset committed");
    assert!(evs.iter().any(|x| x.symbol == "ZZZ"), "records on the partition with no committed offset were dropped during warm-up; got {evs:?}");
    assert_eq!(evs.iter().filter(|x| x.symbol == "AAA").count(), 1, "AAA spike must not be republished");
    e.teardown().await;
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn missing_source_topic_exits_nonzero_instead_of_idling() {
    let e = Env::new("notopic").await;
    // Delete the source topic so it does not exist (broker auto-create may recreate it on metadata fetch).
    let admin: AdminClient<DefaultClientContext> =
        ClientConfig::new().set("bootstrap.servers", brokers()).create().unwrap();
    let _ = admin.delete_topics(&[e.src.as_str()], &AdminOptions::new()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut child = e.spawn(&e.out);
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break Some(s);
        }
        if start.elapsed() > Duration::from_secs(45) {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    e.teardown().await;
    let s = status.expect("service idled for 45s on a missing source topic instead of exiting");
    assert!(!s.success(), "must exit non-zero, got {s:?}");
}

#[tokio::test]
#[ignore = "needs a live Kafka broker (set SPIKE_IT_BROKERS, default localhost:19092)"]
async fn malformed_payloads_are_skipped_and_service_keeps_running() {
    let e = Env::new("malformed").await;
    send_raw(&e.src, Some(b"not json")).await; // 0
    send_raw(&e.src, None).await; // 1 tombstone
    send_raw(&e.src, Some(b"{}")).await; // 2
    send_raw(&e.src, Some(&[0xff, 0xfe, 0x00])).await; // 3
    send_trade(&e.src, "AAPL", 1, 100.0, 0).await; // 4
    send_trade(&e.src, "AAPL", 2, 106.0, 10).await; // 5
    let mut child = e.spawn(&e.out);
    let raw = read_out(&e.out, 1, Duration::from_secs(60), Duration::from_secs(2));
    assert_eq!(raw.len(), 1, "spike after the garbage still detected");
    assert!(child.try_wait().unwrap().is_none(), "service must not exit on malformed input");
    let ok = stop(child);
    assert!(ok);
    assert_eq!(wait_committed(&e, 6, Duration::from_secs(5)).await, Some(6), "malformed records are skipped past, not retried forever");
    e.teardown().await;
}
