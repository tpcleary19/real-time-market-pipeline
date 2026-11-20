//! Skeptic (Agent 5) probes that run the REAL `spike-detector` binary against a live broker.
//! Each test asserts the documented/expected behaviour; a FAILING test is a finding
//! (see docs/spike-detector-skeptic.md). All `#[ignore]`d.
//!
//! Broker (own container, never the compose one):
//!   docker run -d --name skeptic-kafka -p 29092:9092 ... apache/kafka:4.1.0   (see skeptic doc)
//! Run:
//!   SKEPTIC_BROKERS=localhost:29092 cargo test -p spike-detector --test skeptic_kafka -- --ignored --test-threads=1
use chrono::{DateTime, Duration as Cd, Utc};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use spike_detector::SpikeEvent;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn brokers() -> String {
    std::env::var("SKEPTIC_BROKERS").unwrap_or_else(|_| "localhost:29092".into())
}

static N: AtomicU32 = AtomicU32::new(0);

struct Env {
    src: String,
    out: String,
    group: String,
}

fn admin() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .unwrap()
}

impl Env {
    async fn new(tag: &str, parts: i32) -> Env {
        let id = format!(
            "{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        );
        let e = Env {
            src: format!("skeptic-src-{id}"),
            out: format!("skeptic-out-{id}"),
            group: format!("skeptic-grp-{id}"),
        };
        for (t, n) in [(&e.src, parts), (&e.out, 1)] {
            let nt = NewTopic::new(t, n, TopicReplication::Fixed(1));
            admin()
                .create_topics([&nt], &AdminOptions::new())
                .await
                .unwrap()[0]
                .as_ref()
                .unwrap();
        }
        e
    }
    async fn teardown(&self) {
        let _ = admin()
            .delete_topics(&[&self.src, &self.out], &AdminOptions::new())
            .await;
    }
    fn spawn(&self, extra: &[(&str, &str)]) -> Child {
        let mut c = Command::new(env!("CARGO_BIN_EXE_spike-detector"));
        c.env("KAFKA_BROKERS", brokers())
            .env("SPIKE_SOURCE_TOPIC", &self.src)
            .env("SPIKE_OUTPUT_TOPIC", &self.out)
            .env("SPIKE_CONSUMER_GROUP", &self.group)
            .env("SPIKE_AUTO_OFFSET_RESET", "earliest")
            .env("SPIKE_METRICS_INTERVAL_SECONDS", "3600");
        for (k, v) in extra {
            c.env(k, v);
        }
        c.stdout(Stdio::null())
            .stderr(if std::env::var("SKEPTIC_LOG").is_ok() {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .spawn()
            .unwrap()
    }
    fn committed(&self, part: i32) -> Option<i64> {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers())
            .set("group.id", &self.group)
            .create()
            .unwrap();
        let mut l = TopicPartitionList::new();
        l.add_partition(&self.src, part);
        match c
            .committed_offsets(l, Duration::from_secs(10))
            .unwrap()
            .elements()[0]
            .offset()
        {
            Offset::Offset(o) => Some(o),
            _ => None,
        }
    }
}

fn stop(mut c: Child) -> Option<i32> {
    let _ = Command::new("kill")
        .args(["-TERM", &c.id().to_string()])
        .status();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(40) {
        if let Some(s) = c.try_wait().unwrap() {
            return s.code();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = c.kill();
    let _ = c.wait();
    None
}

fn wait_exit(c: &mut Child, max: Duration) -> Option<i32> {
    let start = Instant::now();
    while start.elapsed() < max {
        if let Some(s) = c.try_wait().unwrap() {
            return s.code();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("acks", "all")
        .set("message.max.bytes", "2000000")
        .create()
        .unwrap()
}

fn trade_json(sym: &str, id: i64, price: f64, ts: DateTime<Utc>) -> String {
    format!(
        r#"{{"i":{id},"S":"{sym}","p":{price},"s":100,"t":"{}","x":"V","z":"C"}}"#,
        ts.to_rfc3339()
    )
}

/// Produce with an explicit Kafka record timestamp equal to the trade time (like a live feed).
async fn send(topic: &str, part: i32, sym: &str, id: i64, price: f64, ts: DateTime<Utc>) {
    let payload = trade_json(sym, id, price, ts);
    producer()
        .send(
            FutureRecord::to(topic)
                .key(sym)
                .partition(part)
                .timestamp(ts.timestamp_millis())
                .payload(payload.as_bytes()),
            Duration::from_secs(10),
        )
        .await
        .map_err(|e| e.0)
        .expect("produce");
}

fn read_out(topic: &str, settle: Duration) -> Vec<SpikeEvent> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set(
            "group.id",
            format!(
                "skeptic-reader-{}",
                Utc::now().timestamp_nanos_opt().unwrap()
            ),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[topic]).unwrap();
    let mut got = Vec::new();
    let start = Instant::now();
    while start.elapsed() < settle {
        if let Some(Ok(m)) = c.poll(Duration::from_millis(200)) {
            got.push(serde_json::from_slice(m.payload().unwrap()).unwrap());
        }
    }
    got
}

fn sleep(s: u64) {
    std::thread::sleep(Duration::from_secs(s));
}

/// A trade whose symbol is a few hundred KB long (garbage from upstream) is valid per
/// `valid_symbol`, so a spike on it produces an event (key + symbol + event_id) larger than
/// the producer's message.max.bytes. That is classified non-retriable: the service exits 1
/// without storing the offset, and on every restart it hits the same record again.
/// Expected: a poison record must not block the whole stream forever.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_oversized_symbol_is_not_a_poison_pill() {
    let e = Env::new("poison", 1).await;
    let base = Utc::now() - Cd::seconds(120);
    let big = "X".repeat(350_000);
    send(&e.src, 0, &big, 1, 100.0, base).await;
    send(&e.src, 0, &big, 2, 110.0, base + Cd::seconds(1)).await;
    send(&e.src, 0, "OK", 3, 100.0, base + Cd::seconds(2)).await;
    send(&e.src, 0, "OK", 4, 110.0, base + Cd::seconds(3)).await;
    let mut exits = Vec::new();
    for _ in 0..3 {
        let mut c = e.spawn(&[]);
        let code = wait_exit(&mut c, Duration::from_secs(25));
        if code.is_none() {
            let _ = stop(c);
        }
        exits.push(code);
    }
    let out = read_out(&e.out, Duration::from_secs(3));
    let committed = e.committed(0);
    e.teardown().await;
    assert!(
        out.iter().any(|ev| ev.symbol == "OK"),
        "healthy symbol never published; exits per restart = {exits:?}, committed = {committed:?}"
    );
}

/// Real-service version of the warm-up cooldown phase divergence: a steady trend keeps the
/// spike condition true; stop and restart mid-trend; the post-restart events should be the
/// ones an uninterrupted run emits.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_restart_mid_trend_emits_same_event_ids_as_uninterrupted() {
    let cfg = [
        ("SPIKE_WINDOW_SECONDS", "60"),
        ("SPIKE_COOLDOWN_SECONDS", "60"),
    ];
    let base = Utc::now() - Cd::seconds(1200);
    let trade = |i: i64| (100.0 * 1.01f64.powi(i as i32), base + Cd::seconds(i * 10));

    // Reference: uninterrupted run on its own topics.
    let r = Env::new("trend-ref", 1).await;
    for i in 0..100 {
        let (p, ts) = trade(i);
        send(&r.src, 0, "TRND", i + 1, p, ts).await;
    }
    let c = r.spawn(&cfg);
    sleep(8);
    stop(c);
    let reference: Vec<String> = read_out(&r.out, Duration::from_secs(3))
        .into_iter()
        .map(|e| e.event_id)
        .collect();
    r.teardown().await;

    // Interrupted: first 51 records, graceful stop (commit 51), then the rest, restart.
    let e = Env::new("trend", 1).await;
    for i in 0..51 {
        let (p, ts) = trade(i);
        send(&e.src, 0, "TRND", i + 1, p, ts).await;
    }
    let c = e.spawn(&cfg);
    sleep(8);
    assert_eq!(stop(c), Some(0));
    let committed = e.committed(0);
    for i in 51..100 {
        let (p, ts) = trade(i);
        send(&e.src, 0, "TRND", i + 1, p, ts).await;
    }
    let c = e.spawn(&cfg);
    sleep(10);
    stop(c);
    let got: Vec<String> = read_out(&e.out, Duration::from_secs(3))
        .into_iter()
        .map(|e| e.event_id)
        .collect();
    e.teardown().await;
    assert_eq!(committed, Some(51));
    assert_eq!(got, reference, "event ids differ after restart");
}

/// Host clock behind the feed by more than SPIKE_MAX_FUTURE_SKEW_SECONDS (e.g. WSL2 clock
/// drift). Every live trade is rejected as "future", yet its offset is stored, so the spikes
/// are lost permanently, and warm-up (which uses the record timestamp, not wall clock)
/// would have accepted the same records.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_clock_behind_feed_does_not_lose_spikes() {
    let e = Env::new("skew", 1).await;
    // Simulate "host clock 90 s behind": trades and record timestamps 90 s ahead of host now.
    let ahead = Utc::now() + Cd::seconds(90);
    send(&e.src, 0, "SKEW", 1, 100.0, ahead).await;
    send(&e.src, 0, "SKEW", 2, 110.0, ahead + Cd::seconds(1)).await;
    let c = e.spawn(&[]);
    sleep(8);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    let committed = e.committed(0);
    e.teardown().await;
    assert!(
        !out.is_empty() || committed != Some(2),
        "spike lost: 0 events published but offset committed at {committed:?}"
    );
}

/// Default `latest`: start once with no traffic, stop, trades + spike arrive while down,
/// restart. Documented as a known limitation; verify it really loses the spike.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_latest_default_restart_keeps_records_produced_while_down() {
    let e = Env::new("latest", 1).await;
    let latest = [("SPIKE_AUTO_OFFSET_RESET", "latest")];
    let c = e.spawn(&latest);
    sleep(6);
    assert_eq!(stop(c), Some(0));
    let base = Utc::now() - Cd::seconds(5);
    send(&e.src, 0, "LATE", 1, 100.0, base).await;
    send(&e.src, 0, "LATE", 2, 110.0, base + Cd::seconds(1)).await;
    let c = e.spawn(&latest);
    sleep(8);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    e.teardown().await;
    assert_eq!(
        out.len(),
        1,
        "spike produced while the service was down was skipped"
    );
}

/// Partitions added to the source topic while the service runs: not consumed until restart,
/// and after a restart (default `latest`) records already in the new partition are skipped.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_added_partition_records_are_not_lost() {
    let e = Env::new("addpart", 1).await;
    let latest = [("SPIKE_AUTO_OFFSET_RESET", "latest")];
    let c = e.spawn(&latest);
    sleep(6);
    let r = admin()
        .create_partitions(&[NewPartitions::new(&e.src, 2)], &AdminOptions::new())
        .await
        .unwrap();
    r[0].as_ref().unwrap();
    sleep(2);
    let base = Utc::now() - Cd::seconds(5);
    send(&e.src, 1, "NEWP", 1, 100.0, base).await;
    send(&e.src, 1, "NEWP", 2, 110.0, base + Cd::seconds(1)).await;
    sleep(6);
    let before_restart = read_out(&e.out, Duration::from_secs(2)).len();
    stop(c);
    let c = e.spawn(&latest);
    sleep(8);
    stop(c);
    let after = read_out(&e.out, Duration::from_secs(3)).len();
    e.teardown().await;
    assert_eq!(
        after, 1,
        "spike in added partition never published (before restart {before_restart}, after {after})"
    );
}

/// kill -9 while a publish is being retried (output topic deleted), then recreate the topic
/// and restart: the spike must appear exactly with the same event_id, nothing skipped.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_kill9_mid_publish_then_restart_delivers() {
    let e = Env::new("k9", 1).await;
    admin()
        .delete_topics(&[&e.out], &AdminOptions::new())
        .await
        .unwrap();
    sleep(2);
    let base = Utc::now() - Cd::seconds(60);
    send(&e.src, 0, "KNIN", 1, 100.0, base).await;
    send(&e.src, 0, "KNIN", 2, 110.0, base + Cd::seconds(1)).await;
    send(&e.src, 0, "KNIN", 3, 95.0, base + Cd::seconds(2)).await;
    let mut c = e.spawn(&[]);
    sleep(12); // retrying publish against a missing topic
    let _ = c.kill();
    let _ = c.wait();
    let committed_after_kill = e.committed(0);
    let nt = NewTopic::new(&e.out, 1, TopicReplication::Fixed(1));
    admin()
        .create_topics([&nt], &AdminOptions::new())
        .await
        .unwrap();
    sleep(2);
    let c = e.spawn(&[]);
    sleep(10);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    let committed = e.committed(0);
    e.teardown().await;
    assert!(
        committed_after_kill.is_none_or(|o| o <= 1),
        "offset {committed_after_kill:?} stored past the unpublished spike record"
    );
    let ids: Vec<_> = out.iter().map(|x| x.event_id.as_str()).collect();
    assert_eq!(ids.len(), 2, "UP and DOWN expected once each: {ids:?}");
    assert_eq!(committed, Some(3));
}

/// Garbage payloads must be skipped without crashing; a valid spike afterwards still publishes.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_garbage_payloads_do_not_stop_the_service() {
    let e = Env::new("garbage", 1).await;
    let p = producer();
    let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
    let big_junk = "z".repeat(900_000);
    let ts = (Utc::now() - Cd::seconds(30)).to_rfc3339();
    let garbage: Vec<Vec<u8>> = vec![
        deep.into_bytes(),
        big_junk.into_bytes(),
        vec![0xff, 0xfe, 0x00, 0x80],
        br#"{"i":1,"S":"G","p":1e400,"s":1,"t":"2024-01-01T00:00:00Z"}"#.to_vec(),
        br#"{"i":1,"S":"G","p":"100","s":1,"t":"2024-01-01T00:00:00Z"}"#.to_vec(),
        format!(r#"{{"i":1,"S":"G","p":-0.0,"s":1,"t":"{ts}"}}"#).into_bytes(),
        format!(r#"{{"i":9223372036854775807,"S":"G\u0000","p":5,"s":1,"t":"{ts}"}}"#).into_bytes(),
        br#"{"i":1,"S":"G","p":5,"s":1,"t":"+262143-12-31T23:59:59Z"}"#.to_vec(),
        br#"{"i":1,"S":"G","p":5,"s":1,"t":"-262143-01-01T00:00:00Z"}"#.to_vec(),
        format!(r#"{{"i":1,"S":"G","p":5e-324,"s":1,"t":"{ts}"}}"#).into_bytes(),
        Vec::new(),
    ];
    for g in &garbage {
        p.send(
            FutureRecord::to(&e.src)
                .key("k")
                .partition(0)
                .payload(g.as_slice()),
            Duration::from_secs(10),
        )
        .await
        .map_err(|x| x.0)
        .unwrap();
    }
    // A denormal price followed by a normal one under the same symbol: +inf change, skipped.
    send(&e.src, 0, "G", 2, 100.0, Utc::now() - Cd::seconds(20)).await;
    let base = Utc::now() - Cd::seconds(10);
    send(&e.src, 0, "OKAY", 1, 100.0, base).await;
    send(&e.src, 0, "OKAY", 2, 106.0, base + Cd::seconds(1)).await;
    let mut c = e.spawn(&[]);
    sleep(8);
    let alive = c.try_wait().unwrap().is_none();
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    let committed = e.committed(0);
    e.teardown().await;
    assert!(alive, "service died on garbage");
    let syms: Vec<_> = out.iter().map(|x| x.symbol.clone()).collect();
    assert_eq!(syms, vec!["OKAY".to_string()], "unexpected events {out:?}");
    assert_eq!(committed, Some(garbage.len() as i64 + 3));
}

/// The same trades produced twice (upstream replay), plus duplicate and late trades across a
/// restart, must not create new events.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_replayed_duplicate_and_late_trades_across_restart() {
    let e = Env::new("dup", 1).await;
    let base = Utc::now() - Cd::seconds(200);
    let seq = [(1, 100.0, 0), (2, 103.0, 10), (3, 106.0, 20), (4, 93.0, 30)];
    for (id, p, s) in seq {
        send(&e.src, 0, "DUPE", id, p, base + Cd::seconds(s)).await;
    }
    let c = e.spawn(&[]);
    sleep(8);
    assert_eq!(stop(c), Some(0));
    // Upstream replays everything, plus a late trade and a duplicate id with a different price.
    for (id, p, s) in seq {
        send(&e.src, 0, "DUPE", id, p, base + Cd::seconds(s)).await;
    }
    send(&e.src, 0, "DUPE", 5, 200.0, base + Cd::seconds(5)).await; // late
    send(&e.src, 0, "DUPE", 3, 50.0, base + Cd::seconds(30)).await; // dup id, same ET date
    let c = e.spawn(&[]);
    sleep(10);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    e.teardown().await;
    let ids: Vec<_> = out.iter().map(|x| x.event_id.clone()).collect();
    assert_eq!(
        ids.len(),
        2,
        "expected exactly UP@3 and DOWN@4, got {ids:?}"
    );
}

/// Burst / consumer lag: 30k trades over 300 symbols pre-loaded, then the service starts.
/// Every symbol has exactly one 6% UP move; all 300 must be published exactly once.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_burst_backlog_all_spikes_once() {
    let e = Env::new("burst", 3).await;
    let p = producer();
    let base = Utc::now() - Cd::seconds(500);
    let mut futs = Vec::new();
    for i in 0..100i64 {
        for s in 0..300 {
            let sym = format!("B{s:03}");
            let price = if i < 99 { 100.0 } else { 106.0 };
            let ts = base + Cd::seconds(i * 4);
            let payload = trade_json(&sym, i + 1, price, ts);
            let rec = FutureRecord::to(&e.src)
                .key(&sym)
                .partition(s % 3)
                .timestamp(ts.timestamp_millis())
                .payload(&payload);
            futs.push(p.send_result(rec).map_err(|x| x.0).unwrap());
        }
    }
    for f in futs {
        f.await.unwrap().map_err(|x| x.0).unwrap();
    }
    let c = e.spawn(&[]);
    sleep(25);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(5));
    e.teardown().await;
    let mut ids: Vec<_> = out.iter().map(|x| x.event_id.clone()).collect();
    let n = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!((n, ids.len()), (300, 300));
}

/// kill -9 in the middle of a long warm-up, then restart: committed offset unchanged, the next
/// spike is still published exactly once.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_kill9_mid_warm_up() {
    let e = Env::new("wu", 1).await;
    let p = producer();
    let base = Utc::now() - Cd::seconds(900);
    let mut futs = Vec::new();
    for i in 0..200_000i64 {
        let ts = base + Cd::milliseconds(i * 4);
        let payload = trade_json("WARM", i + 1, 100.0, ts);
        let rec = FutureRecord::to(&e.src)
            .key("WARM")
            .partition(0)
            .timestamp(ts.timestamp_millis())
            .payload(&payload);
        futs.push(p.send_result(rec).map_err(|x| x.0).unwrap());
    }
    for f in futs {
        f.await.unwrap().map_err(|x| x.0).unwrap();
    }
    let c = e.spawn(&[]);
    sleep(15);
    assert_eq!(stop(c), Some(0));
    let committed = e.committed(0);
    let mut c = e.spawn(&[]);
    std::thread::sleep(Duration::from_millis(700)); // inside warm-up
    let _ = c.kill();
    let _ = c.wait();
    let after_kill = e.committed(0);
    let t = Utc::now() - Cd::seconds(5);
    send(&e.src, 0, "WARM", 300_001, 110.0, t).await;
    let c = e.spawn(&[]);
    sleep(15);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(3));
    e.teardown().await;
    assert_eq!(committed, Some(200_000));
    assert_eq!(after_kill, committed);
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].baseline_price == 100.0);
}

/// Broker restart while a backlog of spikes is being published (restarts the skeptic's OWN
/// container named by SKEPTIC_CONTAINER, default `skeptic-kafka`): every spike exactly once
/// by event_id, offsets fully committed afterwards.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn skeptic_broker_restart_during_publishing() {
    let container = std::env::var("SKEPTIC_CONTAINER").unwrap_or_else(|_| "skeptic-kafka".into());
    let e = Env::new("brk", 1).await;
    let p = producer();
    let base = Utc::now() - Cd::seconds(500);
    let mut futs = Vec::new();
    let mut total = 0i64;
    for s in 0..2000 {
        let sym = format!("R{s:04}");
        for (i, price) in [(0i64, 100.0), (1, 107.0)] {
            let ts = base + Cd::milliseconds(s * 100 + i * 50);
            let payload = trade_json(&sym, i + 1, price, ts);
            let rec = FutureRecord::to(&e.src)
                .key(&sym)
                .partition(0)
                .timestamp(ts.timestamp_millis())
                .payload(&payload);
            futs.push(p.send_result(rec).map_err(|x| x.0).unwrap());
            total += 1;
        }
    }
    for f in futs {
        f.await.unwrap().map_err(|x| x.0).unwrap();
    }
    let c = e.spawn(&[]);
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        Command::new("docker")
            .args(["restart", &container])
            .status()
            .unwrap()
            .success()
    );
    sleep(90);
    stop(c);
    let out = read_out(&e.out, Duration::from_secs(8));
    let committed = e.committed(0);
    e.teardown().await;
    let mut ids: Vec<_> = out.iter().map(|x| x.event_id.clone()).collect();
    let n = ids.len();
    ids.sort();
    ids.dedup();
    eprintln!(
        "published records={n} unique={} committed={committed:?}",
        ids.len()
    );
    assert_eq!(ids.len(), 2000, "lost spikes");
    assert_eq!(committed, Some(total));
}
