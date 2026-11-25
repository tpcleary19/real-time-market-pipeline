//! Skeptic (Agent 5) probes against the pure detection core. These tests encode the
//! behaviour the docs promise; a FAILING test here is a finding (see docs/spike-detector-skeptic.md).
//!
//! Run: cargo test -p spike-detector --test skeptic_detector
use chrono::{DateTime, Duration, Utc};
use market_events::Trade;
use spike_detector::detector::{Config, Detector};
use spike_detector::{Direction, SpikeEvent};

fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2024-01-02T15:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn trade(sym: &str, id: i64, price: f64, secs: i64) -> Trade {
    Trade {
        trade_id: id,
        symbol: sym.into(),
        price,
        size: 1,
        timestamp: t0() + Duration::seconds(secs),
        exchange: None,
        tape: None,
    }
}

fn cfg(window: u64, cooldown: u64) -> Config {
    Config {
        window_seconds: window,
        threshold_percent: 5.0,
        cooldown_seconds: cooldown,
        max_symbols: 10_000,
    }
}

fn ids(evs: &[SpikeEvent]) -> Vec<String> {
    evs.iter().map(|e| e.event_id.clone()).collect()
}

/// A steady trend that keeps the spike condition true for longer than the cooldown.
/// Price rises 1% every 10 s; window 60 s, cooldown 60 s.
fn trend(n: i64) -> Vec<Trade> {
    (0..n)
        .map(|i| trade("TRND", i + 1, 100.0 * 1.01f64.powi(i as i32), i * 10))
        .collect()
}

/// Model of the service restart (kafka.rs `assign_and_warm_up` + main.rs seeding):
/// replay records with time >= ts(C-1) - lookback (events discarded), then
/// `reset_cooldowns()` and seed each (symbol, direction) with the latest `current_timestamp`
/// among events already in `price-spikes` whose Kafka time >= that same bound. In these
/// fixtures Kafka record time == trade time, so an output event's Kafka time is modelled as
/// its `current_timestamp` (it can only be later in reality, which reads more, not less).
fn restart_like_service(
    trades: &[Trade],
    c: usize,
    published: &[SpikeEvent],
    window: u64,
    cooldown: u64,
) -> Detector {
    let lookback = Duration::seconds((window + cooldown + 60) as i64);
    let since = trades[c - 1].timestamp - lookback;
    let mut d = Detector::new(cfg(window, cooldown));
    for t in &trades[..c] {
        if t.timestamp >= since {
            let _ = d.observe(t, t0()); // warm-up: discarded
        }
    }
    d.reset_cooldowns();
    for e in published.iter().filter(|e| e.current_timestamp >= since) {
        d.seed_cooldown(&e.symbol, e.direction, e.current_timestamp);
    }
    d
}

/// Warm-up replays only `window + cooldown + 60 s` before the last committed record, with
/// an empty cooldown table. Under a sustained spike condition the cooldown "phase" in the
/// replayed state differs from the original run, so after the restart the service emits
/// spikes at different trades (different event_ids) than an uninterrupted run would.
/// The service now seeds cooldowns from price-spikes after warm-up; this test models that
/// (see `restart_like_service`). Without the seed it fails (original skeptic finding #4).
#[test]
fn skeptic_warm_up_cooldown_phase_matches_uninterrupted_run() {
    let (window, cooldown) = (60u64, 60u64);
    let trades = trend(100); // t = 0 .. 990 s
    let now = t0();

    // Uninterrupted run.
    let mut live = Detector::new(cfg(window, cooldown));
    let all: Vec<SpikeEvent> = trades.iter().flat_map(|t| live.observe(t, now)).collect();

    // Restart with committed offset C = 51 (last handled record index 50, t = 500 s).
    // Everything the original run published from records < C is in price-spikes.
    let c = 51usize;
    let last_ts = trades[c - 1].timestamp;
    let published: Vec<SpikeEvent> = all
        .iter()
        .filter(|e| e.current_timestamp <= last_ts)
        .cloned()
        .collect();
    let mut restarted = restart_like_service(&trades, c, &published, window, cooldown);
    let after: Vec<SpikeEvent> = trades[c..]
        .iter()
        .flat_map(|t| restarted.observe(t, now))
        .collect();

    let expected: Vec<SpikeEvent> = all
        .into_iter()
        .filter(|e| e.current_timestamp > last_ts)
        .collect();
    assert_eq!(
        ids(&after),
        ids(&expected),
        "post-restart spikes differ from the uninterrupted run"
    );
}

/// Same scenario, crash variant: the original run PUBLISHED a spike from record C but crashed
/// before the offset was committed. After restart the record is reprocessed, but the replayed
/// cooldown suppresses it and a later trade fires instead: the consumer now sees two UP
/// events for one symbol inside one cooldown period, with different event_ids.
#[test]
fn skeptic_crash_after_publish_does_not_yield_second_event_inside_cooldown() {
    let (window, cooldown) = (60u64, 60u64);
    let trades = trend(100);
    let now = t0();
    let mut live = Detector::new(cfg(window, cooldown));
    let all: Vec<(usize, SpikeEvent)> = trades
        .iter()
        .enumerate()
        .flat_map(|(i, t)| live.observe(t, now).into_iter().map(move |e| (i, e)))
        .collect();
    // Pick a spike record k published by the original run; crash before committing it,
    // so the committed offset is C = k (last committed record k-1).
    let (k, first) = all
        .iter()
        .find(|(i, _)| *i > 50)
        .cloned()
        .expect("a spike after index 50");
    // price-spikes holds everything published up to and including record k's event.
    let published: Vec<SpikeEvent> = all
        .iter()
        .filter(|(i, _)| *i <= k)
        .map(|(_, e)| e.clone())
        .collect();
    let mut restarted = restart_like_service(&trades, k, &published, window, cooldown);
    let replay: Vec<SpikeEvent> = trades[k..]
        .iter()
        .flat_map(|t| restarted.observe(t, now))
        .collect();
    // Not trivially quiet: after the restart the service must emit exactly what an
    // uninterrupted run emits after record k (record k itself was already published).
    let expected_after: Vec<String> = all
        .iter()
        .filter(|(i, _)| *i > k)
        .map(|(_, e)| e.event_id.clone())
        .collect();
    assert!(!expected_after.is_empty());
    assert_eq!(ids(&replay), expected_after, "post-crash events differ");
    // Union of what the consumer received: `first` (pre-crash) + everything after restart.
    let mut seen = vec![first.clone()];
    for e in replay {
        if !seen.iter().any(|s| s.event_id == e.event_id) {
            seen.push(e);
        }
    }
    seen.sort_by_key(|e| e.current_timestamp);
    for w in seen.windows(2) {
        assert!(
            w[0].direction != w[1].direction
                || w[1].current_timestamp - w[0].current_timestamp
                    >= Duration::seconds(cooldown as i64),
            "two {:?} events inside one cooldown: {} and {}",
            w[0].direction,
            w[0].event_id,
            w[1].event_id
        );
    }
}

/// Idle cleanup uses ONE global event-time watermark across all partitions. If one partition
/// is processed far ahead in event time (warm-up batches, a lagging partition catching up),
/// symbols of the lagging partition lose their window and cooldown state mid-stream, and a
/// spike that is inside its cooldown is emitted again with a new event_id.
#[test]
fn skeptic_lagging_partition_keeps_cooldown_when_another_partition_runs_ahead() {
    let mut d = Detector::new(cfg(600, 600));
    let now = t0();
    // Partition 1 (symbol LAG): UP spike at t = 1.
    assert!(d.observe(&trade("LAG", 1, 100.0, 0), now).is_empty());
    let first = d.observe(&trade("LAG", 2, 110.0, 1), now);
    assert_eq!(first.len(), 1);
    // Partition 0 (symbol FAST) is delivered as one big batch reaching t = 2000 s.
    for i in 0..1100 {
        d.observe(&trade("FAST", i + 1, 50.0, i * 2), now);
    }
    // Partition 1 continues where it was: t = 2..3 s, same 10% move, still inside LAG's cooldown.
    let mut again = d.observe(&trade("LAG", 3, 100.0, 2), now);
    again.extend(d.observe(&trade("LAG", 4, 110.0, 3), now));
    assert!(
        again.iter().all(|e| e.direction != Direction::Up),
        "UP re-emitted 2 s after the first UP (cooldown 600 s): {:?}",
        ids(&again)
    );
}

/// Threshold exactly at the boundary for awkward decimal prices (f64). The docs say
/// `abs >= threshold` is a spike with 1e-9 tolerance.
#[test]
fn skeptic_exact_boundary_awkward_prices() {
    let cases = [
        (0.07, 0.0735),
        (19.99, 20.9895),
        (333.33, 349.9965),
        (0.0001, 0.000105),
        (1234.56, 1296.288),
        (100.0, 95.0),
        (0.3, 0.285),
        (12345.67, 11728.3865),
    ];
    let mut misses = Vec::new();
    for (i, (b, c)) in cases.iter().enumerate() {
        let mut d = Detector::new(cfg(600, 600));
        d.observe(&trade("B", 1, *b, 0), t0());
        if d.observe(&trade("B", 2, *c, 1), t0()).is_empty() {
            misses.push((i, *b, *c));
        }
    }
    assert!(misses.is_empty(), "exact 5% moves not detected: {misses:?}");
}

/// A syntactically valid but absurd timestamp from upstream must be dropped, not panic.
/// (`window.rs` converts every trade time to America/New_York for the dedupe key.)
#[test]
fn skeptic_extreme_past_timestamp_does_not_panic() {
    let ts = DateTime::<Utc>::MIN_UTC; // "-262143-01-01T00:00:00Z" parses to this
    let t = Trade {
        trade_id: 1,
        symbol: "G".into(),
        price: 5.0,
        size: 1,
        timestamp: ts,
        exchange: None,
        tape: None,
    };
    let r = std::panic::catch_unwind(move || {
        let mut d = Detector::new(cfg(600, 600));
        d.observe(&t, t0()).len()
    });
    assert!(
        r.is_ok(),
        "Detector::observe panicked on a parseable timestamp"
    );
}

// ---- Follow-up pass: edges of the fixes (parse_trade bounds, symbol bounds) ----

fn parse_at(t: &str) -> spike_detector::kafka::Parsed {
    let js = format!(r#"{{"i":1,"S":"A","p":1.0,"s":1,"t":"{t}"}}"#);
    // Reference far in the future so only the year bounds matter here.
    let reference = DateTime::parse_from_rfc3339("2099-12-31T23:59:59Z")
        .unwrap()
        .with_timezone(&Utc);
    spike_detector::kafka::parse_trade(Some(js.as_bytes()), reference, 3600)
}

#[test]
fn skeptic_year_bounds_are_exact() {
    use spike_detector::kafka::Parsed;
    let ok = |t| matches!(parse_at(t), Parsed::Trade(_));
    assert!(!ok("1999-12-31T23:59:59.999999999Z"));
    assert!(ok("2000-01-01T00:00:00Z"));
    assert!(ok("2099-12-31T23:59:59Z"));
    assert!(!ok("2100-01-01T00:00:00Z"));
    assert!(!ok("+262142-12-31T23:59:59Z"));
    assert!(!ok("-262143-01-01T00:00:00Z"));
    // Offsets that move the UTC instant across the bound.
    assert!(!ok("2000-01-01T00:00:00+01:00"));
    assert!(ok("1999-12-31T23:00:00-01:00"));
}

#[test]
fn skeptic_symbol_byte_bounds_and_odd_chars() {
    let now = t0();
    let mut accepted = Vec::new();
    let cases = [
        "X".repeat(32),        // 32 bytes: ok
        "X".repeat(33),        // 33: reject
        "€".repeat(10) + "XX", // non-ASCII: reject (symbols are ASCII graphic only)
        "€".repeat(11),        // 33 bytes: reject
        "AAPL\u{0007}".into(), // control: reject
        "AAPL\u{200B}".into(), // zero-width space: reject
        " AAPL".into(),        // leading space: reject
        "AAPL\u{202E}".into(), // RTL override: reject
    ];
    for (s, ok) in &cases {
        let mut d = Detector::new(cfg(600, 600));
        d.observe(&trade(s, 1, 100.0, 0), now);
        assert_eq!(d.symbol_count() == 1, *ok, "symbol {s:?}");
        assert_eq!(
            d.stats().invalid,
            u64::from(!ok),
            "symbol {s:?} counted invalid"
        );
        accepted.push(d.symbol_count());
    }
    // Only the 32-byte ASCII-graphic symbol is valid.
    assert_eq!(accepted, vec![0]);
}

#[test]
fn skeptic_trade_id_extremes() {
    let mut d = Detector::new(cfg(600, 600));
    d.observe(&trade("I", i64::MIN, 100.0, 0), t0());
    let e = d.observe(&trade("I", i64::MAX, 110.0, 1), t0());
    assert_eq!(e.len(), 1);
    assert!(e[0].event_id.starts_with(&format!("I-UP-{}-", i64::MAX)));
}

// ---- Round 3: processing-time eviction ----

/// Idle eviction now runs on PROCESSING time. If consumption stalls for longer than the
/// window (broker outage, long GC/pause, network partition) while event times do not move,
/// the first cleanup after the stall evicts every symbol not yet touched since the stall,
/// even though their windows and cooldowns are still current in event time.
/// (a) a baseline that is 5 s old in event time is gone, so a real spike is lost;
/// (b) a cooldown 3 s old in event time is pruned, so a second UP fires inside the cooldown.
#[test]
fn skeptic_processing_stall_keeps_event_time_state() {
    let mut d = Detector::new(cfg(600, 600));
    let p0 = t0() + Duration::days(1);
    // Before the stall.
    d.observe(&trade("BASE", 1, 100.0, 0), p0);
    d.observe(&trade("COOL", 1, 100.0, 0), p0);
    assert_eq!(d.observe(&trade("COOL", 2, 110.0, 1), p0).len(), 1);
    // Consumption resumes 11 minutes later (processing time); event times continue.
    let p1 = p0 + Duration::seconds(660);
    for i in 0..1100 {
        d.observe(&trade(&format!("F{}", i % 50), i + 1, 50.0, 2), p1);
    }
    let lost = d.observe(&trade("BASE", 2, 106.0, 5), p1);
    let mut dup = d.observe(&trade("COOL", 3, 100.0, 3), p1);
    dup.extend(d.observe(&trade("COOL", 4, 110.0, 4), p1));
    assert_eq!(
        lost.len(),
        1,
        "BASE: 6% move 5 s after baseline not detected"
    );
    assert!(
        dup.is_empty(),
        "COOL: UP re-emitted 3 s (event time) after the first UP: {:?}",
        ids(&dup)
    );
}
