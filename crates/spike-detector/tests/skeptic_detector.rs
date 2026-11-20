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

/// Warm-up replays only `window + cooldown + 60 s` before the last committed record, with
/// an empty cooldown table. Under a sustained spike condition the cooldown "phase" in the
/// replayed state differs from the original run, so after the restart the service emits
/// spikes at different trades (different event_ids) than an uninterrupted run would.
/// Docs claim: "Event time was chosen so replay and warm-up behave identically to live processing."
#[test]
fn skeptic_warm_up_cooldown_phase_matches_uninterrupted_run() {
    let (window, cooldown) = (60u64, 60u64);
    let trades = trend(100); // t = 0 .. 990 s
    let now = t0();

    // Uninterrupted run.
    let mut live = Detector::new(cfg(window, cooldown));
    let all: Vec<SpikeEvent> = trades.iter().flat_map(|t| live.observe(t, now)).collect();

    // Restart with committed offset C = 51 (last handled record index 50, t = 500 s).
    let c = 51usize;
    let lookback = (window + cooldown + 60) as i64;
    let last_ts = trades[c - 1].timestamp;
    let mut restarted = Detector::new(cfg(window, cooldown));
    for t in &trades[..c] {
        if t.timestamp >= last_ts - Duration::seconds(lookback) {
            let _ = restarted.observe(t, now); // warm-up: discarded
        }
    }
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
    let lookback = (window + cooldown + 60) as i64;
    let last_ts = trades[k - 1].timestamp;
    let mut restarted = Detector::new(cfg(window, cooldown));
    for t in &trades[..k] {
        if t.timestamp >= last_ts - Duration::seconds(lookback) {
            let _ = restarted.observe(t, now);
        }
    }
    let replay: Vec<SpikeEvent> = trades[k..]
        .iter()
        .flat_map(|t| restarted.observe(t, now))
        .collect();
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
