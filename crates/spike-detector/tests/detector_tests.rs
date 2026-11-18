//! Unit tests for the pure detection core (assignment section 8, items 1-13, 15).
mod common;
use common::*;
use spike_detector::detector::{Config, Detector};
use spike_detector::{Direction, event::event_id};

fn det() -> Detector {
    Detector::new(cfg())
}

// 1
#[test]
fn upward_spike_detected() {
    let mut d = det();
    assert!(feed(&mut d, &trade("AAPL", 1, 100.0, 0)).is_empty());
    let ev = feed(&mut d, &trade("AAPL", 2, 106.0, 30));
    assert_eq!(ev.len(), 1);
    let e = &ev[0];
    assert_eq!(e.direction, Direction::Up);
    assert_eq!(e.symbol, "AAPL");
    assert_eq!(e.baseline_price, 100.0);
    assert_eq!(e.current_price, 106.0);
    assert!((e.percentage_change - 6.0).abs() < 1e-9);
    assert_eq!(e.window_seconds, 600);
    assert_eq!(e.baseline_timestamp, at(0));
    assert_eq!(e.current_timestamp, at(30));
    assert_eq!(
        e.detected_at,
        detected(),
        "detected_at must be the injected value"
    );
    assert_eq!(e.schema_version, 1);
    assert_eq!(e.event_id, event_id("AAPL", Direction::Up, 2, at(30)));
}

// 2
#[test]
fn downward_spike_detected() {
    let mut d = det();
    feed(&mut d, &trade("AAPL", 1, 100.0, 0));
    let ev = feed(&mut d, &trade("AAPL", 2, 94.0, 30));
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].direction, Direction::Down);
    assert!((ev[0].percentage_change + 6.0).abs() < 1e-9);
}

// 3
#[test]
fn below_threshold_no_event() {
    let mut d = det();
    feed(&mut d, &trade("AAPL", 1, 100.0, 0));
    assert!(feed(&mut d, &trade("AAPL", 2, 104.99, 10)).is_empty());
    assert!(feed(&mut d, &trade("AAPL", 3, 95.01, 20)).is_empty());
}

// 4: boundary inclusive (lead decision 2)
#[test]
fn exact_threshold_is_inclusive_up_and_down() {
    let mut d = det();
    feed(&mut d, &trade("UP", 1, 100.0, 0));
    assert_eq!(
        feed(&mut d, &trade("UP", 2, 105.0, 1)).len(),
        1,
        "+5.0% exactly must fire"
    );
    let mut d = det();
    feed(&mut d, &trade("DN", 1, 100.0, 0));
    assert_eq!(
        feed(&mut d, &trade("DN", 2, 95.0, 1)).len(),
        1,
        "-5.0% exactly must fire"
    );
}

#[test]
fn just_under_threshold_does_not_fire() {
    let mut d = det();
    feed(&mut d, &trade("X", 1, 100.0, 0));
    assert!(feed(&mut d, &trade("X", 2, 104.9999, 1)).is_empty());
}

// 5: window boundary t >= current_t - window is inclusive
#[test]
fn baseline_at_exact_window_edge_still_counts() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("A", 2, 110.0, 600)).len(), 1);
}

#[test]
fn expired_baseline_is_not_used() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    // 601s later the first trade has left the window; new trade is its own baseline.
    assert!(feed(&mut d, &trade("A", 2, 110.0, 601)).is_empty());
}

#[test]
fn baseline_advances_to_earliest_remaining_trade() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    feed(&mut d, &trade("A", 2, 102.0, 300));
    // At t=700 trade 1 expired; earliest remaining is trade 2 (102.0). 107.0 is +4.9% vs 102.
    assert!(feed(&mut d, &trade("A", 3, 107.0, 700)).is_empty());
    // 107.2 vs 102 = +5.098% => fires, with baseline from trade 2.
    let ev = feed(&mut d, &trade("A", 4, 107.2, 701));
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].baseline_price, 102.0);
    assert_eq!(ev[0].baseline_timestamp, at(300));
}

#[test]
fn expiry_bounds_retained_state() {
    let mut d = det();
    for i in 0..100 {
        feed(&mut d, &trade("A", i, 100.0, i));
    }
    assert!(d.retained_trades() <= 100);
    feed(&mut d, &trade("A", 1000, 100.0, 5000));
    assert!(
        d.retained_trades() <= 2,
        "old trades must be evicted, got {}",
        d.retained_trades()
    );
}

// 6
#[test]
fn spike_detected_without_full_window_elapsed() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("A", 2, 120.0, 1)).len(), 1);
}

#[test]
fn single_trade_never_spikes() {
    let mut d = det();
    assert!(feed(&mut d, &trade("A", 1, 100.0, 0)).is_empty());
}

// 7
#[test]
fn symbols_are_isolated() {
    let mut d = det();
    feed(&mut d, &trade("AAA", 1, 100.0, 0));
    feed(&mut d, &trade("BBB", 1, 50.0, 0));
    // A big move in AAA must not use BBB's baseline and must not affect BBB.
    assert_eq!(feed(&mut d, &trade("AAA", 2, 110.0, 5)).len(), 1);
    assert!(feed(&mut d, &trade("BBB", 2, 50.5, 6)).is_empty());
    // Cooldown on AAA UP does not suppress BBB UP.
    let ev = feed(&mut d, &trade("BBB", 3, 55.0, 7));
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].symbol, "BBB");
    assert_eq!(d.symbol_count(), 2);
}

#[test]
fn new_symbol_at_cap_is_rejected_without_eviction() {
    let mut d = Detector::new(Config {
        max_symbols: 2,
        ..cfg()
    });
    feed(&mut d, &trade("A", 1, 100.0, 0));
    feed(&mut d, &trade("B", 1, 100.0, 1));
    assert!(feed(&mut d, &trade("C", 1, 100.0, 2)).is_empty());
    assert_eq!(d.symbol_count(), 2);
    // C was rejected, so it never got a baseline.
    assert!(feed(&mut d, &trade("C", 2, 120.0, 3)).is_empty());
    assert_eq!(d.symbol_count(), 2);
    // Existing symbols are unaffected.
    assert_eq!(feed(&mut d, &trade("A", 2, 110.0, 4)).len(), 1);
}

#[test]
fn idle_symbols_are_cleaned_up_and_free_capacity() {
    let mut d = Detector::new(Config {
        max_symbols: 2,
        ..cfg()
    });
    feed(&mut d, &trade("A", 1, 100.0, 0));
    feed(&mut d, &trade("B", 1, 100.0, 1));
    // B stays active; A idle for > window.
    feed(&mut d, &trade("B", 2, 100.0, 700));
    feed(&mut d, &trade("C", 1, 100.0, 701));
    assert!(d.symbol_count() <= 2);
    // C must have been admitted: a spike on it is detected.
    assert_eq!(feed(&mut d, &trade("C", 2, 110.0, 702)).len(), 1);
}

// 8
#[test]
fn cooldown_suppresses_repeat_same_direction() {
    let mut d = det();
    feed(&mut d, &trade("AAPL", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("AAPL", 2, 105.2, 10)).len(), 1);
    assert!(feed(&mut d, &trade("AAPL", 3, 105.5, 40)).is_empty());
    assert!(feed(&mut d, &trade("AAPL", 4, 108.0, 100)).is_empty());
}

#[test]
fn cooldown_is_per_direction() {
    let mut d = det();
    feed(&mut d, &trade("AAPL", 1, 100.0, 0));
    let up = feed(&mut d, &trade("AAPL", 2, 106.0, 10));
    assert_eq!(up[0].direction, Direction::Up);
    // Falls below baseline by >5% while UP cooldown is active: DOWN evaluated independently.
    let down = feed(&mut d, &trade("AAPL", 3, 94.0, 20));
    assert_eq!(down.len(), 1);
    assert_eq!(down[0].direction, Direction::Down);
}

// 9: use a wide window so the baseline survives past the (short) cooldown.
#[test]
fn cooldown_expires_in_event_time() {
    let mut d = Detector::new(Config {
        window_seconds: 10_000,
        cooldown_seconds: 100,
        ..cfg()
    });
    feed(&mut d, &trade("A", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("A", 2, 106.0, 10)).len(), 1);
    assert!(feed(&mut d, &trade("A", 3, 107.0, 50)).is_empty());
    let ev = feed(&mut d, &trade("A", 4, 108.0, 500));
    assert_eq!(
        ev.len(),
        1,
        "cooldown elapsed in event time; new event allowed"
    );
    assert_eq!(ev[0].current_price, 108.0);
}

#[test]
fn cooldown_ignores_wall_clock_detected_at() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    assert_eq!(d.observe(&trade("A", 2, 106.0, 10), detected()).len(), 1);
    // Wildly later detected_at must not expire the cooldown; only event time counts.
    let far = detected() + chrono::Duration::days(30);
    assert!(d.observe(&trade("A", 3, 107.0, 20), far).is_empty());
}

#[test]
fn cooldown_boundary_is_exact() {
    let c = Config {
        window_seconds: 10_000,
        cooldown_seconds: 100,
        ..cfg()
    };
    let run = |second_at: i64| {
        let mut d = Detector::new(c.clone());
        feed(&mut d, &trade("A", 1, 100.0, 0));
        assert_eq!(feed(&mut d, &trade("A", 2, 106.0, 10)).len(), 1);
        feed(&mut d, &trade("A", 3, 107.0, second_at)).len()
    };
    assert_eq!(run(10 + 99), 0, "elapsed < cooldown must suppress");
    assert_eq!(run(10 + 100), 1, "elapsed == cooldown must publish");
}

// 10
#[test]
fn duplicate_trade_is_ignored() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    let t = trade("A", 2, 106.0, 10);
    assert_eq!(feed(&mut d, &t).len(), 1);
    let before = d.retained_trades();
    assert!(feed(&mut d, &t).is_empty());
    assert_eq!(
        d.retained_trades(),
        before,
        "duplicate must not be retained twice"
    );
}

#[test]
fn duplicate_does_not_emit_even_with_no_cooldown() {
    let mut d = Detector::new(Config {
        cooldown_seconds: 0,
        ..cfg()
    });
    feed(&mut d, &trade("A", 1, 100.0, 0));
    let t = trade("A", 2, 106.0, 10);
    assert_eq!(feed(&mut d, &t).len(), 1);
    assert!(
        feed(&mut d, &t).is_empty(),
        "redelivery of same trade must not re-emit"
    );
}

#[test]
fn same_trade_id_on_different_et_date_is_not_duplicate() {
    // Wide window so the baseline survives across the date change.
    let mut d = Detector::new(Config {
        window_seconds: 200_000,
        cooldown_seconds: 0,
        ..cfg()
    });
    feed(&mut d, &trade("A", 1, 100.0, 0)); // 2024-01-02 ET
    // Same id one day later (2024-01-03 ET): a new trade, must be processed.
    assert_eq!(feed(&mut d, &trade("A", 1, 110.0, 86_400)).len(), 1);
}

#[test]
fn same_trade_id_different_symbol_is_not_duplicate() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    feed(&mut d, &trade("B", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("B", 2, 110.0, 1)).len(), 1);
    assert_eq!(d.symbol_count(), 2);
}

// 11
#[test]
fn out_of_order_trade_dropped() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 100));
    feed(&mut d, &trade("A", 2, 101.0, 200));
    let before = d.retained_trades();
    // Older than latest (200) and would be a huge spike if used: must be dropped.
    assert!(feed(&mut d, &trade("A", 3, 200.0, 150)).is_empty());
    assert_eq!(d.retained_trades(), before);
}

#[test]
fn out_of_order_does_not_disturb_later_detection() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 100));
    feed(&mut d, &trade("A", 2, 100.0, 200));
    feed(&mut d, &trade("A", 3, 1.0, 50)); // late, dropped
    let ev = feed(&mut d, &trade("A", 4, 106.0, 210));
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].baseline_price, 100.0);
    assert_eq!(ev[0].baseline_timestamp, at(100));
}

#[test]
fn equal_timestamp_trades_are_not_late() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 10));
    // Same event time, different id: not older than latest, must be processed.
    assert_eq!(feed(&mut d, &trade("A", 2, 110.0, 10)).len(), 1);
}

// 12
#[test]
fn invalid_trades_rejected_without_panic() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 100.0, 0));
    let before = d.retained_trades();
    let bad = [
        trade("A", 2, 0.0, 1),
        trade("A", 3, -5.0, 2),
        trade("A", 4, f64::NAN, 3),
        trade("A", 5, f64::INFINITY, 4),
        trade("A", 6, f64::NEG_INFINITY, 5),
        trade("", 7, 100.0, 6),
        trade("   ", 8, 100.0, 7),
        trade("A\u{0}B", 11, 100.0, 10),
    ];
    for t in &bad {
        assert!(
            feed(&mut d, t).is_empty(),
            "invalid trade {:?} emitted an event",
            t.symbol
        );
    }
    assert_eq!(
        d.retained_trades(),
        before,
        "invalid trades must not be retained"
    );
    assert_eq!(d.symbol_count(), 1, "invalid symbols must not create state");
}

#[test]
fn dotted_and_mixed_tickers_are_valid() {
    let mut d = det();
    feed(&mut d, &trade("BRK.B", 1, 100.0, 0));
    assert_eq!(feed(&mut d, &trade("BRK.B", 2, 106.0, 1)).len(), 1);
    assert_eq!(d.symbol_count(), 1);
}

#[test]
fn invalid_trade_does_not_poison_baseline() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 0.0, 0)); // rejected
    feed(&mut d, &trade("A", 2, 100.0, 1));
    assert_eq!(feed(&mut d, &trade("A", 3, 106.0, 2)).len(), 1);
}

#[test]
fn large_price_gap_still_single_event() {
    let mut d = det();
    feed(&mut d, &trade("A", 1, 0.01, 0));
    let ev = feed(&mut d, &trade("A", 2, 1_000_000.0, 1));
    assert_eq!(ev.len(), 1);
    assert!(ev[0].percentage_change.is_finite());
}

// 13
#[test]
fn event_id_is_stable_and_discriminating() {
    let a = event_id("AAPL", Direction::Up, 7, at(5));
    assert_eq!(a, event_id("AAPL", Direction::Up, 7, at(5)));
    assert_ne!(a, event_id("AAPL", Direction::Down, 7, at(5)));
    assert_ne!(a, event_id("AAPL", Direction::Up, 8, at(5)));
    assert_ne!(a, event_id("AAPL", Direction::Up, 7, at(6)));
    assert_ne!(a, event_id("MSFT", Direction::Up, 7, at(5)));
}

#[test]
fn event_id_independent_of_detected_at() {
    let run = |det_at| {
        let mut d = det();
        d.observe(&trade("A", 1, 100.0, 0), det_at);
        d.observe(&trade("A", 2, 106.0, 5), det_at)
    };
    let a = run(detected());
    let b = run(detected() + chrono::Duration::hours(5));
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].event_id, b[0].event_id);
}

// 15: warm-up / restart. Replay determinism is what the warm-up design relies on.
fn stream() -> Vec<market_events::Trade> {
    vec![
        trade("A", 1, 100.0, 0),
        trade("A", 2, 106.0, 10),
        trade("A", 3, 107.0, 40),
        trade("B", 1, 50.0, 5),
        trade("B", 2, 46.0, 50),
        trade("A", 4, 93.0, 80),
    ]
}

#[test]
fn replay_is_deterministic() {
    let a = feed_all(&mut det(), &stream());
    let b = feed_all(&mut det(), &stream());
    assert_eq!(a, b);
    assert_eq!(a.len(), 3); // A UP, B DOWN, A DOWN
}

#[test]
fn replay_with_different_wall_clock_yields_same_event_ids() {
    let s = stream();
    let mut d1 = det();
    let ids1: Vec<_> = s
        .iter()
        .flat_map(|t| d1.observe(t, detected()))
        .map(|e| e.event_id)
        .collect();
    let mut d2 = det();
    let later = detected() + chrono::Duration::days(1);
    let ids2: Vec<_> = s
        .iter()
        .flat_map(|t| d2.observe(t, later))
        .map(|e| e.event_id)
        .collect();
    assert_eq!(ids1, ids2);
}

#[test]
fn warm_up_restores_cooldown_so_no_duplicate_after_restart() {
    let s = stream();
    // Original process saw the whole stream.
    let full = feed_all(&mut det(), &s);
    // Restarted process replays the same history, then continues with new trades.
    let mut restarted = det();
    let replayed = feed_all(&mut restarted, &s);
    assert_eq!(full, replayed);
    // Rebuilt cooldown suppresses a same-direction spike right after warm-up.
    assert!(feed(&mut restarted, &trade("A", 5, 108.0, 90)).is_empty());
}

#[test]
fn redelivered_history_after_replay_emits_nothing() {
    let s = stream();
    let mut d = det();
    feed_all(&mut d, &s);
    // At-least-once: Kafka redelivers records after a crash before offset store.
    assert!(feed_all(&mut d, &s).is_empty());
}
