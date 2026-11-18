//! Event contract (assignment section 8, item 14).
mod common;
use common::*;
use serde_json::{Value, json};
use spike_detector::{Direction, SCHEMA_VERSION, SpikeEvent, event::event_id};

fn sample() -> SpikeEvent {
    SpikeEvent {
        schema_version: SCHEMA_VERSION,
        event_id: event_id("AAPL", Direction::Up, 42, at(30)),
        symbol: "AAPL".into(),
        direction: Direction::Up,
        baseline_price: 100.0,
        current_price: 105.25,
        percentage_change: 5.25,
        window_seconds: 600,
        baseline_timestamp: at(0),
        current_timestamp: at(30),
        detected_at: detected(),
    }
}

#[test]
fn roundtrip() {
    let e = sample();
    let s = serde_json::to_string(&e).unwrap();
    let back: SpikeEvent = serde_json::from_str(&s).unwrap();
    assert_eq!(e, back);
}

#[test]
fn wire_format_field_names_and_types() {
    let v: Value = serde_json::to_value(sample()).unwrap();
    let o = v.as_object().unwrap();
    let mut keys: Vec<_> = o.keys().cloned().collect();
    keys.sort();
    let mut want = vec![
        "schema_version",
        "event_id",
        "symbol",
        "direction",
        "baseline_price",
        "current_price",
        "percentage_change",
        "window_seconds",
        "baseline_timestamp",
        "current_timestamp",
        "detected_at",
    ];
    want.sort();
    assert_eq!(keys, want);
    assert_eq!(v["schema_version"], json!(1));
    assert_eq!(v["direction"], json!("UP"));
    assert!(v["baseline_price"].is_number());
    assert!(v["current_price"].is_number());
    assert!(v["percentage_change"].is_number());
    assert_eq!(v["window_seconds"], json!(600));
    assert_eq!(v["baseline_timestamp"], json!("2024-01-02T15:00:00Z"));
}

#[test]
fn direction_serializes_uppercase() {
    assert_eq!(serde_json::to_string(&Direction::Down).unwrap(), "\"DOWN\"");
    assert_eq!(Direction::Up.as_str(), "UP");
    assert_eq!(Direction::Down.as_str(), "DOWN");
    assert!(serde_json::from_str::<Direction>("\"up\"").is_err());
}

#[test]
fn rejects_malformed_payloads_without_panic() {
    for bad in [
        "",
        "{}",
        "null",
        "[]",
        "{\"schema_version\":1}",
        "not json",
        "{\"direction\":\"SIDEWAYS\"}",
    ] {
        assert!(
            serde_json::from_str::<SpikeEvent>(bad).is_err(),
            "accepted: {bad}"
        );
    }
}

#[test]
fn event_id_format_is_stable() {
    // Pinned: changing this breaks consumer-side dedupe across versions.
    let id = event_id("AAPL", Direction::Up, 42, at(0));
    assert_eq!(id, "AAPL-UP-42-1704207600000000000");
}

#[test]
fn schema_version_is_one() {
    assert_eq!(SCHEMA_VERSION, 1);
}

#[test]
fn detector_event_roundtrips_through_json() {
    use spike_detector::detector::Detector;
    let mut d = Detector::new(cfg());
    feed(&mut d, &trade("AAPL", 1, 100.0, 0));
    let ev = feed(&mut d, &trade("AAPL", 2, 106.1234567, 5))
        .pop()
        .unwrap();
    let back: SpikeEvent = serde_json::from_str(&serde_json::to_string(&ev).unwrap()).unwrap();
    assert_eq!(ev, back);
    // Lead decision 7: percentage_change rounded to 4 dp.
    let scaled = ev.percentage_change * 10_000.0;
    assert!(
        (scaled - scaled.round()).abs() < 1e-6,
        "not 4dp: {}",
        ev.percentage_change
    );
}
