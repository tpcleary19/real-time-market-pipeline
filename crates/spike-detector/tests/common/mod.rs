//! Deterministic fixtures. All times are event time; nothing reads the wall clock.
#![allow(dead_code)]
use chrono::{DateTime, TimeZone, Utc};
use market_events::Trade;
use spike_detector::SpikeEvent;
use spike_detector::detector::{Config, Detector};

/// 2024-01-02 15:00:00Z = 10:00 ET (a Tuesday, mid-session).
pub fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2024, 1, 2, 15, 0, 0).unwrap()
}

pub fn at(offset_secs: i64) -> DateTime<Utc> {
    t0() + chrono::Duration::seconds(offset_secs)
}

/// Fixed injected "processing" time, deliberately unrelated to event time.
pub fn detected() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 6, 1, 0, 0, 0).unwrap()
}

pub fn cfg() -> Config {
    Config {
        window_seconds: 600,
        threshold_percent: 5.0,
        cooldown_seconds: 600,
        max_symbols: 1000,
    }
}

pub fn trade_at(sym: &str, id: i64, price: f64, ts: DateTime<Utc>) -> Trade {
    Trade {
        trade_id: id,
        symbol: sym.to_string(),
        price,
        size: 100,
        timestamp: ts,
        exchange: Some("V".into()),
        tape: Some("C".into()),
    }
}

pub fn trade(sym: &str, id: i64, price: f64, offset_secs: i64) -> Trade {
    trade_at(sym, id, price, at(offset_secs))
}

pub fn feed(d: &mut Detector, t: &Trade) -> Vec<SpikeEvent> {
    d.observe(t, detected())
}

/// Feed a list, collecting every emitted event in order.
pub fn feed_all(d: &mut Detector, trades: &[Trade]) -> Vec<SpikeEvent> {
    trades.iter().flat_map(|t| feed(d, t)).collect()
}
