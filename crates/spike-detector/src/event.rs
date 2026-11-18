//! Versioned `price-spikes` event contract (schema_version = 1). LEAD-OWNED: do not change without lead approval.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Direction {
    Up,
    Down,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Up => "UP",
            Direction::Down => "DOWN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpikeEvent {
    pub schema_version: u32,
    pub event_id: String,
    pub symbol: String,
    pub direction: Direction,
    pub baseline_price: f64,
    pub current_price: f64,
    pub percentage_change: f64,
    pub window_seconds: u64,
    pub baseline_timestamp: DateTime<Utc>,
    pub current_timestamp: DateTime<Utc>,
    pub detected_at: DateTime<Utc>,
}

/// Stable across replays: derived only from event-time data, never wall clock.
pub fn event_id(symbol: &str, direction: Direction, current_trade_id: i64, current_ts: DateTime<Utc>) -> String {
    format!(
        "{symbol}-{}-{current_trade_id}-{}",
        direction.as_str(),
        current_ts.timestamp_nanos_opt().unwrap_or_default()
    )
}
