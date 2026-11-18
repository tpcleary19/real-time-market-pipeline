//! Pure detection core. API CONTRACT (lead-defined, implementation agent fills in):
//!
//!   Detector::new(Config) -> Detector
//!   Detector::observe(&mut self, trade: &Trade, detected_at: DateTime<Utc>) -> Vec<SpikeEvent>
//!   Detector::symbol_count(&self) -> usize      // retained-state visibility
//!   Detector::retained_trades(&self) -> usize   // retained-state visibility
//!
//! Time is event-time only; `detected_at` is injected so tests are deterministic.
use crate::event::SpikeEvent;
use chrono::{DateTime, Utc};
use market_events::Trade;

#[derive(Debug, Clone)]
pub struct Config {
    pub window_seconds: u64,    // default 600
    pub threshold_percent: f64, // default 5.0
    pub cooldown_seconds: u64,  // default 600
    pub max_symbols: usize,
}

pub struct Detector;

impl Detector {
    pub fn new(_config: Config) -> Self { todo!("implementation agent") }
    pub fn observe(&mut self, _trade: &Trade, _detected_at: DateTime<Utc>) -> Vec<SpikeEvent> { todo!("implementation agent") }
    pub fn symbol_count(&self) -> usize { todo!("implementation agent") }
    pub fn retained_trades(&self) -> usize { todo!("implementation agent") }
}
