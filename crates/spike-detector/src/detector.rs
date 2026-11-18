//! Pure detection core. API CONTRACT (lead-defined, implementation agent fills in):
//!
//!   Detector::new(Config) -> Detector
//!   Detector::observe(&mut self, trade: &Trade, detected_at: DateTime<Utc>) -> Vec<SpikeEvent>
//!   Detector::symbol_count(&self) -> usize      // retained-state visibility
//!   Detector::retained_trades(&self) -> usize   // retained-state visibility
//!
//! Time is event-time only; `detected_at` is injected so tests are deterministic.
use crate::cooldown::Cooldown;
use crate::event::{Direction, SCHEMA_VERSION, SpikeEvent, event_id};
use crate::window::{Admit, Entry, SymbolWindow};
use chrono::{DateTime, Duration, Utc};
use market_events::Trade;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Config {
    pub window_seconds: u64,    // default 600
    pub threshold_percent: f64, // default 5.0
    pub cooldown_seconds: u64,  // default 600
    pub max_symbols: usize,
}

/// Tolerance for the inclusive threshold comparison (floating-point noise only).
const EPSILON: f64 = 1e-9;
/// Run idle-state cleanup every this many observed trades.
const CLEANUP_EVERY: u64 = 1024;

/// Monotonic counters, readable via [`Detector::stats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub trades_accepted: u64,
    pub invalid: u64,
    pub duplicates: u64,
    pub out_of_order: u64,
    pub spikes_detected: u64,
    pub suppressed_by_cooldown: u64,
    pub dropped_symbol_capacity: u64,
    pub evicted_symbols: u64,
    /// Trades discarded early because a symbol exceeded the per-symbol retention cap.
    pub evicted_by_cap: u64,
}

pub struct Detector {
    config: Config,
    window: Duration,
    windows: HashMap<String, SymbolWindow>,
    cooldown: Cooldown,
    /// Highest event time accepted for any symbol; drives idle cleanup.
    watermark: Option<DateTime<Utc>>,
    observed: u64,
    stats: Stats,
}

fn valid_symbol(s: &str) -> bool {
    !s.trim().is_empty() && !s.chars().any(char::is_control)
}

impl Detector {
    pub fn new(config: Config) -> Self {
        let window = Duration::seconds(config.window_seconds.min(i64::MAX as u64 / 1000) as i64);
        let cooldown =
            Duration::seconds(config.cooldown_seconds.min(i64::MAX as u64 / 1000) as i64);
        Self {
            config,
            window,
            windows: HashMap::new(),
            cooldown: Cooldown::new(cooldown),
            watermark: None,
            observed: 0,
            stats: Stats::default(),
        }
    }

    /// Feed one trade. Returns 0 or 1 events (up and down cannot both hold against one baseline).
    /// Invalid, duplicate and out-of-order trades are dropped and counted; this never panics.
    pub fn observe(&mut self, trade: &Trade, detected_at: DateTime<Utc>) -> Vec<SpikeEvent> {
        if !valid_symbol(&trade.symbol) || !trade.price.is_finite() || trade.price <= 0.0 {
            self.stats.invalid += 1;
            return Vec::new();
        }
        self.observed += 1;
        if self.observed.is_multiple_of(CLEANUP_EVERY) {
            self.cleanup();
        }
        if !self.windows.contains_key(&trade.symbol)
            && self.windows.len() >= self.config.max_symbols
        {
            self.cleanup();
            if self.windows.len() >= self.config.max_symbols {
                self.stats.dropped_symbol_capacity += 1;
                return Vec::new();
            }
        }

        let win = self.windows.entry(trade.symbol.clone()).or_default();
        let entry = Entry {
            ts: trade.timestamp,
            price: trade.price,
            trade_id: trade.trade_id,
        };
        match win.push(entry, self.window) {
            Admit::Duplicate => {
                self.stats.duplicates += 1;
                return Vec::new();
            }
            Admit::OutOfOrder => {
                self.stats.out_of_order += 1;
                return Vec::new();
            }
            Admit::Accepted => {}
        }
        self.stats.trades_accepted += 1;
        self.stats.evicted_by_cap += win.take_cap_evictions();
        self.watermark = Some(
            self.watermark
                .map_or(trade.timestamp, |w| w.max(trade.timestamp)),
        );

        let Some(base) = win.baseline() else {
            return Vec::new();
        };
        let pct = (trade.price - base.price) / base.price * 100.0;
        if !pct.is_finite() || pct.abs() + EPSILON < self.config.threshold_percent {
            return Vec::new();
        }
        let direction = if pct > 0.0 {
            Direction::Up
        } else {
            Direction::Down
        };
        self.stats.spikes_detected += 1;
        if !self
            .cooldown
            .try_fire(&trade.symbol, direction, trade.timestamp)
        {
            self.stats.suppressed_by_cooldown += 1;
            return Vec::new();
        }
        vec![SpikeEvent {
            schema_version: SCHEMA_VERSION,
            event_id: event_id(&trade.symbol, direction, trade.trade_id, trade.timestamp),
            symbol: trade.symbol.clone(),
            direction,
            baseline_price: base.price,
            current_price: trade.price,
            percentage_change: (pct * 1e4).round() / 1e4,
            window_seconds: self.config.window_seconds,
            baseline_timestamp: base.ts,
            current_timestamp: trade.timestamp,
            detected_at,
        }]
    }

    /// Drop symbols with no trade inside the window (relative to the global event-time
    /// watermark) and expired cooldown entries.
    fn cleanup(&mut self) {
        let Some(wm) = self.watermark else { return };
        let cutoff = wm - self.window;
        let before = self.windows.len();
        self.windows
            .retain(|_, w| w.latest().is_some_and(|l| l >= cutoff));
        self.stats.evicted_symbols += (before - self.windows.len()) as u64;
        self.cooldown.prune(wm);
    }

    pub fn symbol_count(&self) -> usize {
        self.windows.len()
    }

    pub fn retained_trades(&self) -> usize {
        self.windows.values().map(SymbolWindow::len).sum()
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config {
            window_seconds: 600,
            threshold_percent: 5.0,
            cooldown_seconds: 600,
            max_symbols: 100,
        }
    }
    fn t(sym: &str, id: i64, price: f64, secs: i64) -> Trade {
        Trade {
            trade_id: id,
            symbol: sym.into(),
            price,
            size: 1,
            timestamp: DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap(),
            exchange: None,
            tape: None,
        }
    }
    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    #[test]
    fn up_down_boundary_and_below() {
        let mut d = Detector::new(cfg());
        assert!(d.observe(&t("A", 1, 100.0, 0), now()).is_empty());
        assert!(d.observe(&t("A", 2, 104.99, 1), now()).is_empty());
        let up = d.observe(&t("A", 3, 105.0, 2), now());
        assert_eq!(up.len(), 1);
        assert_eq!(up[0].direction, Direction::Up);
        assert_eq!(up[0].percentage_change, 5.0);
        // Cooldown suppresses repeat, but DOWN is independent.
        assert!(d.observe(&t("A", 4, 110.0, 3), now()).is_empty());
        let down = d.observe(&t("A", 5, 94.0, 4), now());
        assert_eq!(down[0].direction, Direction::Down);
        assert_eq!(d.stats().suppressed_by_cooldown, 1);
    }

    #[test]
    fn invalid_dup_ooo_dropped() {
        let mut d = Detector::new(cfg());
        d.observe(&t("A", 1, 100.0, 10), now());
        d.observe(&t("", 2, 100.0, 11), now());
        d.observe(&t("A", 3, 0.0, 11), now());
        d.observe(&t("A", 4, f64::NAN, 11), now());
        d.observe(&t("A", 1, 100.0, 10), now());
        d.observe(&t("A", 5, 100.0, 5), now());
        let s = d.stats();
        assert_eq!((s.invalid, s.duplicates, s.out_of_order), (3, 1, 1));
    }

    #[test]
    fn per_symbol_cap_evictions_counted() {
        let mut d = Detector::new(cfg());
        let cap = crate::window::MAX_TRADES_PER_SYMBOL as i64;
        for i in 0..cap + 5 {
            d.observe(&t("A", i, 100.0, 0), now());
        }
        assert_eq!(d.stats().evicted_by_cap, 5);
        assert_eq!(d.retained_trades(), cap as usize);
    }

    #[test]
    fn idle_symbols_evicted_at_capacity() {
        let mut c = cfg();
        c.max_symbols = 1;
        let mut d = Detector::new(c);
        d.observe(&t("A", 1, 100.0, 0), now());
        d.observe(&t("B", 2, 100.0, 1), now()); // A still active: B dropped
        assert_eq!(d.stats().dropped_symbol_capacity, 1);
        d.observe(&t("A", 3, 100.0, 1000), now());
        // watermark advanced; A is the only symbol and active
        assert_eq!(d.symbol_count(), 1);
    }
}
