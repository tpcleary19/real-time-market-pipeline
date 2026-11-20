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
    /// Latest injected processing time (`detected_at`); drives idle-symbol cleanup, so one
    /// symbol's event times can never affect another symbol's state.
    proc_now: Option<DateTime<Utc>>,
    observed: u64,
    stats: Stats,
}

/// Longest accepted symbol, in bytes. Keeps every event far below Kafka's message size limit.
pub const MAX_SYMBOL_BYTES: usize = 32;

/// 2000-01-01T00:00:00Z and 2100-01-01T00:00:00Z. Trade times outside this range are treated
/// as garbage and dropped, which also keeps all chrono arithmetic far from its limits.
const MIN_TRADE_SECS: i64 = 946_684_800;
const MAX_TRADE_SECS: i64 = 4_102_444_800;

/// True if `ts` is a plausible trade time (year 2000..2100).
pub fn timestamp_in_range(ts: DateTime<Utc>) -> bool {
    (MIN_TRADE_SECS..MAX_TRADE_SECS).contains(&ts.timestamp())
}

/// 1..=32 bytes, ASCII graphic characters only (0x21..=0x7E). Rejects spaces, control and
/// all non-ASCII characters (zero-width, RTL, lookalikes). Lowercase and `.`/`-`/`/` are fine.
fn valid_symbol(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_SYMBOL_BYTES && s.bytes().all(|b| (0x21..=0x7E).contains(&b))
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
            proc_now: None,
            observed: 0,
            stats: Stats::default(),
        }
    }

    /// Feed one trade. Returns 0 or 1 events (up and down cannot both hold against one baseline).
    /// Invalid, duplicate and out-of-order trades are dropped and counted; this never panics.
    pub fn observe(&mut self, trade: &Trade, detected_at: DateTime<Utc>) -> Vec<SpikeEvent> {
        if !valid_symbol(&trade.symbol)
            || !trade.price.is_finite()
            || trade.price <= 0.0
            || !timestamp_in_range(trade.timestamp)
        {
            self.stats.invalid += 1;
            return Vec::new();
        }
        self.observed += 1;
        let proc_now = self.proc_now.map_or(detected_at, |p| p.max(detected_at));
        self.proc_now = Some(proc_now);
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
        win.touch(proc_now);

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
            .try_fire(&trade.symbol, direction, trade.timestamp, proc_now)
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

    /// Drop symbols with no ACCEPTED trade for more than `window` of processing time, then
    /// prune finished cooldowns per symbol. Event times of other symbols play no role, so a
    /// symbol poisoned by a future-dated trade recovers on its own.
    fn cleanup(&mut self) {
        let Some(now) = self.proc_now else { return };
        let cutoff = now
            .checked_sub_signed(self.window)
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        let before = self.windows.len();
        self.windows
            .retain(|_, w| w.last_activity().is_some_and(|a| a >= cutoff));
        self.stats.evicted_symbols += (before - self.windows.len()) as u64;
        let windows = &self.windows;
        self.cooldown
            .prune(now, &|s| windows.get(s).and_then(|w| w.latest()));
    }

    /// Highest event time of any retained symbol (reference for sanity-checking seeds).
    pub fn max_event_time(&self) -> Option<DateTime<Utc>> {
        self.windows.values().filter_map(|w| w.latest()).max()
    }

    /// Forget all cooldown state (used before seeding it from already-published events).
    pub fn reset_cooldowns(&mut self) {
        self.cooldown.clear();
    }

    /// Record that an event for `(symbol, direction)` was already published with the given
    /// event-time `current_timestamp`, so replay does not emit another one inside the cooldown.
    pub fn seed_cooldown(&mut self, symbol: &str, direction: Direction, at: DateTime<Utc>) {
        self.cooldown.seed(symbol, direction, at);
    }

    /// Like [`Self::seed_cooldown`] but replaces the existing entry even if it is later (the
    /// published event is the authority; a replay-derived phase may be wrong).
    pub fn override_cooldown(&mut self, symbol: &str, direction: Direction, at: DateTime<Utc>) {
        self.cooldown.set(symbol, direction, at);
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
    fn rejects_out_of_range_time_and_long_symbols_without_panic() {
        let mut d = Detector::new(cfg());
        for secs in [i64::MIN / 1000, -10_000_000_000, 0, 5_000_000_000] {
            let mut tr = t("A", 1, 100.0, 0);
            tr.timestamp = DateTime::from_timestamp(secs.max(-62_135_596_800), 0)
                .unwrap_or(DateTime::<Utc>::MIN_UTC);
            assert!(d.observe(&tr, now()).is_empty());
        }
        let mut tr = t("A", 2, 100.0, 0);
        tr.timestamp = DateTime::<Utc>::MIN_UTC;
        assert!(d.observe(&tr, now()).is_empty());
        tr.timestamp = DateTime::<Utc>::MAX_UTC;
        assert!(d.observe(&tr, now()).is_empty());
        assert!(
            d.observe(&t(&"X".repeat(MAX_SYMBOL_BYTES + 1), 3, 100.0, 0), now())
                .is_empty()
        );
        assert_eq!(d.stats().invalid, 7);
        assert_eq!(d.symbol_count(), 0);
        d.observe(&t(&"x".repeat(MAX_SYMBOL_BYTES), 4, 100.0, 0), now());
        assert_eq!(d.symbol_count(), 1);
    }

    #[test]
    fn seeded_cooldown_suppresses_republish() {
        let mut d = Detector::new(cfg());
        d.observe(&t("A", 1, 100.0, 0), now());
        d.seed_cooldown("A", Direction::Up, t("A", 0, 1.0, 5).timestamp);
        assert!(d.observe(&t("A", 2, 110.0, 6), now()).is_empty());
        assert_eq!(d.stats().suppressed_by_cooldown, 1);
        d.reset_cooldowns();
        assert_eq!(d.observe(&t("A", 3, 111.0, 7), now()).len(), 1);
    }

    #[test]
    fn symbol_charset_ascii_graphic_only() {
        for (sym, ok) in [
            ("AAPL", true),
            ("brk.b", true),
            ("BRK/B", true),
            (" AAPL", false),
            ("AA PL", false),
            ("AAPL\u{200B}", false),
            ("AAPL\u{202E}", false),
            ("\u{20AC}", false),
            ("A\u{7f}", false),
        ] {
            let mut d = Detector::new(cfg());
            d.observe(&t(sym, 1, 100.0, 0), now());
            assert_eq!(d.symbol_count() == 1, ok, "{sym:?}");
        }
    }

    #[test]
    fn future_dated_symbol_does_not_affect_others_and_recovers_by_processing_time() {
        let mut d = Detector::new(cfg());
        let p0 = now();
        d.observe(&t("QQQ", 1, 100.0, 0), p0);
        d.observe(&t("POIS", 1, 1.0, 100_000), p0); // far ahead in event time
        // 1100 trades on other symbols force cleanup passes (>1024 observations).
        for i in 0..1100 {
            d.observe(&t(&format!("F{}", i % 50), i + 1, 50.0, 1), p0);
        }
        assert_eq!(
            d.observe(&t("QQQ", 2, 110.0, 5), p0).len(),
            1,
            "QQQ unaffected"
        );
        // Real trades on POIS are late and do not refresh activity.
        assert!(d.observe(&t("POIS", 2, 100.0, 10), p0).is_empty());
        assert_eq!(d.stats().out_of_order, 1);
        // After > window of processing time with nothing accepted for POIS it is evicted.
        let later = p0 + Duration::seconds(601);
        for i in 0..1100 {
            d.observe(&t(&format!("F{}", i % 50), 5000 + i, 50.0, 2), later);
        }
        d.observe(&t("POIS", 3, 100.0, 20), later);
        let ev = d.observe(&t("POIS", 4, 110.0, 21), later);
        assert_eq!(ev.len(), 1, "POIS recovered");
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
