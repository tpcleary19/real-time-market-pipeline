//! Per-symbol rolling window of accepted trades (event time).
use chrono::{DateTime, Duration, NaiveDate, Utc};
use chrono_tz::America::New_York;
use std::collections::{HashSet, VecDeque};

/// Hard cap on retained trades per symbol. If a symbol exceeds this inside one window the
/// oldest trades are discarded early (the baseline then moves forward). Bounds memory.
pub const MAX_TRADES_PER_SYMBOL: usize = 100_000;

/// Trade ids restart each trading day, so identity is `(ET calendar date, trade id)`.
type TradeKey = (NaiveDate, i64);

#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub ts: DateTime<Utc>,
    pub price: f64,
    pub trade_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    Accepted,
    Duplicate,
    OutOfOrder,
}

#[derive(Debug, Default)]
pub struct SymbolWindow {
    cap: Option<usize>,
    /// Processing time (injected clock) of the last ACCEPTED trade.
    last_activity: Option<DateTime<Utc>>,
    cap_evictions: u64,
    entries: VecDeque<(Entry, TradeKey)>,
    ids: HashSet<TradeKey>,
    latest: Option<DateTime<Utc>>,
}

impl SymbolWindow {
    /// Window with a custom per-symbol cap (tests); `default()` uses [`MAX_TRADES_PER_SYMBOL`].
    pub fn with_cap(cap: usize) -> Self {
        Self {
            cap: Some(cap),
            ..Self::default()
        }
    }

    pub fn touch(&mut self, at: DateTime<Utc>) {
        self.last_activity = Some(at);
    }

    pub fn last_activity(&self) -> Option<DateTime<Utc>> {
        self.last_activity
    }

    /// Number of trades discarded early by the cap since the last call.
    pub fn take_cap_evictions(&mut self) -> u64 {
        std::mem::take(&mut self.cap_evictions)
    }

    /// Try to add a trade. Trades older than the latest accepted trade are rejected;
    /// trades with equal timestamps are accepted unless the id was already seen.
    pub fn push(&mut self, entry: Entry, window: Duration) -> Admit {
        if self.latest.is_some_and(|l| entry.ts < l) {
            return Admit::OutOfOrder;
        }
        let key = (
            entry.ts.with_timezone(&New_York).date_naive(),
            entry.trade_id,
        );
        if self.ids.contains(&key) {
            return Admit::Duplicate;
        }
        self.latest = Some(entry.ts);
        self.expire(entry.ts, window);
        self.ids.insert(key);
        self.entries.push_back((entry, key));
        let cap = self.cap.unwrap_or(MAX_TRADES_PER_SYMBOL);
        while self.entries.len() > cap {
            self.pop_front();
            self.cap_evictions += 1;
        }
        Admit::Accepted
    }

    /// Remove trades with `ts < now - window`.
    pub fn expire(&mut self, now: DateTime<Utc>, window: Duration) {
        let cutoff = now
            .checked_sub_signed(window)
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        while self.entries.front().is_some_and(|(e, _)| e.ts < cutoff) {
            self.pop_front();
        }
    }

    fn pop_front(&mut self) {
        if let Some((_, key)) = self.entries.pop_front() {
            self.ids.remove(&key);
        }
    }

    /// Earliest trade still in the window.
    pub fn baseline(&self) -> Option<Entry> {
        self.entries.front().map(|(e, _)| *e)
    }

    pub fn latest(&self) -> Option<DateTime<Utc>> {
        self.latest
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + s, 0).unwrap()
    }
    fn e(s: i64, p: f64, id: i64) -> Entry {
        Entry {
            ts: at(s),
            price: p,
            trade_id: id,
        }
    }

    #[test]
    fn expires_and_keeps_boundary() {
        let w = Duration::seconds(10);
        let mut win = SymbolWindow::default();
        assert_eq!(win.push(e(0, 1.0, 1), w), Admit::Accepted);
        assert_eq!(win.push(e(10, 2.0, 2), w), Admit::Accepted);
        assert_eq!(win.baseline().unwrap().trade_id, 1); // t == now - window stays
        assert_eq!(win.push(e(11, 3.0, 3), w), Admit::Accepted);
        assert_eq!(win.baseline().unwrap().trade_id, 2);
        assert_eq!(win.len(), 2);
    }

    #[test]
    fn cap_evictions_are_counted() {
        let w = Duration::seconds(1000);
        let mut win = SymbolWindow::with_cap(2);
        for i in 0..5 {
            win.push(e(i, 1.0, i), w);
        }
        assert_eq!(win.len(), 2);
        assert_eq!(win.baseline().unwrap().trade_id, 3);
        assert_eq!(win.take_cap_evictions(), 3);
        assert_eq!(win.take_cap_evictions(), 0);
    }

    #[test]
    fn rejects_duplicates_and_out_of_order() {
        let w = Duration::seconds(10);
        let mut win = SymbolWindow::default();
        win.push(e(5, 1.0, 1), w);
        assert_eq!(win.push(e(5, 1.0, 1), w), Admit::Duplicate);
        assert_eq!(win.push(e(4, 1.0, 2), w), Admit::OutOfOrder);
        assert_eq!(win.push(e(5, 1.0, 3), w), Admit::Accepted);
        assert_eq!(win.len(), 2);
    }
}
