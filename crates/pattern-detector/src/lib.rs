use chrono::{DateTime, Duration, NaiveDate, Utc};
use market_events::Trade;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

#[derive(Debug)]
pub struct Alert {
    pub symbol: String,
    pub direction: Direction,
    pub movement_percent: f64,
    pub baseline_price: f64,
    pub price: f64,
    pub window_start: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
}

fn key(t: &Trade) -> (NaiveDate, i64) {
    (
        t.timestamp
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive(),
        t.trade_id,
    )
}

#[derive(Default)]
struct Window {
    trades: VecDeque<Trade>,
    seen: HashSet<(NaiveDate, i64)>,
    active: Option<Direction>,
}

/// Per-symbol event-time windows. Older-than-latest trades are ignored.
/// Both endpoints of the five-minute interval are included.
#[derive(Default)]
pub struct Detector {
    symbols: HashMap<String, Window>,
}

impl Detector {
    pub fn observe(&mut self, t: Trade) -> Option<Alert> {
        if !t.price.is_finite() || t.price <= 0.0 || t.symbol.trim().is_empty() {
            return None;
        }
        let w = self.symbols.entry(t.symbol.clone()).or_default();
        if w.trades
            .back()
            .is_some_and(|last| t.timestamp < last.timestamp)
        {
            return None;
        }
        let cutoff = t.timestamp - Duration::seconds(300);
        while w.trades.front().is_some_and(|old| old.timestamp < cutoff) {
            let old = w.trades.pop_front().unwrap();
            w.seen.remove(&key(&old));
        }
        if !w.seen.insert(key(&t)) {
            return None;
        }
        w.trades.push_back(t);
        let first = w.trades.front().unwrap();
        let last = w.trades.back().unwrap();
        let movement = (last.price - first.price) / first.price;
        let direction = if movement >= 0.02 {
            Some(Direction::Up)
        } else if movement <= -0.02 {
            Some(Direction::Down)
        } else {
            None
        };
        let previous = w.active;
        w.active = direction;
        let direction = direction.filter(|d| Some(*d) != previous)?;
        Some(Alert {
            symbol: last.symbol.clone(),
            direction,
            movement_percent: movement * 100.0,
            baseline_price: first.price,
            price: last.price,
            window_start: first.timestamp,
            timestamp: last.timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    fn t(s: &str, id: i64, sec: i64, price: f64) -> Trade {
        Trade {
            symbol: s.into(),
            trade_id: id,
            price,
            size: 1,
            timestamp: Utc.timestamp_opt(1_700_000_000 + sec, 0).unwrap(),
            exchange: None,
            tape: None,
        }
    }
    #[test]
    fn thresholds_and_exact_window_boundary() {
        for (price, dir) in [(102.0, Direction::Up), (98.0, Direction::Down)] {
            let mut d = Detector::default();
            assert!(d.observe(t("A", 1, 0, 100.0)).is_none());
            assert!(d.observe(t("A", 2, 1, 101.99)).is_none());
            let a = d.observe(t("A", 3, 300, price)).unwrap();
            assert_eq!(a.direction, dir);
            assert!((a.movement_percent.abs() - 2.0).abs() < 1e-10);
        }
    }
    #[test]
    fn expiry_removes_trades_and_dedup_keys() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        d.observe(t("A", 2, 1, 101.0));
        assert!(d.observe(t("A", 3, 301, 102.0)).is_none());
        assert_eq!(d.symbols["A"].trades.len(), 2);
        assert_eq!(d.symbols["A"].seen.len(), 2);
    }
    #[test]
    fn symbols_are_independent() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        d.observe(t("B", 1, 0, 200.0));
        assert_eq!(d.observe(t("A", 2, 1, 102.0)).unwrap().symbol, "A");
        assert!(d.observe(t("B", 2, 1, 201.0)).is_none());
    }
    #[test]
    fn duplicate_trade_does_not_change_state() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        assert!(d.observe(t("A", 1, 1, 110.0)).is_none());
        assert!(d.observe(t("A", 2, 2, 102.0)).is_some());
        assert!(d.observe(t("A", 2, 2, 102.0)).is_none());
        assert_eq!(d.symbols["A"].trades.len(), 2);
    }
    #[test]
    fn alert_latch_rearms_and_changes_direction() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        assert!(d.observe(t("A", 2, 1, 102.0)).is_some());
        assert!(d.observe(t("A", 3, 2, 104.0)).is_none());
        assert!(d.observe(t("A", 4, 3, 101.0)).is_none());
        assert!(d.observe(t("A", 5, 4, 102.0)).is_some());
        assert_eq!(
            d.observe(t("A", 6, 5, 98.0)).unwrap().direction,
            Direction::Down
        );
    }
    #[test]
    fn idle_gap_establishes_new_baseline() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        d.observe(t("A", 2, 1, 102.0));
        assert!(d.observe(t("A", 3, 302, 200.0)).is_none());
        assert!(d.observe(t("A", 4, 303, 204.0)).is_some());
    }
    #[test]
    fn invalid_prices_and_symbols_are_ignored() {
        let mut d = Detector::default();
        for p in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(d.observe(t("A", 1, 0, p)).is_none());
        }
        d.observe(t(" ", 1, 0, 100.0));
        assert!(d.symbols.is_empty());
    }
    #[test]
    fn late_trades_are_ignored_equal_timestamps_are_accepted() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 10, 100.0));
        assert!(d.observe(t("A", 2, 9, 200.0)).is_none());
        assert!(d.observe(t("A", 3, 10, 102.0)).is_some());
        assert_eq!(d.symbols["A"].trades.len(), 2);
    }
    #[test]
    fn ids_can_be_reused_next_day() {
        let mut d = Detector::default();
        d.observe(t("A", 1, 0, 100.0));
        d.observe(t("A", 1, 86400, 200.0));
        assert_eq!(d.symbols["A"].trades.front().unwrap().price, 200.0);
        assert_eq!(d.symbols["A"].seen.len(), 1);
    }
    #[test]
    fn shared_wire_format() {
        let trade: Trade = serde_json::from_str(
            r#"{"i":1,"S":"AAPL","p":100.0,"s":5,"t":"2026-10-19T14:00:00Z","x":"V","z":"C"}"#,
        )
        .unwrap();
        assert!(Detector::default().observe(trade).is_none());
    }
}
