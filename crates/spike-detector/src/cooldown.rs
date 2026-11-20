//! Cooldown keyed by `(symbol, direction)`, measured in event time so replay is deterministic.
use crate::event::Direction;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
struct Entry {
    /// Event time of the published event.
    event_ts: DateTime<Utc>,
    /// Processing time (injected `detected_at`) when the entry was created or first seen by a
    /// prune; used to drop entries of symbols that have been evicted.
    proc_ts: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct Cooldown {
    duration: Duration,
    last: HashMap<(String, Direction), Entry>,
}

impl Cooldown {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            last: HashMap::new(),
        }
    }

    /// Returns true if an event may be published at event time `now`; if so, starts a new
    /// cooldown. Suppressed while `now < last_published + duration`.
    pub fn try_fire(
        &mut self,
        symbol: &str,
        dir: Direction,
        now: DateTime<Utc>,
        proc_now: DateTime<Utc>,
    ) -> bool {
        if let Some(prev) = self.last.get(&(symbol.to_string(), dir))
            && prev
                .event_ts
                .checked_add_signed(self.duration)
                .is_none_or(|end| now < end)
        {
            return false;
        }
        self.last.insert(
            (symbol.to_string(), dir),
            Entry {
                event_ts: now,
                proc_ts: Some(proc_now),
            },
        );
        true
    }

    /// Drop finished cooldowns. An entry of a symbol that still has a window ends when THAT
    /// symbol's own latest event time passes `event_ts + duration` (other symbols' clocks are
    /// irrelevant). An entry of a symbol without a window ends once `duration` of processing
    /// time has passed.
    pub fn prune(
        &mut self,
        proc_now: DateTime<Utc>,
        latest_event: &dyn Fn(&str) -> Option<DateTime<Utc>>,
    ) {
        let d = self.duration;
        self.last.retain(|(sym, _), e| match latest_event(sym) {
            Some(l) => e.event_ts.checked_add_signed(d).is_none_or(|end| l < end),
            None => {
                let p = *e.proc_ts.get_or_insert(proc_now);
                p.checked_add_signed(d).is_none_or(|end| proc_now < end)
            }
        });
    }

    /// Record that an event for `(symbol, dir)` was already published at event time `at`
    /// (keeps the later of the existing and given time).
    pub fn seed(&mut self, symbol: &str, dir: Direction, at: DateTime<Utc>) {
        let e = self.last.entry((symbol.to_string(), dir)).or_insert(Entry {
            event_ts: at,
            proc_ts: None,
        });
        if at > e.event_ts {
            e.event_ts = at;
        }
    }

    /// Overwrite the entry for `(symbol, dir)` with an event time known to be exact.
    pub fn set(&mut self, symbol: &str, dir: Direction, at: DateTime<Utc>) {
        self.last.insert(
            (symbol.to_string(), dir),
            Entry {
                event_ts: at,
                proc_ts: None,
            },
        );
    }

    /// Forget all cooldown state.
    pub fn clear(&mut self) {
        self.last.clear();
    }

    pub fn len(&self) -> usize {
        self.last.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn suppress_then_expire_per_direction() {
        let t0 = t0();
        let mut c = Cooldown::new(Duration::seconds(600));
        assert!(c.try_fire("A", Direction::Up, t0, t0));
        assert!(!c.try_fire("A", Direction::Up, t0 + Duration::seconds(599), t0));
        assert!(c.try_fire("A", Direction::Down, t0 + Duration::seconds(1), t0));
        assert!(c.try_fire("B", Direction::Up, t0 + Duration::seconds(1), t0));
        assert!(c.try_fire("A", Direction::Up, t0 + Duration::seconds(600), t0));
    }

    #[test]
    fn prune_uses_symbols_own_clock() {
        let t0 = t0();
        let mut c = Cooldown::new(Duration::seconds(600));
        c.try_fire("A", Direction::Up, t0, t0);
        // Another symbol's clock is far ahead; A's own latest time has not moved.
        c.prune(t0, &|_| Some(t0 + Duration::seconds(5)));
        assert_eq!(c.len(), 1);
        c.prune(t0, &|_| Some(t0 + Duration::seconds(600)));
        assert!(c.is_empty());
    }

    #[test]
    fn prune_without_window_uses_processing_time() {
        let t0 = t0();
        let mut c = Cooldown::new(Duration::seconds(600));
        c.seed("A", Direction::Up, t0);
        c.prune(t0, &|_| None); // first sighting stamps processing time
        assert_eq!(c.len(), 1);
        c.prune(t0 + Duration::seconds(599), &|_| None);
        assert_eq!(c.len(), 1);
        c.prune(t0 + Duration::seconds(600), &|_| None);
        assert!(c.is_empty());
    }
}
