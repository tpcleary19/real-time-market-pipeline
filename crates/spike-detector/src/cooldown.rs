//! Cooldown keyed by `(symbol, direction)`, measured in event time so replay is deterministic.
use crate::event::Direction;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Cooldown {
    duration: Duration,
    last: HashMap<(String, Direction), DateTime<Utc>>,
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
    pub fn try_fire(&mut self, symbol: &str, dir: Direction, now: DateTime<Utc>) -> bool {
        if let Some(&prev) = self.last.get(&(symbol.to_string(), dir))
            && prev
                .checked_add_signed(self.duration)
                .is_none_or(|end| now < end)
        {
            return false;
        }
        self.last.insert((symbol.to_string(), dir), now);
        true
    }

    /// Drop entries whose cooldown has ended relative to the global event-time watermark.
    pub fn prune(&mut self, watermark: DateTime<Utc>) {
        let d = self.duration;
        self.last
            .retain(|_, &mut t| t.checked_add_signed(d).is_none_or(|end| watermark < end));
    }

    /// Record that an event for `(symbol, dir)` was already published at event time `at`
    /// (keeps the later of the existing and given time).
    pub fn seed(&mut self, symbol: &str, dir: Direction, at: DateTime<Utc>) {
        let e = self.last.entry((symbol.to_string(), dir)).or_insert(at);
        if at > *e {
            *e = at;
        }
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

    #[test]
    fn suppress_then_expire_per_direction() {
        let t0 = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let mut c = Cooldown::new(Duration::seconds(600));
        assert!(c.try_fire("A", Direction::Up, t0));
        assert!(!c.try_fire("A", Direction::Up, t0 + Duration::seconds(599)));
        assert!(c.try_fire("A", Direction::Down, t0 + Duration::seconds(1)));
        assert!(c.try_fire("B", Direction::Up, t0 + Duration::seconds(1)));
        assert!(c.try_fire("A", Direction::Up, t0 + Duration::seconds(600)));
    }
}
