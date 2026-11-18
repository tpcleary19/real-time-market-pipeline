//! Lightweight counters logged periodically (project convention: plain stderr logging).
use crate::detector::Stats;
use chrono::Local;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct Metrics {
    pub consumed: u64,
    pub malformed: u64,
    pub rejected_future: u64,
    pub published: u64,
    pub publish_errors: u64,
    latency_count: u64,
    latency_sum: Duration,
    latency_max: Duration,
    /// partition -> next offset to be processed (last handled + 1)
    pub next_offsets: BTreeMap<i32, i64>,
}

impl Metrics {
    pub fn record_latency(&mut self, d: Duration) {
        self.latency_count += 1;
        self.latency_sum += d;
        self.latency_max = self.latency_max.max(d);
    }

    /// Emit one log line; `lag` is total consumer lag when known.
    pub fn report(&mut self, stats: &Stats, symbols: usize, retained: usize, lag: Option<i64>) {
        let avg_us = if self.latency_count == 0 {
            0
        } else {
            self.latency_sum.as_micros() / self.latency_count as u128
        };
        eprintln!(
            "{} METRICS consumed={} malformed={} future_rejected={} invalid={} accepted={} duplicates={} out_of_order={} \
             spikes_detected={} suppressed_cooldown={} published={} publish_errors={} dropped_capacity={} \
             evicted_symbols={} active_symbols={} retained_trades={} lag={} latency_avg_us={} latency_max_us={}",
            Local::now(),
            self.consumed,
            self.malformed,
            self.rejected_future,
            stats.invalid,
            stats.trades_accepted,
            stats.duplicates,
            stats.out_of_order,
            stats.spikes_detected,
            stats.suppressed_by_cooldown,
            self.published,
            self.publish_errors,
            stats.dropped_symbol_capacity,
            stats.evicted_symbols,
            symbols,
            retained,
            lag.map_or("unknown".to_string(), |l| l.to_string()),
            avg_us,
            self.latency_max.as_micros(),
        );
        self.latency_count = 0;
        self.latency_sum = Duration::ZERO;
        self.latency_max = Duration::ZERO;
    }
}
