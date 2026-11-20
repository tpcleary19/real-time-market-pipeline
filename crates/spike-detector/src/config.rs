//! Environment-variable configuration.
use crate::detector::Config as DetectorConfig;

#[derive(Debug, Clone)]
pub struct Settings {
    pub brokers: String,
    pub source_topic: String,
    pub output_topic: String,
    pub group_id: String,
    pub window_seconds: u64,
    pub threshold_percent: f64,
    pub cooldown_seconds: u64,
    pub max_symbols: usize,
    pub metrics_interval_seconds: u64,
    /// Where a brand-new consumer group starts: `latest` or `earliest`.
    pub auto_offset_reset: String,
    /// Trades timestamped further than this into the future are rejected as invalid.
    pub max_future_skew_seconds: i64,
}

fn parse<T: std::str::FromStr>(
    get: &dyn Fn(&str) -> Option<String>,
    key: &str,
    default: T,
) -> Result<T, String> {
    match get(key) {
        None => Ok(default),
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| format!("invalid value for {key}: {v:?}")),
    }
}

impl Settings {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> Result<Self, String> {
        let s = Settings {
            brokers: get("KAFKA_BROKERS").unwrap_or_else(|| "localhost:9092".into()),
            source_topic: get("SPIKE_SOURCE_TOPIC").unwrap_or_else(|| "stock-trades".into()),
            output_topic: get("SPIKE_OUTPUT_TOPIC").unwrap_or_else(|| "price-spikes".into()),
            group_id: get("SPIKE_CONSUMER_GROUP").unwrap_or_else(|| "price-spike-detector".into()),
            window_seconds: parse(get, "SPIKE_WINDOW_SECONDS", 600)?,
            threshold_percent: parse(get, "SPIKE_THRESHOLD_PERCENT", 5.0)?,
            cooldown_seconds: parse(get, "SPIKE_COOLDOWN_SECONDS", 600)?,
            max_symbols: parse(get, "SPIKE_MAX_SYMBOLS", 10_000)?,
            metrics_interval_seconds: parse(get, "SPIKE_METRICS_INTERVAL_SECONDS", 30)?,
            auto_offset_reset: get("SPIKE_AUTO_OFFSET_RESET").unwrap_or_else(|| "latest".into()),
            max_future_skew_seconds: parse(get, "SPIKE_MAX_FUTURE_SKEW_SECONDS", 300)?,
        };
        s.validate()?;
        Ok(s)
    }

    fn validate(&self) -> Result<(), String> {
        if !(self.threshold_percent.is_finite() && self.threshold_percent > 0.0) {
            return Err("SPIKE_THRESHOLD_PERCENT must be a positive number".into());
        }
        if self.window_seconds == 0 || self.window_seconds > 86_400 {
            return Err("SPIKE_WINDOW_SECONDS must be in 1..=86400".into());
        }
        if self.cooldown_seconds > 86_400 {
            return Err("SPIKE_COOLDOWN_SECONDS must be <= 86400".into());
        }
        if self.max_symbols == 0 || self.metrics_interval_seconds == 0 {
            return Err("SPIKE_MAX_SYMBOLS and SPIKE_METRICS_INTERVAL_SECONDS must be > 0".into());
        }
        if !(0..=3600).contains(&self.max_future_skew_seconds) {
            return Err("SPIKE_MAX_FUTURE_SKEW_SECONDS must be in 0..=3600".into());
        }
        if !matches!(self.auto_offset_reset.as_str(), "latest" | "earliest") {
            return Err("SPIKE_AUTO_OFFSET_RESET must be latest or earliest".into());
        }
        Ok(())
    }

    pub fn detector_config(&self) -> DetectorConfig {
        DetectorConfig {
            window_seconds: self.window_seconds,
            threshold_percent: self.threshold_percent,
            cooldown_seconds: self.cooldown_seconds,
            max_symbols: self.max_symbols,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_overrides() {
        let s = Settings::from_lookup(&|_| None).unwrap();
        assert_eq!(
            (s.window_seconds, s.threshold_percent, s.cooldown_seconds),
            (600, 5.0, 600)
        );
        assert_eq!(s.output_topic, "price-spikes");
        let s = Settings::from_lookup(&|k| (k == "SPIKE_THRESHOLD_PERCENT").then(|| "2.5".into()))
            .unwrap();
        assert_eq!(s.threshold_percent, 2.5);
    }

    #[test]
    fn skew_default_and_range() {
        let s = Settings::from_lookup(&|_| None).unwrap();
        assert_eq!(s.max_future_skew_seconds, 300);
        for bad in ["-1", "3601", "9223372036854775807", "abc"] {
            let r = Settings::from_lookup(&|k| {
                (k == "SPIKE_MAX_FUTURE_SKEW_SECONDS").then(|| bad.to_string())
            });
            assert!(r.is_err(), "{bad} should be rejected");
        }
        assert!(
            Settings::from_lookup(&|k| (k == "SPIKE_MAX_FUTURE_SKEW_SECONDS").then(|| "0".into()))
                .is_ok()
        );
    }

    #[test]
    fn rejects_bad_values() {
        assert!(
            Settings::from_lookup(&|k| (k == "SPIKE_THRESHOLD_PERCENT").then(|| "nan".into()))
                .is_err()
        );
        assert!(
            Settings::from_lookup(&|k| (k == "SPIKE_WINDOW_SECONDS").then(|| "0".into())).is_err()
        );
        assert!(
            Settings::from_lookup(&|k| (k == "SPIKE_WINDOW_SECONDS").then(|| "x".into())).is_err()
        );
    }
}
