# Spike Detector Testing

Run: `cargo test -p spike-detector` (no Kafka needed; all tests use synthetic event-time fixtures, no wall clock).

## Files (`crates/spike-detector/tests/`)
- `common/mod.rs`: fixtures (fixed t0 = 2024-01-02T15:00:00Z, injected `detected_at`).
- `detector_tests.rs`: assignment section 8 items 1-13, 15 against `Detector` (thresholds incl. inclusive boundary, window edge `t >= now - window`, baseline advance, expiry/state bounds, symbol isolation, `max_symbols`, cooldown per direction and in event time, duplicates, out-of-order, invalid input, replay/warm-up determinism, redelivery).
- `event_tests.rs`: item 14 (JSON round trip, field names, uppercase direction, pinned `event_id` format, malformed payload rejection, 4 dp rounding).

## Assumptions encoded (from plan.md decisions)
Boundary inclusive; window edge inclusive; late = older than latest for the symbol (equal timestamps allowed); invalid symbol = empty/whitespace/lowercase/over-long/control chars (impl must confirm its symbol rule); cooldown expiry tested only well past the boundary.

## Not covered
Kafka integration (consumption, publish, offset-store ordering, producer failure) is NOT written or executed: it needs the impl's `kafka.rs` API and a broker. Warm-up is tested only as replay determinism of the pure core.
