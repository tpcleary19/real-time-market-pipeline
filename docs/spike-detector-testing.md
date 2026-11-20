# Spike Detector Testing

Run: `cargo test -p spike-detector` (no Kafka needed; all tests use synthetic event-time fixtures, no wall clock).

## Files (`crates/spike-detector/tests/`)
- `common/mod.rs`: fixtures (fixed t0 = 2024-01-02T15:00:00Z, injected `detected_at`).
- `detector_tests.rs`: assignment section 8 items 1-13, 15 against `Detector` (thresholds incl. inclusive boundary, window edge `t >= now - window`, baseline advance, expiry/state bounds, symbol isolation, `max_symbols`, cooldown per direction and in event time, duplicates, out-of-order, invalid input, replay/warm-up determinism, redelivery).
- `event_tests.rs`: item 14 (JSON round trip, field names, uppercase direction, pinned `event_id` format, malformed payload rejection, 4 dp rounding).

## Assumptions encoded (from plan.md decisions)
Boundary inclusive; window edge inclusive; late = older than latest for the symbol (equal timestamps allowed); invalid symbol = empty, whitespace-only, or control chars only (lead ruling); cooldown: elapsed >= cooldown permits, < suppresses (exact-boundary test); max_symbols: new symbol at cap rejected, idle symbols cleaned up.

## Not covered
Kafka integration (consumption, publish, offset-store ordering, producer failure) is NOT written or executed: it needs the impl's `kafka.rs` API and a broker. Warm-up is tested only as replay determinism of the pure core.

## Kafka integration tests (`kafka_parse_tests.rs`, `kafka_integration.rs`)
`kafka_parse_tests.rs` (parse_trade) needs no broker. `kafka_integration.rs` runs the real `spike-detector` binary against a live broker; every test is `#[ignore]`d. Each test creates unique topics/group and deletes the topics afterwards.

Broker (separate from the compose one, to avoid the fixed `market-kafka` container name):
    docker run -d --name spikeit-kafka -p 19092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.1.0
Run (about 2 min; `SPIKE_IT_LOG=1` shows the service's stderr):
    SPIKE_IT_BROKERS=localhost:19092 cargo test -p spike-detector --test kafka_integration -- --ignored --test-threads=1
Cleanup: `docker rm -f spikeit-kafka`.

Note: at startup the service commits the resolved start offset for partitions that never had a committed offset, so the two-partition test expects p1's committed offset to be its start (0) after run 1, then its end offset (3002) after the restart. Keep `--test-threads=1`: parallel runs against a cold broker are flaky.
