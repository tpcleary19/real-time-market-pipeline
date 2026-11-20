# Spike Detector

Standalone Rust service (`crates/spike-detector`). Consumes `stock-trades`, keeps a rolling
event-time window per symbol, and publishes UP/DOWN spike events to `price-spikes`. It never
waits on any downstream consumer of `price-spikes`.

## Run locally
```bash
docker compose up -d kafka kafka-init        # kafka-init creates the price-spikes topic
cargo run -p spike-detector                  # Ctrl-C / SIGTERM for graceful stop
```
Inspect output:
```bash
docker exec market-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic price-spikes --from-beginning --property print.key=true
```
Generate a synthetic spike (two trades 10% apart; ids and prices are synthetic):
```bash
T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf 'ZZZZ:{"i":1,"S":"ZZZZ","p":100.0,"s":1,"t":"%s","x":"V","z":"C"}\nZZZZ:{"i":2,"S":"ZZZZ","p":110.0,"s":1,"t":"%s","x":"V","z":"C"}\n' $T $T | \
docker exec -i market-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic stock-trades --property parse.key=true --property key.separator=:
```
Trade timestamps must be within `SPIKE_MAX_FUTURE_SKEW_SECONDS` of this host's wall clock.

## Configuration (environment variables)
| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:9092` |
| `SPIKE_SOURCE_TOPIC` | `stock-trades` |
| `SPIKE_OUTPUT_TOPIC` | `price-spikes` |
| `SPIKE_CONSUMER_GROUP` | `price-spike-detector` |
| `SPIKE_WINDOW_SECONDS` | `600` |
| `SPIKE_THRESHOLD_PERCENT` | `5.0` |
| `SPIKE_COOLDOWN_SECONDS` | `600` |
| `SPIKE_MAX_SYMBOLS` | `10000` |
| `SPIKE_METRICS_INTERVAL_SECONDS` | `30` |
| `SPIKE_AUTO_OFFSET_RESET` | `latest` (new group only; `earliest` allowed) |
| `SPIKE_MAX_FUTURE_SKEW_SECONDS` | `300` (valid range 0..=3600) |

## Detection semantics
- **Event time** (trade `t`) drives windows and cooldown; wall clock is used only for `detected_at`. Replay is deterministic.
- Baseline = earliest trade with `t >= current_t - window` (inclusive edge). `percentage_change = (current - baseline) / baseline * 100`; spike if `abs >= threshold` (inclusive, 1e-9 tolerance for float noise). No full window is required: with one trade in the window the change is 0.
- `percentage_change` is rounded to 4 dp. Prices are JSON numbers (f64).
- **Dropped and counted**: invalid symbol (must be 1 to 32 bytes of ASCII graphic characters, 0x21..=0x7E: no spaces, control, zero-width, right-to-left or other non-ASCII characters; lowercase and `.`, `-`, `/` are fine); price <= 0 or non-finite; trade time outside the years 2000 to 2099 (malformed in the I/O layer, invalid in the core; also keeps chrono arithmetic from panicking); malformed JSON, empty payload or missing `t`; trades whose `t` is more than `SPIKE_MAX_FUTURE_SKEW_SECONDS` ahead of the **host wall clock** (see *Clock and skew* below); duplicates `(symbol, trade_id, ET date)` still retained; **late** trades (older than the latest accepted trade for the symbol). Equal timestamps are processed.
- **Cooldown** per `(symbol, direction)` in event time: after an event, same-direction spikes are suppressed while `elapsed < cooldown`; `>=` allows a new event. Suppressed spikes do not extend the cooldown. UP and DOWN are independent. Event time makes replay deterministic given the same inputs and starting state; the cooldown *starting state* after a restart is restored from `price-spikes` (see below), because replaying a bounded lookback alone cannot reproduce it.
- **Bounded state** (worst case: `SPIKE_MAX_SYMBOLS` x 100,000 trades at roughly 100-130 B each is far above any realistic RAM, so the caps are not a hard global bound; size `SPIKE_MAX_SYMBOLS` for your universe. Per-symbol cap evictions are counted as `evicted_by_cap` in `METRICS`). **Symbols are isolated**: a symbol's window expires using only its own event times. A symbol is evicted when nothing has been *accepted* for it for more than the window of **processing time** (the injected `detected_at`, i.e. wall clock in production); late or duplicate trades do not refresh it. This frees capacity (a new symbol at the cap is dropped and counted only if cleanup frees nothing). Cleanup runs every 1024 trades and when at capacity. Cooldown entries are pruned against that symbol's own latest event time (or, once its window is gone, after one cooldown of processing time). One symbol's bogus timestamp therefore cannot evict or expire any other symbol, and a symbol poisoned by a future-dated trade recovers by itself once nothing is accepted for it for a window of wall time. An evicted symbol forgets its dedupe/late history.
- `event_id` = `{symbol}-{DIR}-{trade_id}-{current_ts_nanos}`, stable across replays.

## Delivery guarantees: at-least-once
1. A record's offset is stored only after every spike derived from it is acknowledged (`acks=all`, idempotent producer). Transient publish failures retry every 2 s indefinitely; the offset is not stored meanwhile. Non-retriable failures (message too large, topic/cluster authorization or authentication failure, invalid topic, fatal/fenced idempotent producer, or an unknown output topic that persists for 30 retries) make the process log `FATAL` and exit with status 1 without storing the offset; a restart replays the record. Stored offsets are auto-committed every 5 s and synchronously on graceful shutdown.
2. A crash or shutdown between publish and offset commit re-delivers the record, so a spike may be **published more than once**. Consumers must deduplicate on `event_id`. Kafka transactions were not used: at-least-once plus stable ids is the simplest defensible design.
3. No spike is lost from a record whose offset was stored.

## Restart / warm-up
Window and cooldown state lives in memory only. At startup, for each partition with committed offset `C`, the service reads the Kafka timestamp of record `C-1`, seeks back to `ts - (window + cooldown + 60s)` using `offsets_for_times`, replays up to `C` into the detector and **discards the resulting events** (those records were fully handled before their offset was stored, so their spikes were already delivered), then resumes normal processing at `C`. Partitions not being replayed are paused during warm-up and every partition is re-seeked to its resume offset afterwards, so nothing read during warm-up is lost. SIGTERM/Ctrl-C during startup exits cleanly. If the source topic has no partitions the service retries for 60 s, then exits with status 1. Records between the last committed offset and where the previous run stopped are reprocessed normally (duplicates possible, see above). A new consumer group starts per `SPIKE_AUTO_OFFSET_RESET` with empty state.

Limits: the start point relies on Kafka record timestamps roughly tracking trade time; if record `C-1` is unreadable (retention), `offsets_for_times` fails, or warm-up makes no progress for 15 s (including persistent consumer errors), the service logs a `WARNING` and continues with partial or empty state; it is a **single instance** that manually assigns all partitions (no group rebalancing); trades older than the lookback are not reproduced.

### Clock and skew
The future-skew check compares the trade time `t` with this host's wall clock (record timestamps are producer-supplied and can be wrong in either direction, so they are not trusted). Trade-off: a host clock more than `SPIKE_MAX_FUTURE_SKEW_SECONDS` behind the feed (for example a WSL2 VM clock that drifted after sleep) makes every trade look future-dated and the feed is dropped; offsets are still stored, so those trades are not replayed. This is made loud rather than silent: each rejection increments `future_rejected` and a `WARNING` (at most one per 30 s, with the count and the largest drift seen) is logged. Fix the clock (`sudo hwclock -s` or restarting WSL) or raise the limit (default 300 s; a larger limit lets one future-dated trade pin a symbol as "late" for longer, until it recovers by processing time as described above).

### Cooldown seeding
Replaying `window + cooldown + 60 s` rebuilds windows but not the cooldown phase: under a sustained trend the moment the last event fired depends on unbounded history. After warm-up the service reads the tail of the output topic (records at or after the last committed record's Kafka time minus the lookback, at most the newest 100,000 records per partition, up to the high watermark, within 10 s). It never publishes while doing so. Rules:
- Events with another `schema_version` and unparsable payloads are skipped. Per `(symbol, direction)` the latest `current_timestamp` is used.
- A seed later than the replay's newest event time (or wall now if nothing was replayed) plus `SPIKE_MAX_FUTURE_SKEW_SECONDS` is **ignored** and logged; a bogus or foreign event in the topic cannot suppress real spikes for the life of the process.
- If everything in range was read, the seeds are authoritative and replace the replay-derived cooldowns. If only the newest records were read (tail cap) but the read reached the high watermark, each pair that appears among the seeds is overridden with its newest published event (exact) and pairs not seen keep their replay-derived value. If the 10 s budget ran out before the end, or the topic is unreadable, nothing is reset: seeds are merged with max(`current_timestamp`) and replay-derived entries are kept. A clear `WARNING` says which case applied. The tail cap means a shortfall skips old events, not new ones.
- This also covers a crash after publishing but before the offset commit: the re-processed record is suppressed by the seeded cooldown instead of a later trade firing a second event with a different `event_id`.

### Startup offsets and shutdown
- A partition with no committed offset resolves to the low/high watermark per `SPIKE_AUTO_OFFSET_RESET` and that resolved start is **committed at startup**, so with `latest` records produced while the service is down are not skipped on later restarts. The default stays `latest`.
- Shutdown (SIGTERM/SIGINT): the final offset commit waits at most 10 s and the producer flush 5 s, then the process exits. A second signal forces an immediate exit with status 130. Both are data-safe: uncommitted offsets just replay.

## Observability
A `METRICS` line goes to stderr every `SPIKE_METRICS_INTERVAL_SECONDS`: consumed, malformed, future-rejected, invalid, accepted, duplicates, out-of-order, spikes detected, suppressed by cooldown, published, publish errors, capacity drops, evicted symbols, active symbols, retained trades, total consumer lag (high watermark minus next offset), average and max processing latency in microseconds. Each spike is also logged as `SPIKE ...`.

## Event contract
See `crates/spike-detector/src/event.rs` (`schema_version = 1`). Kafka key = symbol. Fields: `schema_version`, `event_id`, `symbol`, `direction` (`UP`/`DOWN`), `baseline_price`, `current_price`, `percentage_change`, `window_seconds`, `baseline_timestamp`, `current_timestamp` (event times, RFC 3339 UTC), `detected_at` (wall clock at detection). Optional fields may be added within a version; breaking changes bump `schema_version`.

## Persistence ownership
`spike-detector` does not write to PostgreSQL (or Neon) and has no database dependency, no `DATABASE_URL`, and no outbox. Its only outputs are the `price-spikes` Kafka events and logs. Persisting spikes belongs to the future AI investigation harness, which will consume `price-spikes` and use the existing `DATABASE_URL` (Neon when configured); consumers should dedupe on `event_id`. Any database integration tests for that harness must use a separate `TEST_DATABASE_URL`. `pattern-detector` and the alert API are untouched by this change; the owner will retire `pattern-detector` separately once the replacement persistence path and alert API compatibility are validated.

## Known limitations
- **Partitions added while running.** Partitions are assigned once at startup; a partition added later is not read until a restart, and on restart (with `latest`) the records it already holds are skipped because its start resolves to the current high watermark. Use `earliest` or restart before producing to a new partition if that matters. (Covered by the failing broker probe `skeptic_added_partition_records_are_not_lost`.)
- **Startup failures are fatal.** Calls made at startup (committed offsets, watermarks, assignment and similar) are not retried: on failure the process logs `FATAL` and exits with status 1. Run it under a supervisor that restarts it with backoff; this PR adds no compose service for the detector.
- **Shutdown during startup can be slow.** The blocking startup calls do not check for shutdown, so SIGTERM during startup may take several 10 s call timeouts; a second signal exits immediately. This is data-safe, since no offset is stored during startup.
- **Unknown-topic exit is bounded by attempts, not time.** An output topic that stays unknown ends the process after 30 consecutive failed attempts. Each attempt can take up to the send queue timeout plus the 2 s sleep, so in the worst case the exit takes on the order of 15 minutes.
- **Expired committed offsets.** If retention removed the data at the committed offset while the service was down, Kafka falls back to `auto.offset.reset`. With `latest`, the remaining backlog is skipped.
- Single instance. Two instances would race on commits (duplicates, not loss).
- Warm-up and cooldown-seed start points rely on Kafka record timestamps tracking trade time (the start offsets only; the skew check does not); backfilled data can make warm-up under-rebuild state, and nothing checks this.
- **Global state is only the capacity cap.** Everything else is per symbol (see Bounded state). The lagging-partition scenario is covered by `skeptic_lagging_partition_keeps_cooldown_when_another_partition_runs_ahead`, which now passes. Processing time is wall clock in production; when a replay runs much faster than real time, symbols idle in event time are only evicted after a window of wall time.
- **Duplicate key assumption.** Duplicates are `(symbol, trade_id, ET date)`; this assumes trade ids are unique per symbol and day, which is unverified against real Alpaca data (distinct trades with equal ids would be dropped).
- Pure-detector tests that restart from a bare replay without seeding cooldowns are expected to fail; the seeded variants and the real-service tests pass.
- At symbol capacity every unknown-symbol trade triggers an O(symbols) cleanup scan; a flood of distinct garbage symbols can fill capacity. There is no symbol allowlist.
- `event_id` uses nanoseconds, which saturate for timestamps outside 1677-2262 (event.rs, lead-owned).
- Lag is computed with sequential blocking watermark calls on the metrics tick and pauses consumption briefly.
- The signal task failing to install would be treated as an immediate shutdown request.

## Verification status
Against a single local Kafka 4.1.0 broker, with the `--ignored` tests: `kafka_integration` 7 of 7 pass; `skeptic_kafka` 14 of 15 pass (`skeptic_added_partition_records_are_not_lost` fails, a documented limitation). Pure tests: `skeptic_lagging_partition_keeps_cooldown_when_another_partition_runs_ahead` (ignored by default) passes. Not verified: a multi-broker cluster, real `market-producer` traffic, load/performance, and the `kafka-init` compose wiring (only its command was run).
