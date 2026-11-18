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
Trade timestamps must be within `SPIKE_MAX_FUTURE_SKEW_SECONDS` of now.

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
| `SPIKE_MAX_FUTURE_SKEW_SECONDS` | `30` (valid range 0..=3600) |

## Detection semantics
- **Event time** (trade `t`) drives windows and cooldown; wall clock is used only for `detected_at`. Replay is deterministic.
- Baseline = earliest trade with `t >= current_t - window` (inclusive edge). `percentage_change = (current - baseline) / baseline * 100`; spike if `abs >= threshold` (inclusive, 1e-9 tolerance for float noise). No full window is required: with one trade in the window the change is 0.
- `percentage_change` is rounded to 4 dp. Prices are JSON numbers (f64).
- **Dropped and counted**: invalid symbol (empty, whitespace-only, or containing control characters); price <= 0 or non-finite; malformed JSON, empty payload or missing `t`; trades timestamped more than `SPIKE_MAX_FUTURE_SKEW_SECONDS` ahead of now (a bogus future timestamp would otherwise block that symbol); duplicates `(symbol, trade_id, ET date)` still retained; **late** trades (older than the latest accepted trade for the symbol). Equal timestamps are processed.
- **Cooldown** per `(symbol, direction)` in event time: after an event, same-direction spikes are suppressed while `elapsed < cooldown`; `>=` allows a new event. Suppressed spikes do not extend the cooldown. UP and DOWN are independent. Event time was chosen so replay and warm-up behave identically to live processing.
- **Bounded state** (worst case: `SPIKE_MAX_SYMBOLS` x 100,000 trades at roughly 100-130 B each is far above any realistic RAM, so the caps are not a hard global bound; size `SPIKE_MAX_SYMBOLS` for your universe. Per-symbol cap evictions are counted as `evicted_by_cap` in `METRICS`): at most `SPIKE_MAX_SYMBOLS` symbols (a new symbol at the cap is dropped and counted, after attempting cleanup); symbols idle for more than the window (relative to the highest event time seen) are cleaned up, which frees capacity; each symbol retains at most 100,000 trades (oldest discarded early, moving the baseline forward). Cleanup runs every 1024 trades and when at capacity. A cleaned-up symbol forgets its dedupe/late history.
- `event_id` = `{symbol}-{DIR}-{trade_id}-{current_ts_nanos}`, stable across replays.

## Delivery guarantees: at-least-once
1. A record's offset is stored only after every spike derived from it is acknowledged (`acks=all`, idempotent producer). Transient publish failures retry every 2 s indefinitely; the offset is not stored meanwhile. Non-retriable failures (message too large, topic/cluster authorization or authentication failure, invalid topic, fatal/fenced idempotent producer, or an unknown output topic that persists for 30 retries) make the process log `FATAL` and exit with status 1 without storing the offset; a restart replays the record. Stored offsets are auto-committed every 5 s and synchronously on graceful shutdown.
2. A crash or shutdown between publish and offset commit re-delivers the record, so a spike may be **published more than once**. Consumers must deduplicate on `event_id`. Kafka transactions were not used: at-least-once plus stable ids is the simplest defensible design.
3. No spike is lost from a record whose offset was stored.

## Restart / warm-up
Window and cooldown state lives in memory only. At startup, for each partition with committed offset `C`, the service reads the Kafka timestamp of record `C-1`, seeks back to `ts - (window + cooldown + 60s)` using `offsets_for_times`, replays up to `C` into the detector and **discards the resulting events** (those records were fully handled before their offset was stored, so their spikes were already delivered), then resumes normal processing at `C`. Partitions not being replayed are paused during warm-up and every partition is re-seeked to its resume offset afterwards, so nothing read during warm-up is lost. SIGTERM/Ctrl-C during startup exits cleanly. If the source topic has no partitions the service retries for 60 s, then exits with status 1. Records between the last committed offset and where the previous run stopped are reprocessed normally (duplicates possible, see above). A new consumer group starts per `SPIKE_AUTO_OFFSET_RESET` with empty state.

Limits: the start point relies on Kafka record timestamps roughly tracking trade time; if record `C-1` is unreadable (retention), `offsets_for_times` fails, or warm-up makes no progress for 15 s (including persistent consumer errors), the service logs a `WARNING` and continues with partial or empty state; it is a **single instance** that manually assigns all partitions (no group rebalancing); trades older than the lookback are not reproduced.

## Observability
A `METRICS` line goes to stderr every `SPIKE_METRICS_INTERVAL_SECONDS`: consumed, malformed, future-rejected, invalid, accepted, duplicates, out-of-order, spikes detected, suppressed by cooldown, published, publish errors, capacity drops, evicted symbols, active symbols, retained trades, total consumer lag (high watermark minus next offset), average and max processing latency in microseconds. Each spike is also logged as `SPIKE ...`.

## Event contract
See `crates/spike-detector/src/event.rs` (`schema_version = 1`). Kafka key = symbol. Fields: `schema_version`, `event_id`, `symbol`, `direction` (`UP`/`DOWN`), `baseline_price`, `current_price`, `percentage_change`, `window_seconds`, `baseline_timestamp`, `current_timestamp` (event times, RFC 3339 UTC), `detected_at` (wall clock at detection). Optional fields may be added within a version; breaking changes bump `schema_version`.

## Known limitations
- Single instance; partitions added after startup are not picked up (restart to pick them up). Two instances would race on commits (duplicates, not loss).
- Warm-up start relies on Kafka record timestamps tracking trade time; backfilled data can make warm-up under-rebuild state, and nothing checks this.
- A future-timestamp rejection depends on wall clock, so replay with a later clock could accept a record the original run rejected.
- At symbol capacity every unknown-symbol trade triggers an O(symbols) cleanup scan; a flood of distinct garbage symbols can fill capacity. There is no symbol allowlist.
- `event_id` uses nanoseconds, which saturate for timestamps outside 1677-2262 (event.rs, lead-owned).
- Lag is computed with sequential blocking watermark calls on the metrics tick and pauses consumption briefly.
- The signal task failing to install would be treated as an immediate shutdown request.

## Not verified
The Kafka paths (`kafka.rs`, `main.rs`: publish, offset storage, warm-up) have not been run against a live broker; they compile and pass clippy. Unit tests cover the pure core.
