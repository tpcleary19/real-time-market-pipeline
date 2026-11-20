# Spike Detector: Skeptic Report (Agent 5)

Commit under test: `a392a20` (`feature/price-spike-detector`). One pass on 2026-11-20, about 35 minutes.
I reproduced every finding myself against the real `spike-detector` release binary on my own
broker (`skeptic-kafka`, Kafka 4.1.0, single node, `localhost:29092`, auto-create disabled).
I read no other agent's report and did not use their test results.

Probes:
- `crates/spike-detector/tests/skeptic_detector.rs`: pure core, no broker.
  Run with `cargo test -p spike-detector --test skeptic_detector`.
- `crates/spike-detector/tests/skeptic_kafka.rs`: runs the real binary; all tests are `#[ignore]`d.

Each test asserts the **expected** behaviour, so a failing test is a finding.

```bash
docker run -d --name skeptic-kafka -p 29092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false apache/kafka:4.1.0
SKEPTIC_BROKERS=localhost:29092 cargo test -p spike-detector --test skeptic_kafka -- --ignored --test-threads=1
docker rm -f skeptic-kafka
```

## Findings, ranked by severity

### 1. CRITICAL: one record with an extreme timestamp panics the service, causing a crash loop
- **Repro:** produce `{"i":1,"S":"G","p":5,"s":1,"t":"-262143-01-01T00:00:00Z"}` to the source topic, then start the service.
  - Test: `skeptic_detector::skeptic_extreme_past_timestamp_does_not_panic`
  - Test: `skeptic_kafka::skeptic_garbage_payloads_do_not_stop_the_service`
- **Observed:** the process panics with `Local time out of range for NaiveDateTime` (chrono `datetime/mod.rs:579`).
  - Backtrace: `SymbolWindow::push` → `Detector::observe` → `main`. The process exits with status 101.
  - The offset is never stored, so every restart hits the same record. I reproduced 2 of 2 restarts, both exit 101.
  - The whole topic stays blocked.
- **Expected:** the record is dropped and counted as invalid. Section 6 says "Avoid panics on malformed external data".
- **Where:** `window.rs:57` (`entry.ts.with_timezone(&New_York).date_naive()`). The future-skew guard only covers future times. Nothing bounds past times.
- **Fix (not applied):** reject timestamps outside a sane range in `parse_trade`. Use non-panicking chrono arithmetic (`checked_*`) in `window.rs`, `cooldown.rs` and `detector.rs`.

### 2. HIGH: an oversized symbol causes a permanent crash loop (poison pill)
- **Repro:** produce two trades 10% apart for a symbol of 350,000 `X` characters, then a valid spike for `OK`.
  - Test: `skeptic_kafka::skeptic_oversized_symbol_is_not_a_poison_pill`
- **Observed:** the service logs `FATAL non-retriable publish error: ... MessageSizeTooLarge` and exits with status 1.
  - This happens on all 3 restarts. The committed offset stays at 1.
  - The `OK` spike is never published.
  - The FATAL line also logs the 350 KB `event_id`.
- **Expected:** a record that can never be published must not stop the pipeline.
- **Where:**
  - `detector.rs:55` (`valid_symbol` has no length or charset bound)
  - `kafka.rs:81-85` (non-retriable error, then exit)
  - `main.rs:92-97`
- **Fix (not applied):** bound the symbol length and charset, for example `^[A-Z0-9.\-]{1,16}$`. For truly unpublishable events, log them loudly, count them and skip them, or send them to a dead-letter topic. Exit-and-replay is right for transient or config errors, not for data-dependent ones.

### 3. HIGH: when the host clock is behind the feed, every trade is rejected and the offsets are still committed
- **Repro:** produce trades whose `t` and record timestamp are 90 s ahead of the host clock. This simulates a host clock 90 s behind, such as WSL2 drift after sleep.
  - Test: `skeptic_kafka::skeptic_clock_behind_feed_does_not_lose_spikes`
- **Observed:** 0 events. Committed offset = 2. Both records are counted as `future_rejected`. The spike is lost for good.
  - During a real drift, the whole feed would be silently dropped.
- **Where:**
  - `main.rs:80`: live processing uses `Utc::now()`.
  - `kafka.rs` warm-up (`reference = msg.timestamp()`, about line 338): warm-up uses the record timestamp.
  - Live and warm-up therefore disagree about the same record, so state rebuilt after a restart can differ from the original run.
- **Fix (not applied):** use the Kafka record timestamp as the reference in live processing too. This is deterministic and needs no host clock. Alternatively, compare against the per-partition max record time. At minimum, alert on a high `future_rejected` rate.

### 4. MEDIUM: after a restart, warm-up rebuilds a different cooldown phase, so spikes are lost or duplicated under a different `event_id`
- **Repro (real service):** window 60 s, cooldown 60 s, a steady +1% move every 10 s. Process 51 records, stop gracefully (commit 51), produce 49 more, then restart.
  - Test: `skeptic_kafka::skeptic_restart_mid_trend_emits_same_event_ids_as_uninterrupted`
  - The pure test `skeptic_detector::skeptic_warm_up_cooldown_phase_matches_uninterrupted_run` shows the same.
- **Observed:** after the restart the run emits `UP-56, 62, 68, ... 98`. An uninterrupted run emits `UP-54, 60, 66, ... 96`. Every later event differs.
- **Crash variant:** `skeptic_detector::skeptic_crash_after_publish_does_not_yield_second_event_inside_cooldown`.
  - UP-54 is published, then the process crashes before the commit.
  - The restart suppresses 54 and emits UP-58.
  - The consumer receives two UP events 40 s apart while the cooldown is 60 s.
  - The ids differ, so `event_id` dedupe cannot help.
- **Why:** warm-up replays only `window + cooldown + 60 s` and starts with an empty cooldown table. Under a sustained spike condition, the cooldown phase depends on unbounded history.
  - This contradicts the docs: "replay and warm-up behave identically to live processing" and "No spike is lost / consumers dedupe on event_id".
- **Where:** `kafka.rs:244` (lookback) and `kafka.rs:346`.
- **Fix (not applied):** on startup, seed the cooldown table from the tail of `price-spikes`, which records what was actually published. Read the last `cooldown` seconds and take the last event per `(symbol, direction)`. Also document the limitation.

### 5. MEDIUM: with the default `latest`, records are skipped after a restart (also for partitions added later)
- **Repro (a):** start with `latest` and no traffic, stop, produce a spike, then restart.
  - Test: `skeptic_latest_default_restart_keeps_records_produced_while_down`
  - Observed: 0 events. The spike is lost. This matches the documented limitation.
- **Repro (b):** while the service runs, increase the source partitions from 1 to 2. Produce a spike to partition 1, then restart.
  - Test: `skeptic_added_partition_records_are_not_lost`
  - Observed: 0 events before the restart, because the partition is not picked up. 0 events after the restart, because `latest` starts at the high watermark.
  - With the default config, every trade sent to a new partition before the next restart is lost permanently. This combination is not documented.
- **Where:** `kafka.rs:251-255`. The resolved start offset is used but never committed.
- **Fix (not applied):** commit the resolved start offset at startup, which is one `commit()` call. That makes `latest` mean "from first deployment" rather than "from every restart". Consider defaulting to `earliest`, since the at-least-once claim is the selling point.

### 6. MEDIUM: shutdown blocks for as long as the broker is unreachable, and a second SIGTERM is ignored
- **Repro:** run the service, `docker pause skeptic-kafka`, then `kill -TERM`. Manual steps; details below.
- **Observed, 3 runs:**
  - Run 1: a spike had been published about 1.5 s before the pause, then SIGTERM. "shutting down" was logged, then no final METRICS line. The process was still alive about 4 minutes **after** `docker unpause`, so I killed it with `kill -9`. The committed offset was 2, and the event was in the output topic. This was not reproduced.
  - Runs 2 and 3: the process stayed alive for the whole pause (30 s and 45 s) and exited only once the broker returned.
  - The signal task returns after the first signal, so the tokio handler swallows any further SIGTERM or SIGINT.
- **Where:** `main.rs:121` (`commit_consumer_state(CommitMode::Sync)` has no timeout), consumer/producer drop, and `main.rs:160`.
- **Impact:** data-safe, because nothing is committed, but `docker stop` will always end in SIGKILL during a broker outage.
- **Fix (not applied):** make the final commit bounded (async commit plus a bounded wait), or do the commit on a thread with a deadline. Exit on a second signal.

### 7. LOW to MEDIUM: one global event-time watermark evicts state of lagging partitions
- **Repro:** `skeptic_detector::skeptic_lagging_partition_keeps_cooldown_when_another_partition_runs_ahead`. Symbol LAG fires UP at t=1. Then 1,100 trades of another symbol reach t=2000. LAG continues at t=2..3.
- **Observed:** a second UP for LAG (`LAG-UP-4-...`), 2 s after the first, with a 600 s cooldown.
- **Where:** `detector.rs:157-164` (`cutoff = wm - window` and `cooldown.prune(wm)` use the max event time across all partitions).
- **Real-service trigger (unverified):** this needs one partition to be processed more than `window` ahead in event time, which can happen in these cases:
  - warm-up fetch batching across partitions
  - catching up a lagging partition
  - a multi-broker outage of one partition's leader

  My 3-partition burst test did not trigger it.
- **Fix (not applied):** track the watermark per partition, or never prune cooldown entries by the global watermark. Prune per symbol by that symbol's latest time.

### 8. LOW (design, unverified): dedupe key ignores the exchange
- `window.rs:57` keys duplicates by `(ET date, trade_id)` per symbol.
- If upstream trade ids are only unique per exchange or tape, real distinct trades are dropped as duplicates.
- I had no real feed to check this. Verify against Alpaca SIP data before relying on it.

## Design challenges (no code changed)
- **Event-time cooldown:** the right choice for determinism, but see #4. It is only deterministic if the cooldown state survives restarts. Seeding from `price-spikes` would make the design actually hold.
- **f64 prices:** acceptable. The inputs are JSON floats, the 1e-9 tolerance works, and awkward decimal 5% moves were detected (`skeptic_exact_boundary_awkward_prices` passes). Denormal and inf changes are skipped, not emitted.
- **At-least-once with warm-up replay:** the offset discipline itself held in everything I tried. The weak point is the cooldown state (#4) and data-dependent fatal errors (#1, #2) turning at-least-once into "never again".
- **`latest` default:** see #5. I would commit the start offset and default to `earliest`.
- **Memory bounds:** as documented, there is no real global bound (10k symbols × 100k trades). Symbol length is unbounded too (#2). A bounded symbol format would close both.
- **Clock:** the live path should not depend on the host wall clock (#3).

## What I tried that did NOT break
- **kill -9 while a publish was retrying** against a deleted output topic: offset not stored, topic recreated, restart → UP and DOWN published exactly once, committed 3. (`skeptic_kill9_mid_publish_then_restart_delivers`)
- **kill -9 700 ms into a 200k-record warm-up:** committed offset unchanged, restart rebuilt state, the next spike was published exactly once with the correct baseline. (`skeptic_kill9_mid_warm_up`)
- **Same trades produced twice, plus late and duplicate-id trades across a restart:** no extra events. (`skeptic_replayed_duplicate_and_late_trades_across_restart`)
- **Backlog burst** of 30k trades, 300 symbols, 3 partitions: 300 events, all unique. (`skeptic_burst_backlog_all_spikes_once`)
- **Broker `docker restart` 1.5 s into publishing 2,000 spikes:** 2,000 unique events, all offsets committed. Caveat: most publishes probably finished before the broker went down, so this is weak evidence. (`skeptic_broker_restart_during_publishing`)
- **Garbage that did not crash it:**
  - 100k-deep nested JSON
  - 900 KB junk
  - invalid UTF-8
  - `p` of `1e400` or as a string
  - `-0.0` price
  - NUL in the symbol
  - year +262143
  - denormal price
  - empty payload
- **Exact 5% boundary** for 8 awkward price pairs: all detected.
- **Not tried:**
  - multi-instance rebalance, because the service uses manual assignment and the single-instance limit is documented
  - multi-broker leader change
  - real `market-producer` traffic

---

## Follow-up pass: re-attack of the fixes (2026-11-20, merged `2d64838`)

Same setup: my own broker `skeptic-kafka` on :29092, real release binary. Default
`cargo test -p spike-detector` is green: 68 passed, 0 failed, 23 ignored. Broker probes stay `#[ignore]`.

### Test reconciliation
- `skeptic_warm_up_cooldown_phase_matches_uninterrupted_run` and `skeptic_crash_after_publish_does_not_yield_second_event_inside_cooldown`:
  - They now model the service restart in `restart_like_service`: lookback replay, then `reset_cooldowns()`, then `seed_cooldown()` from events already in price-spikes with time >= `ts(C-1) - lookback`.
  - The crash test also asserts that post-crash events equal the uninterrupted run's, so it cannot pass by being quiet.
  - Without the seed they fail, as before.
- These two are `#[ignore = "documented limitation ..."]`:
  - `skeptic_lagging_partition_keeps_cooldown_when_another_partition_runs_ahead`
  - `skeptic_added_partition_records_are_not_lost`

### Original findings 1-6: re-verified fixed on the real service
- **1. Extreme timestamp:** `garbage_payloads`, `extreme_past_timestamp` pass.
- **2. Oversized symbol:** `oversized_symbol` passes.
- **3. Detector host clock:** `clock_behind_feed` passes, but see N2.
- **4. Restart mid-trend:** `restart_mid_trend` passes, but see N3 and N4.
- **5. `latest` restart:** `latest_default_restart` passes.
- **6. Bounded shutdown:** broker paused, then SIGTERM. Exit 0 after 10.06 s and 10.10 s with `WARNING final commit timed out`. A second SIGTERM 2 s in gave exit 130 after 2.01 s.

Also still passing:
- `kill9_mid_publish`
- `kill9_mid_warm_up`
- `replayed_duplicate_and_late`
- `burst_backlog`
- `broker_restart_during_publishing`

### New findings (each reproduced; tests in `skeptic_kafka.rs`, failing)

**N1. HIGH: one future-dated record wipes other symbols' state and blocks its own symbol** (`skeptic_future_record_timestamp_does_not_wipe_other_symbols`)
- **Repro:** one trade whose `t` and Kafka record timestamp are both 50 min ahead. Then 1,100 normal trades, then a real 10% QQQ move, then real trades on the poisoned symbol.
- **Seen:**
  - The broker accepted the record.
  - The service shows `future_rejected=0`, `evicted_symbols=51`, `out_of_order=2`, `published=0`.
  - QQQ's baseline was evicted by cleanup because the global watermark is 50 min ahead, so its spike was lost.
  - Real trades on the poisoned symbol are rejected as late until event time catches up.
- **Expected:** this is what the old wall-clock check prevented (30 s cap).
- **Where:** `main.rs:112` (`kafka::record_time(&msg)`). The skew reference is now producer-controlled CreateTime.
- **Unverified:** how far ahead the broker accepts records. KIP-937 `log.message.timestamp.after.max.ms` may cap it at 1 h on 4.x. I only tested 50 min.

**N2. HIGH (local setup): the clock-skew loss moved to the producer host** (`skeptic_producer_clock_behind_does_not_lose_spikes`)
- **Repro:** record CreateTime is 90 s behind the trade's `t`. This is what market-producer emits if its clock lags, and on WSL2 it shares the same drifting VM clock the original finding was about.
- **Seen:** `future_rejected=2`, 0 events, committed offset 2. The spike is lost.
- **Fix (suggested):** do not drop on skew alone. Either clamp the time used for the window and watermark to `min(t, reference + skew)` while keeping the trade, or bound the skew against a per-partition max of accepted times.

**N3. MEDIUM: a bogus or foreign event in price-spikes suppresses that symbol/direction for the process lifetime** (`skeptic_bogus_seed_event_does_not_suppress_real_spikes`)
- **Repro:** price-spikes holds one `SEED UP` event with `current_timestamp` 2099-12-31, plus malformed events and another symbol.
- **Seen:** the log says "cooldown seeded from 2 pair(s)". The real SEED UP spike is `suppressed_cooldown`. DOWN publishes normally.
- **Where:** `kafka.rs:570`. The seed takes the max `current_timestamp` with no bound, and `cooldown.rs` `try_fire` suppresses while `now < prev + d`. The same happens for any seed later than the trade being processed. For example, a fresh group with `earliest` replaying history while the topic holds recent events would suppress the whole backlog for those pairs (unverified).
- **Fix (suggested):** ignore seeds whose `current_timestamp` is beyond the replay's last event time (plus skew), and validate `schema_version`.
- Malformed events, other symbols and an empty or missing topic were handled without error.

**N4. MEDIUM: seeding over budget brings back finding 4, worse** (`skeptic_seed_budget_overflow_keeps_cooldown_phase`)
- **Repro:** 520k small records in price-spikes inside the lookback, then the restart-mid-trend scenario.
- **Seen:** `WARNING cooldown seeding hit its budget` (the 10 s limit, before the 500k-record cap), then "seeded from 0 pair(s)". The service emitted `TRND-UP-52` 40 s after the already-published `UP-48`, while the cooldown is 60 s. That is two UP events inside one cooldown with different ids.
- **Cause:** `main.rs:54` calls `reset_cooldowns()` even when the read is partial, which throws away the replay-derived cooldowns. The reader also scans oldest-first, so the most relevant (newest) events are the ones skipped.
- **Fix (suggested):** on budget overflow keep the replay-derived entries (merge with max, do not reset), or read the newest records first. Use `since_ms` per symbol key, or read backward from the high watermark.

### Edges that held
- **Year bounds** (`skeptic_year_bounds_are_exact`):
  - 1999-12-31T23:59:59.999999999Z rejected; 2000-01-01T00:00:00Z accepted.
  - 2099-12-31T23:59:59Z accepted; 2100-01-01 rejected.
  - Timezone offsets that cross a bound are handled correctly.
  - ±262143 rejected.
- **Trade id** `i64::MIN`/`MAX`: fine.
- **Symbol length:** 32 bytes accepted, 33 rejected. Multi-byte UTF-8 is counted in bytes (`€`×10 + `XX` = 32 accepted, `€`×11 = 33 rejected). Control characters are rejected.
- **Note (low, design):** U+200B zero-width space, U+202E RTL override and a leading space are accepted. So `"AAPL​"` is a distinct symbol, and lookalike symbols can reach downstream. An allowlist pattern would close this.
- **Startup offset commit:** I found no path where committing the resolved start skips unread records. Start = the resolved watermark the consumer is assigned at, and partitions with a committed offset are untouched. This is by reasoning plus `latest_default_restart`; I did not reproduce a failure.
