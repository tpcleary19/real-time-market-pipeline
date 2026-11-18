# Spike Detector: Lead Plan, Ownership, Decisions

Source assignment: `SPIKE_DETECTOR_AGENT_INSTRUCTIONS.md` (repo root, untracked). Out of scope: Python AI harness, LLM, dashboard, deploys.

## Repo findings
- `market_events::Trade` (fields i,S,p,s,t,x,z; price f64, event time `t`) is the input type. Topic `stock-trades`, local Kafka `localhost:9092`.
- Existing consumers use rdkafka 0.39 (`cmake-build`), manual offset storage (`pattern-detector`) or manual commit (`market-consumer`). `pattern-detector` has a similar window detector; reuse its idioms, do not modify it.
- No metrics crate; logging is `eprintln!`/stdout. Follow that (no new monitoring dependency).

## Lead decisions (binding)
1. **Event time everywhere** (window, cooldown). Replay is deterministic; `detected_at` is injected, never read inside the core.
2. Baseline = earliest trade still in window (`t >= current_t - window`). Spike when `abs(pct) >= threshold` (boundary inclusive).
3. Out-of-order (older than latest for the symbol) and duplicate `(symbol, trade_id, ET date)` trades are dropped and counted. Invalid symbol, price <= 0 or non-finite: dropped, counted.
4. Cooldown keyed by `(symbol, direction)`, in event time.
5. Delivery: **at-least-once**. Publish with acks=all and await delivery, THEN store the source offset. Never store an offset for a record whose spike has not been delivered. Stable `event_id` (see `event.rs`) lets consumers dedupe.
6. Restart: window/cooldown state is rebuilt by replaying the last `window + cooldown` seconds of `stock-trades` before resuming normal operation (warm-up). During warm-up, events are not re-published unless the id was not seen (implementation agent proposes mechanism; lead approves). Document limits.
7. Prices serialize as JSON numbers (f64), `percentage_change` rounded to 4 dp. Document in `docs/spike-detector.md`.

## File ownership (no overlap)
| Owner | Branch / worktree | Owns |
|---|---|---|
| Lead (Agent 1) | `feature/price-spike-detector` in repo root | `src/event.rs`, `Cargo.toml` workspace line, `docs/spike-detector-plan.md`, merges, PR |
| Rust Impl (Agent 2) | `feature/spike-detector-impl` in `../rtmp-wt/impl` | `crates/spike-detector/src/{main,config,detector,window,cooldown,kafka,metrics}.rs`, `crates/spike-detector/Cargo.toml`, `docker-compose.yml`, `docs/spike-detector.md` |
| Testing (Agent 3) | `feature/spike-detector-tests` in `../rtmp-wt/tests` | `crates/spike-detector/tests/**` (incl. fixtures), `docs/spike-detector-testing.md`. Uses only the API in `detector.rs` header and `event.rs`. If it needs a dev-dependency, ask lead (Cargo.toml is impl-owned). |
| Reviewer (Agent 4) | detached read-only worktree `../rtmp-wt/review` | Writes no code. Delivers findings to lead via message and `docs/spike-detector-review.md` (only file it may create). |

Workers commit to their own branch only. They never touch `main`, never push, never edit files they do not own. The lead merges.

## Definition of done / stop
Per assignment section 13. Lead verifies the integrated branch itself (`cargo fmt --check`, `cargo clippy`, `cargo test`), then opens a PR and STOPS. No merge.
