# Multi-Agent Assignment: Rust Price Spike Detector

## Project: Real-Time Market Data Pipeline

**Repository:** `real-time-market-pipeline`  
**Task:** Implement and test a Rust-based market price spike detector.  
**Execution:** Autonomous multi-agent development using Herder, Batty, or another tmux-based agent coordinator.  
**Deliverable:** Working implementation on a feature branch, tests, documentation, and a pull request ready for human review.

**Important: Do not implement the AI investigation harness. That is a separate project phase that the repository owner will develop interactively.**

---

## 1. Project Background

The existing real-time market-data pipeline uses:

- Rust for ingestion and backend processing.
- Apache Kafka for event streaming.
- PostgreSQL for persistent trade storage.
- Redis for current market information.
- Axum for the backend API.
- React/TypeScript for the dashboard.

The existing Kafka topic `stock-trades` receives market trade events.

We want a standalone Rust service that consumes those events, maintains rolling price windows, identifies significant movements, and publishes spike events to a separate Kafka topic named `price-spikes`.

The future Python AI agent harness will consume `price-spikes` and investigate possible explanations for unusual market activity.

The detector must never wait for the AI system.

## 2. Inspect the Existing Repository First

Before implementing anything:

1. Locate the existing Rust Kafka consumers and producers.
2. Examine the stock-trade message schema and shared event crates.
3. Identify the project's Kafka topic, partitioning, and offset conventions.
4. Review Docker Compose, configuration, logging, and testing patterns.
5. Inspect existing PostgreSQL/Redis interactions to determine whether they are relevant.
6. Identify reusable libraries and code.

Do not assume a particular repository structure.

Reuse existing conventions and avoid unrelated refactoring.

Document any assumptions and architectural decisions.

## 3. Functional Requirements

Create a standalone Rust service that:

1. Subscribes to Kafka topic `stock-trades`.
2. Maintains rolling price windows independently for each symbol.
3. Tracks eligible trade prices and timestamps inside each window.
4. Detects upward and downward movements exceeding a configurable threshold.
5. Publishes qualifying events to Kafka topic `price-spikes`.
6. Expires obsolete trade observations.
7. Continues running independently of downstream consumers.

### Default Configuration

| Parameter | Default |
|---|---|
| Window duration | 600 seconds (10 minutes) |
| Percentage threshold | 5% |
| Cooldown duration | 600 seconds (10 minutes) |
| Source Kafka topic | `stock-trades` |
| Output Kafka topic | `price-spikes` |
| Consumer group | `price-spike-detector` |

Make these configurable through the project's existing configuration system, preferably environment variables.

### Baseline Selection

Use the earliest eligible trade price remaining within the rolling time window as the baseline.

Calculate:

`percentage_change = ((current_price - baseline_price) / baseline_price) * 100`

A spike occurs when the absolute percentage change is at least the configured threshold.

Do not require an entire window to elapse before detecting the first eligible spike.

Use trade event timestamps for the window calculation.

Document how late-arriving trades, missing timestamps, and out-of-order events are handled.

### Window Management

Avoid storing unlimited trade history.

Maintain only the information necessary to calculate movements accurately.

Prefer efficient per-symbol data structures and incremental processing over scanning every trade for every new message.

Ensure symbols are isolated so activity in one symbol does not affect another.

Consider idle-symbol cleanup and a configurable maximum for retained state where appropriate.

### Cooldown and Deduplication

A stock may remain above its threshold for several minutes. We do not want to launch thousands of identical AI investigations.

Implement a cooldown keyed by symbol and direction.

Example:

- AAPL increases 5.2%: publish an UP event.
- AAPL increases 5.5% thirty seconds later: suppress the duplicate investigation.
- AAPL decreases by a qualifying amount relative to its window baseline: evaluate independently as a DOWN event.
- After cooldown expires, another qualifying event may be published.

Maintain deterministic cooldown behavior under replay.

Document whether cooldown operates using event time or processing time and explain the decision.

## 4. Output Kafka Event

Create a versioned JSON event contract for the `price-spikes` topic.

Illustrative message:

```json
{
  "schema_version": 1,
  "event_id": "stable-unique-identifier",
  "symbol": "AAPL",
  "direction": "UP",
  "baseline_price": 200.00,
  "current_price": 211.00,
  "percentage_change": 5.5,
  "window_seconds": 600,
  "baseline_timestamp": "2026-11-18T10:00:00Z",
  "current_timestamp": "2026-11-18T10:10:00Z",
  "detected_at": "2026-11-18T10:10:01Z"
}
```

This is synthetic example data, not an actual trade.

Requirements:

- Use a consistent serialized price representation.
- Preserve precision appropriate for financial prices.
- Generate a stable `event_id` for deduplication.
- Use the stock symbol as the Kafka partition key.
- Include timestamps sufficient for downstream investigation.
- Define the schema in a reusable Rust type.
- Document field definitions and event versioning.

The future AI harness must be able to consume this event without needing to understand the detector's internal implementation.

## 5. Kafka Reliability

Review the existing pipeline's delivery guarantees before choosing an implementation.

Address:

- Consumer offset management.
- Producer acknowledgments.
- Transient Kafka failures.
- Repeated message delivery.
- Service restarts.
- Replay from committed offsets.
- Partition assignment and rebalancing.
- Graceful shutdown.

**Critical requirement:** Do not commit a source offset in a way that can silently discard a spike event that has not been successfully published.

Evaluate Kafka transactions or an appropriate replay-safe, at-least-once approach with stable event identifiers.

Document exactly what guarantees the selected approach provides.

Be particularly careful with rolling-window state and cooldown state after restarts. Stable event IDs alone do not restore state. Specify the replay, checkpoint, or warm-up strategy and test its behavior.

For an at-least-once design, downstream consumers must be able to deduplicate repeated spike events.

## 6. Edge Cases

Handle or explicitly document:

- Missing or invalid symbols.
- Zero or negative prices.
- Nonfinite numeric values where applicable.
- Duplicate trades.
- Out-of-order timestamps.
- Late-arriving market events.
- Symbols with insufficient historical data.
- No trades for extended periods.
- Large price gaps.
- High trade volumes.
- Kafka consumer lag.
- Producer failures.
- Service shutdown during event publication.
- State recovery after process restart.

Avoid panics on malformed external data.

## 7. Observability

Use the existing project's logging and metrics conventions.

Add useful operational visibility for:

- Trades consumed.
- Spikes detected.
- Spike events successfully published.
- Spikes suppressed by cooldown.
- Invalid trades rejected.
- Late or out-of-order trades.
- Kafka publishing errors.
- Consumer lag where available.
- Active symbols and retained state size.
- Event processing latency.

Avoid introducing a large monitoring dependency unless the existing project already supports it or a clear need exists.

## 8. Testing Requirements

Implement deterministic unit tests for:

1. Upward price changes exceeding the threshold.
2. Downward price changes exceeding the threshold.
3. Movements below the threshold.
4. Exact threshold boundary conditions.
5. Expiration of trades from rolling windows.
6. Incomplete windows.
7. Multiple symbols operating independently.
8. Cooldown suppression.
9. Cooldown expiration.
10. Duplicate trades.
11. Out-of-order and delayed trades.
12. Invalid prices and malformed messages.
13. Stable event ID generation.
14. Event serialization and deserialization.
15. State recovery or warm-up behavior after restart.

Add integration tests for Kafka message consumption and publication.

Test failure and recovery behavior, particularly around the relationship between publishing output events and committing source offsets.

If required infrastructure is unavailable, provide reproducible test commands and accurately state which tests were not executed.

Use synthetic data or replay fixtures so tests can run when the live stock market is closed.

## 9. Docker and Local Development

Integrate the new service with the existing development environment.

Where appropriate:

- Add the new Rust crate to the workspace.
- Add service configuration to Docker Compose.
- Ensure the `price-spikes` topic is created or documented.
- Make the service independently startable and stoppable.
- Provide commands for local development.
- Describe how to inspect generated spike events using Kafka tooling.

Avoid introducing unnecessary cloud dependencies or paid infrastructure.

The detector should work with the project's local Kafka environment.

## 10. Multi-Agent Responsibilities

### Agent 1: Lead / Architect

Responsibilities:

- Inspect the repository.
- Define implementation boundaries.
- Assign independent tasks.
- Coordinate work across agents.
- Resolve overlapping changes.
- Review architectural trade-offs.
- Ensure integration works.
- Prepare the final pull request summary.

The lead must not assume successful implementation solely from worker-agent reports. Verify the final integrated result.

### Agent 2: Rust Implementation

Responsibilities:

- Implement the rolling-window detector.
- Integrate Kafka input and output.
- Define the spike event schema.
- Add configuration.
- Implement cleanup and cooldown behavior.
- Integrate logging and metrics.
- Handle shutdown and recovery.

Coordinate file ownership with the other agents to minimize merge conflicts.

### Agent 3: Testing and Validation

Responsibilities:

- Develop deterministic test fixtures.
- Create unit and integration tests.
- Test threshold boundaries.
- Validate multi-symbol processing.
- Test duplicate delivery and replay.
- Test Kafka failure scenarios.
- Review numerical precision and timestamp behavior.
- Record test outcomes.

### Agent 4: Code Reviewer / Reliability

Responsibilities:

- Review concurrency and data structures.
- Evaluate memory growth and performance.
- Examine offset handling and producer reliability.
- Identify edge cases and possible message loss.
- Assess maintainability and idiomatic Rust practices.
- Verify that implementation matches the event contract.
- Provide actionable feedback before submission.

Agents should communicate through the orchestration framework, using nonoverlapping assignments wherever practical.

### Agent 5: Skeptic

Responsibilities:

- Assume the code is wrong and try to prove it. Your goal is to find a spike that is lost, duplicated with a different `event_id`, or wrong, or a state in which the service hangs, crashes in a loop or stores an offset before delivery.
- Do not rely on the other agents' reports or on a green test suite. Reproduce things yourself.
- Run the real service against a local Kafka broker (see `docs/spike-detector.md` and `docs/spike-detector-testing.md`). Try, for example: kill -9 at awkward moments (mid-publish, mid-warm-up), broker restart or pause during publishing, output topic missing or deleted, rebalance and partition changes, consumer lag and bursts, replay of the same data twice, out-of-order and duplicate trades across a restart, price gaps and thresholds at the exact boundary, huge or garbage payloads, and clock skew.
- Challenge the design as well as the code: event-time cooldown, f64 prices, at-least-once delivery with warm-up replay, the `latest` default, memory bounds. Say what you would change and why, but do not change it.
- For each finding give: severity, exact reproduction steps or a test, the observed vs. expected behavior, and the file and line. A finding without a reproduction must be labeled "unverified".
- Report to the Lead ("Sonnet 5.5 Leader") via SendMessage, ranked by severity. Also say plainly what you tried that did NOT break.

Boundaries:

- Read-only on all source and docs. The only files you may create are in `crates/spike-detector/tests/skeptic_*.rs` and `docs/spike-detector-skeptic.md`. Do not edit any other file, commit to any branch except your own worktree branch, push, or merge.
- Use your own Kafka broker, ports, topic names and consumer group names (prefix `skeptic-`) so you never touch the Testing agent's or the user's containers. Do not stop, remove or restart existing containers (`market-kafka`, `market-postgres`, `market-redis`). Remove only what you create.
- Do not add database or PostgreSQL access (see section 16).

Stop condition: stop after one full pass over the list above, or after 90 minutes, whichever is first. Do not keep searching for new issues after that. Send one final report and stop.

## 11. Agent Execution Rules

Follow these rules throughout development:

1. Work on a dedicated feature branch, such as `feature/price-spike-detector`.
2. Do not make unrelated changes.
3. Do not remove or overwrite existing functionality.
4. Reuse existing project patterns.
5. Prefer simple, maintainable Rust over unnecessary abstractions.
6. Run `cargo fmt`, `cargo clippy`, and appropriate tests.
7. Do not claim tests passed unless they actually ran successfully.
8. Document trade-offs and known limitations.
9. Do not commit secrets or credentials.
10. Do not deploy to production.
11. Do not merge changes into the default branch.
12. Stop once the implementation is ready for human review.

If architectural decisions are unclear, document the alternatives and choose the simplest defensible approach.

## 12. Explicitly Out of Scope

Do not implement:

- Python AI agents.
- LLM API integration.
- News retrieval.
- Web search.
- AI-generated market explanations.
- Agent memory or orchestration for investigations.
- React dashboard modifications.
- Trading execution or investment recommendations.
- Cloud deployment.

The repository owner will implement the AI investigation harness in a separate, interactive development phase.

## 13. Definition of Done

The assignment is complete when:

- The detector runs independently.
- It consumes existing `stock-trades` messages.
- It maintains bounded rolling-window state per symbol.
- It detects both UP and DOWN price spikes.
- It publishes valid events to `price-spikes`.
- Cooldown behavior prevents excessive repeat events.
- Relevant edge cases are tested.
- Kafka failure and replay behavior are documented and tested where possible.
- Docker/local operation is documented.
- Formatting and lint checks pass.
- Relevant tests pass, or limitations are explicitly reported.
- The implementation is committed to a feature branch.
- A pull request is prepared for human review.

## 14. Required Final Report

At completion, provide a concise report containing:

**Implementation summary**
- What was built.
- Which crates and files were added or modified.

**Architectural decisions**
- Window and baseline strategy.
- Out-of-order event handling.
- Kafka delivery guarantees.
- Cooldown and recovery strategy.

**Testing**
- Commands executed.
- Results.
- Any tests that could not run.

**Operational instructions**
- How to start the detector.
- How to generate a synthetic spike.
- How to consume and inspect `price-spikes` messages.
- How to stop the service.

**Outstanding issues**
- Known limitations.
- Potential future improvements.

**Pull request**
- Branch name.
- PR link, if created.
- Confirmation that the changes were not merged.

---

## 15. Stop Condition

**Once the Rust spike detector is implemented, tested, documented, and ready for review, STOP.**

Do not proceed to building the AI investigation harness.

That next phase will be developed separately, with the repository owner learning how to build an agent harness, implement LLM tool calling, retrieve market context, evaluate explanations, and integrate AI investigations into the pipeline.

The objective is a reliable, independently operating spike detector that emits useful market events and can later support an AI investigation service.

---

## 16. Persistence Ownership Decision

Spike persistence to PostgreSQL belongs to the future AI investigation harness, which will consume `price-spikes`. Do not add PostgreSQL writes, a database dependency, or a transactional outbox to `spike-detector`; it remains responsible for detecting spikes and publishing them to Kafka with its documented delivery and recovery guarantees.

The future harness will use the existing `DATABASE_URL` configuration (Neon when configured) for runtime persistence. Database integration tests must use a separate `TEST_DATABASE_URL`. Harness implementation and any work needed to expose its persisted spikes through the existing alert API remain outside this detector assignment.

Leave `pattern-detector` intact. The owner will remove it separately after the replacement persistence path and alert API compatibility have been implemented and validated.
