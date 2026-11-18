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

**For the initial implementation, stop when the Rust spike detector is implemented, tested, documented, and ready for review. For the follow-up assignment in Section 16, stop when that additional task is implemented, tested, documented, and ready for review.**

Do not proceed to building the AI investigation harness.

That next phase will be developed separately, with the repository owner learning how to build an agent harness, implement LLM tool calling, retrieve market context, evaluate explanations, and integrate AI investigations into the pipeline.

The objective is a reliable, independently operating spike detector that emits useful market events and can later support an AI investigation service.

---

## 16. Additional Task: Persist Spikes Before Downstream Publication

### Goal and Scope

Extend the existing `spike-detector` service to persist detected spikes in PostgreSQL before publishing them to `price-spikes`, using a transactional outbox. The persisted spikes must also be available through the existing alert history API.

This is a follow-up to the initial implementation. Inspect the current code and documentation first and preserve its detection behavior and event contract. This section supersedes the publish-before-source-offset-commit requirement in Section 5 and the initial stop condition only where necessary for the durable outbox design. All other applicable requirements remain in force.

**Do not delete or modify `pattern-detector` as part of this task.** The repository owner will remove it after validating this replacement. Do not implement the AI investigation harness or make dashboard changes.

### Required Data Flow

1. Consume a trade from `stock-trades` and evaluate it using the existing detector.
2. For every detected spike, save the durable alert and its pending outbox event in one PostgreSQL transaction. Store the triggering source topic, partition, and offset for traceability.
3. Only after that transaction commits may the source offset become eligible for Kafka commit. Records producing no spike may advance normally after processing.
4. A background publisher independently reads committed pending outbox events, publishes them to `price-spikes` using the symbol as the key, waits for Kafka acknowledgment, and marks them sent in PostgreSQL.
5. Pending events survive process restarts and are retried until delivered or explicitly surfaced for operator intervention. Never silently discard an event.

PostgreSQL is now a required dependency for recording spikes. A downstream Kafka publication failure must not immediately block trade processing while PostgreSQL remains available and the pending backlog remains within documented operating limits. Do not wait for AI or other downstream consumers.

### Persistence and API Compatibility

- Add numbered, additive SQL migrations following repository conventions. Preserve existing alert history and allow `pattern-detector` to continue operating during migration. Do not rewrite applied migrations or drop existing tables/data.
- Use stable `event_id` values with database uniqueness constraints to make spike and outbox insertion idempotent under source replay, ambiguous database commit results, and retries. A duplicate replay must not reset an already-sent outbox row to pending.
- Persist the complete versioned event payload so retries publish the same event, including the original `detected_at`, without reconstructing it from current detector state.
- Preserve `GET /alerts` and `GET /alerts/{symbol}`, their response fields, filters, ordering, and access to historical alerts. Prefer reusing `pattern_alerts` if practical; if a new table is chosen, adapt the API while preserving old history.
- Map `UP`/`DOWN` to the API's existing `Up`/`Down`, `percentage_change` to `movement_percent`, `current_price` to `price`, `baseline_timestamp` to `window_start`, and `current_timestamp` to `event_timestamp`.
- Retain the existing configurable window, threshold, and cooldown semantics. Document that spike-detector defaults differ from pattern-detector; this task does not require identical alert timing or counts.

### Reliability and Recovery

- Do not advance a source offset past a spike whose database transaction has not successfully committed. Retry transient database failures without consuming onward in a way that loses the spike or advances past it. Handle ambiguous commit results through idempotent retries.
- Reconcile restart warm-up with the new acknowledgment boundary: committed source offsets now mean spikes are durably recorded, not necessarily published. Previously recorded pending events must be delivered by the outbox publisher even when warm-up suppresses replay output. Preserve recovery of rolling-window and cooldown state and document remaining assumptions.
- Keep downstream delivery at-least-once. A crash after Kafka acknowledgment but before marking an outbox row sent can publish a duplicate; consumers must deduplicate by `event_id`. Do not claim exactly-once delivery across PostgreSQL and Kafka.
- Keep producer acknowledgments and safe retry behavior. Track attempt counts, last error, and retry timing; avoid hot retry loops and make permanent errors visible without dropping pending events.
- Define ordering and concurrency behavior, including per-symbol event ordering during retries. Prevent concurrent workers from incorrectly marking unsent events as sent. A single publisher is acceptable if its supported deployment assumptions are explicit.
- Handle shutdown during persistence and publication safely. On startup, resume pending publication without requiring a new incoming trade.
- Define indexes, batch sizes, backlog monitoring, and retention/cleanup for sent outbox records. Never remove unsent events automatically. Document disk/backlog limits and what happens when capacity is exhausted.
- Expose useful logs/metrics for database failures, pending outbox count, oldest pending age, successful publication, retry failures, and publisher health. Document behavior during database and Kafka outages.

### Testing and Validation

Add meaningful deterministic tests and PostgreSQL/Kafka integration tests covering:

1. A persisted spike is visible through the existing alert API with the expected field mapping and filters; old alert history remains accessible.
2. Alert and outbox insertion are atomic: a failed transaction leaves neither record, and the source offset cannot advance past that failure.
3. Replaying a source record or retrying an ambiguous database commit produces one durable spike and one outbox record.
4. Pending events survive restart and publish without another trade arriving.
5. Kafka output failure leaves events pending while additional spikes can still be persisted.
6. A crash after database commit but before source offset commit safely replays without duplicate database rows.
7. A crash after Kafka acknowledgment but before the sent marker safely retries with the same event ID and payload; downstream deduplication is demonstrated.
8. Warm-up after a committed source offset does not lose pending outbox events or break window/cooldown recovery.
9. Shutdown, retry scheduling, permanent publication errors, and the chosen ordering/concurrency guarantees behave as documented.

Use synthetic trades and isolated test data. Run formatting, linting, and relevant tests. Report exactly which infrastructure-backed tests ran and which could not run; provide reproducible commands for the latter.

### Deliverables and Completion

- Working persistence and outbox publication integrated into `spike-detector`, with migrations, tests, configuration, and operational documentation.
- Updated delivery/recovery documentation replacing the current publish-before-offset-commit description, including the new PostgreSQL dependency and known limitations.
- A migration and cutover guide: apply migrations, start the updated service, verify database/API and Kafka output, and explain how to avoid duplicate user-visible alerts if both detectors run during validation. Identify remaining `pattern-detector` references and any other prerequisites for the owner's later removal; leave its crate and historical data intact.
- A review-ready feature branch and pull request, following the existing reporting and no-merge/no-production-deployment rules.

**Stop once this follow-up is implemented, tested, documented, and ready for review. The owner will validate it and delete `pattern-detector` separately.**
