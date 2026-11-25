# Market Investigator

Python service for investigating events from the Rust spike detector. The first stage consumes `price-spikes` and saves pending PostgreSQL jobs. A separate manual command can investigate one job with a web-enabled model and save its report; the intake consumer itself leaves jobs `pending`.

## Setup

From this directory:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
```

Set `DATABASE_URL` in the repository root `.env` or process environment. Existing process variables take precedence. Use the same runtime database as the Rust services (including Neon when configured). See `.env.example` for Kafka settings. No model API key is needed yet.

Apply `../db/migrations/004_create_investigation_jobs.sql` to that database before starting. For local Docker PostgreSQL, from the repository root:

```bash
docker exec -i market-postgres psql -v ON_ERROR_STOP=1 -U market -d market_data < db/migrations/004_create_investigation_jobs.sql
```

For Neon, apply that SQL through its SQL editor or `psql` using the intended connection. The service checks that the table exists; it does not apply migrations automatically.

Start Kafka and the Rust spike detector as described in [the detector runbook](../docs/spike-detector.md), then run from this directory:

```bash
.venv/bin/market-investigator
```

A new consumer group reads retained spikes from the beginning, including synthetic events already published. Existing groups resume their committed offsets. Ctrl-C or SIGTERM stops consumption.

Inspect jobs in your runtime database:

```sql
SELECT event_id, symbol, status, created_at
FROM investigation_jobs ORDER BY created_at DESC LIMIT 20;
```

## Delivery and failure behavior

Each job retains the validated spike payload and its Kafka position. A database transaction commits before the Kafka offset. `event_id` is the primary key: a replay does not create another job or reset an existing job's status. This is at-least-once intake with idempotent storage, not a distributed transaction.

Database errors, invalid payloads, unsupported schema versions, and Kafka commit failures stop the service. Restart after fixing the cause; unacknowledged records replay. An invalid event intentionally blocks progress rather than silently disappearing. There is no automatic retry supervisor or dead-letter queue yet. Logs identify the Kafka position without printing credentials or raw payloads.

The existing `/alerts` endpoints still serve historical `pattern_alerts`; investigation jobs and results are exposed separately by the existing Rust API at `/investigations` and `/investigations/{event_id}`. See the repository README for local development commands.

## Verification

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Database tests are skipped unless `TEST_DATABASE_URL` points to a separate test database. They use session-local temporary tables or isolated, disposable test schemas and do not modify live application tables. No Kafka broker is needed for these tests; live Kafka-to-database operation must be verified separately.

## Investigate one job manually

Apply `../db/migrations/005_add_investigation_results.sql` to the same runtime database. Install updated dependencies with `.venv/bin/python -m pip install -e .`.

Preview the oldest eligible job without changing its status or calling a model:

```bash
.venv/bin/market-investigate --preview
```

To use Claude, set these in the repository root `.env`:

```dotenv
INVESTIGATOR_PROVIDER=anthropic
ANTHROPIC_API_KEY=your_anthropic_api_key
ANTHROPIC_MODEL=claude-sonnet-5-5
```

Use a Claude API key from Anthropic's console. The selected model and web search must be available to your API account. To use OpenAI instead, set `INVESTIGATOR_PROVIDER=openai`, `OPENAI_API_KEY`, and optionally `OPENAI_MODEL` (default `gpt-5-mini`). If no provider is set, a configured `ANTHROPIC_API_KEY` or `CLAUDEKEY` selects Claude; otherwise OpenAI is used. `CLAUDEKEY` is accepted as an alias, with `ANTHROPIC_API_KEY` taking precedence when both exist. Preview requires no model API key.

Then investigate just one job:

```bash
.venv/bin/market-investigate
# Or choose an event shown in the preview:
.venv/bin/market-investigate --event-id YOUR_EVENT_ID
```

This sends the spike payload to the selected provider and performs paid model/web search requests. It makes one API request, caps output at 5,000 tokens, sets a 120-second HTTP timeout, and disables SDK retries. Claude uses the Messages API with at most three web searches; OpenAI uses Responses with at most three tool calls. These limits are not a dollar budget or a strict wall-clock deadline. No background investigations are started by the Kafka consumer.

The report asks the model to verify ticker identity, investigate around the event timestamp, cite evidence, distinguish later reporting, and acknowledge insufficient evidence. Synthetic test symbols may have no supported explanation. These are prompt instructions, not guarantees of factual accuracy: review reports and source dates manually. There is no independent market-data verification or automated evaluation yet.

The job moves from `pending` to `running` to `completed`, storing the report, source/citation annotations, model/response identifiers, and token usage under `result`. The CLI prints that result as JSON. Any future UI must render the saved citation annotations as clickable inline links. Claude citation blocks and OpenAI citation annotations retain their native formats in `result.output`; `result.provider` identifies the format. This is an initial hosted-tool integration; a Python-controlled tool loop can follow once this path is verified.

Claims are atomic with `FOR UPDATE SKIP LOCKED`. No database transaction is held during the model call. A claim abandoned by a process crash can be reclaimed after ten minutes; claim tokens prevent a stale worker from overwriting a newer result. An expired claim can still cause a duplicate paid request. Handled failures become `failed` with only the error class saved. A paused Claude turn (`pause_turn`), token-limit stop, refusal, or embedded web search error is a failure; this initial command does not issue continuation requests. To retry a failed job intentionally, set its status back to `pending` for that exact `event_id` in the database, provided it has fewer than three attempts. Exhausted jobs require manual review rather than automatic retry. Completed jobs are not selected again.

```sql
SELECT event_id, symbol, status, attempts, last_error,
       result ->> 'report' AS report
FROM investigation_jobs ORDER BY created_at DESC LIMIT 20;
```

API integration uses the [OpenAI quickstart](https://developers.openai.com/api/docs/quickstart) and [Responses web search guide](https://developers.openai.com/api/docs/guides/tools-web-search).

Claude integration follows the [Anthropic web search guide](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-search-tool).


## Alpaca asset validation before research

Every investigation first calls Alpaca's read-only `GET /v2/assets/{symbol}` with the existing `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` from the root `.env`. `APCA_API_BASE_URL` defaults to `https://paper-api.alpaca.markets`; set `https://api.alpaca.markets` when using live-account credentials. It uses the Trading API's asset directory, not the market-data host, and does not place orders.

The model receives the asset ID, name, exchange, class, current status, tradability, lookup URL, and lookup time. A data-only (`tradable=false`) US equity remains eligible. Inactive assets are still identifiable: their current status is supplied to the model rather than treated as proof they were inactive at the spike time.

A 404 completes the job with `result.outcome = 'not_found'` and no paid model call. Unsupported asset classes also skip model research. Neither means the event is proven synthetic. Authentication failures, rate limits, timeouts, malformed responses, and symbol mismatches fail the job and never become a false 'not found'. These handled failures require an intentional retry after fixing the cause.

This catalog is current metadata, not historical identity verification. It does not validate trade prices, volume, currency, or a news catalyst. Known test-symbol tagging remains a separate improvement: a real ticker can also be used in synthetic test data.

Reference: [Alpaca asset lookup](https://docs.alpaca.markets/us/reference/get-v2-assets-symbol_or_asset_id).

## Scheduled investigations and backlog

Apply migrations `006_add_investigation_hourly_budget.sql` and
`007_create_investigation_attempts.sql` from `../db/migrations/`, reinstall the
editable package, and run `.venv/bin/market-investigator-scheduler` alongside the
Kafka intake consumer (`.venv/bin/market-investigator`). Starting the scheduler
enables paid model requests. Restart it after changing its Python source.

Every 30 seconds the scheduler checks current-hour spikes. One minute after an
hour closes, it fills that hour's unused slots with the oldest pending spikes
from any earlier time before that hour's end. Hours use UTC boundaries and
spike `current_timestamp`; ties use intake time and event ID.

Each hourly budget allows three started attempts, including failures and asset
lookups that skip model research. Budget reservation and job claiming commit in
one transaction, so concurrent workers share the limit. Backfill consumes the
closed hour's allowance, not the current hour's. This is not a rolling 60-minute
request limit. Only the most recently closed hour is topped up; missed hourly
allowances from longer outages are not replayed, but their pending jobs remain
eligible for future backlog fills. Handled failures are not automatically retried.

### Crash recovery

Before each claim attempt, expired running jobs return to `pending`. A claim
expires ten minutes after `started_at` (a running job with no start timestamp is
also recovered). Clearing its claim token immediately prevents the old worker
from saving a result or marking a later attempt as failed. Concurrent recovery
uses row locks and skips locked jobs.

Recovery does not refund an hourly slot. The next attempt must reserve another
slot through the normal claim path and follows current-hour/backlog eligibility.
After three total attempts, an expired job becomes `failed` with
`last_error = 'ClaimExpiredAttemptLimit'`. Requeued jobs record `ClaimExpired`
until claimed again. Recovery warnings include event ID, resulting status, and
attempt count. Preview remains read-only and does not run recovery.

This is a fixed claim timeout, not proof that a worker has died. A slow or paused
worker exceeding ten minutes can be replaced. Its database writes are fenced,
but a provider request that finished before a crash may be repeated and billed
again. No heartbeat or provider-side exactly-once guarantee is implemented.

The manual CLI and its read-only preview select current-hour jobs. Research
requests include both the original event time and the investigation time so
older backlog events retain historical context.

## Attempt usage and estimated spending

Apply `../db/migrations/007_create_investigation_attempts.sql` before restarting
investigation workers. Then register the new reporting command:

```bash
.venv/bin/python -m pip install -e .
.venv/bin/market-investigator-usage --days 7
```

The report is read-only; it does not claim jobs, call Alpaca, or call a model.
Without reinstalling, run `.venv/bin/python -m market_investigator.usage_report --days 7`.
It includes daily attempt outcomes, known estimated spending, unknown-cost
counts, usage by provider/model, hourly budget utilization, and pending backlog.
Dates in the daily report use UTC; decimal costs are rendered as JSON strings.
Token/search sums cover only recorded values; check the unknown counters.
Cost per completed report includes the cost of other attempts that day, including
failures. It is null when any attempt that day has an unknown cost or there are
no completed reports. Expired attempts have no exact duration; their detection
time is retained in `finished_at` but excluded from latency averages.

An `investigation_jobs` row is the durable job and final report. Every claim
creates a separate `investigation_attempts` row keyed by its claim token in the
same transaction as the job claim and hourly budget reservation. A retry has a
new row. Each attempt records:

- Job ID, attempt number, budget hour, and current-hour/backfill/manual trigger.
- Requested provider/model, actual response model and response ID when received.
- Start, model-request start, response receipt, finish, and observed durations.
- Running/completed/skipped/failed/expired outcome and a safe error class.
- Raw provider token/tool usage plus normalized input, output, cache read/write,
  reasoning tokens, and billable searches when reported.
- Estimated USD cost, pricing snapshot/version, or a reason the cost is unknown.

Usage is persisted immediately after the provider response, before validating
its report. Token-limit, refusal, and other incomplete responses therefore
retain reported usage even though the job fails. If a model request times out or
its worker crashes, usage/cost remains unknown unless a response was durably
recorded. A late worker may update its own expired attempt's usage but cannot
change the expired outcome or the newer job claim. An asset-validation skip or
failure before the model request has known zero model/search cost, distinct from
missing usage. This does not account for infrastructure or Alpaca costs.

`usage.py` contains versioned standard synchronous/global list rates checked on
2026-11-23 for `claude-sonnet-5-5` and `gpt-5-mini` (including its documented dated
snapshot). Sources: [Claude pricing](https://platform.claude.com/docs/en/about-claude/pricing),
[GPT-5 mini pricing](https://developers.openai.com/api/docs/models/gpt-5-mini), and
[OpenAI search pricing](https://developers.openai.com/api/docs/pricing).
Each attempt stores its rates; later code/rate changes do not rewrite old estimates.
Add an explicit pricing entry/version when choosing another model. Unknown
models, missing billable search counts, and unsupported cache pricing produce
null costs with an explanation, rather than guessed totals. Claude's cache
reads/writes are separate from its input tokens; OpenAI's cached input is a subset
of input tokens and reasoning is a subset of output. Neither is double-counted.
OpenAI search counts come from reported `tool_usage.web_search.num_requests`,
not from counting output blocks. Older responses/SDKs may not expose that count.
These estimates exclude account discounts, taxes, and nonstandard service tiers;
compare them with provider billing. Usage tracking does not add a daily spending
limit.

Tracking begins with claims created by this version. Historical attempts and
requests started by an older worker are not reconstructed. Restart all workers
after applying the migration. Avoid resetting job attempt counters: the ledger
keeps a unique `(event_id, attempt_number)` for the complete recorded history.

To inspect recent attempts directly:

```sql
SELECT event_id, attempt_number, trigger, outcome, requested_model,
       input_tokens, output_tokens, billable_searches,
       estimated_cost_usd, cost_status, cost_unknown_reason, duration_ms
FROM investigation_attempts
ORDER BY started_at DESC
LIMIT 20;
```
