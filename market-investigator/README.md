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

Claims are atomic with `FOR UPDATE SKIP LOCKED`. No database transaction is held during the model call. A claim abandoned by a process crash can be reclaimed after ten minutes; claim tokens prevent a stale worker from overwriting a newer result. An expired claim can still cause a duplicate paid request. Handled failures become `failed` with only the error class saved. A paused Claude turn (`pause_turn`), token-limit stop, refusal, or embedded web search error is a failure; this initial command does not issue continuation requests. To retry a failed job intentionally, set its status back to `pending` for that exact `event_id` in the database. Completed jobs are not selected again.

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

Apply `../db/migrations/006_add_investigation_hourly_budget.sql`, reinstall the
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
eligible for future backlog fills. Failed and running jobs are not automatically
retried.

The manual CLI and its read-only preview select current-hour jobs. Research
requests include both the original event time and the investigation time so
older backlog events retain historical context.
