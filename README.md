# Real-Time Market Data Pipeline

> **Status:** Active development

A Rust-based real-time market data pipeline that ingests live equity trades from Alpaca, publishes them through Kafka, persists historical data in PostgreSQL, maintains the latest trade state in Redis, and exposes latest prices, trade history, and historical alerts through an Axum HTTP API backed by PostgreSQL.

The project is intentionally built as a small distributed system rather than a single application. It is being used to explore event-driven architecture, durable vs. low-latency storage, consumer reliability, backpressure, and operational behavior under load.

## Architecture

```mermaid
flowchart TD
    A[Alpaca WebSocket] --> B[market-producer]
    B --> C[Kafka: stock-trades]
    C --> D[market-consumer]
    D --> E[(PostgreSQL / Neon)]
    D --> F[(Redis latest-state cache)]
    C --> G[spike-detector]
    G --> J[Kafka: price-spikes]
    E --> H[query-api / Cloud Run]
    H --> I[React dashboard]
```

PostgreSQL stores durable trades and legacy pattern alert history and serves all current API queries. The spike detector publishes new spikes to Kafka; the Python market investigator persists them as jobs and can investigate one manually. Exposing investigation results through the alert API remains future work. The consumer also maintains Redis latest-state hashes; the query API has no Redis dependency.

The query API is deployed on Google Cloud Run with Neon PostgreSQL. Kafka, ingestion, and the spike detector run independently of the API container. See [deployment instructions](deploy/README.md) and the [dashboard repository](https://github.com/tpcleary19/market-pipeline-dashboard).

## Components

### `market-producer`

Connects to the Alpaca market-data WebSocket, authenticates, subscribes to trade events, and publishes each trade to the Kafka `stock-trades` topic.

The Kafka message key is the stock symbol.

Configuration is read from environment variables:

- `APCA_API_KEY_ID`
- `APCA_API_SECRET_KEY`
- `ALPACA_FEED` (defaults to `iex`)
- `ALPACA_SYMBOLS` (comma-separated; defaults to `AAPL`)

### `market-consumer`

Consumes trade events from Kafka and writes them to both storage systems.

Processing order is deliberately:

```text
Kafka batch (up to 100 messages or 100 ms)
    |
    v
PostgreSQL batch insert
    |
    v
Redis pipeline update
    |
    v
Kafka offset commit
```

Kafka auto-commit is disabled. After both storage writes succeed, the consumer queues an asynchronous commit of the next offset for each partition in the batch. Queueing the commit does not confirm broker acknowledgement. See the reliability limitations below.

PostgreSQL has a partial unique index on `(symbol, trade_id, trade_date)` for non-null trade IDs. Inserts use `ON CONFLICT ... DO NOTHING` to deduplicate replayed trades within a New York trading date.

### `query-api`

Axum HTTP service listening on port `3001`.

A shared PostgreSQL pool is created at startup and provided to request handlers through Axum application state.

Current endpoints:

| Endpoint | Store | Purpose |
| --- | --- | --- |
| `GET /health` | — | Basic process health response |
| `GET /ready` | PostgreSQL | Database readiness; 503 on failure |
| `GET /investigations` | PostgreSQL | Recent investigation jobs; filter by symbol/status |
| `GET /investigations/{event_id}` | PostgreSQL | Saved report, citations, asset lookup, and usage |
| `GET /alerts` | PostgreSQL | Historical pattern alerts |
| `GET /alerts/{symbol}` | PostgreSQL | Historical alerts for one symbol |
| `GET /prices/{symbol}` | PostgreSQL | Latest known trade for a symbol |
| `GET /trades/{symbol}?limit=N` | PostgreSQL | Most recent historical trades |

The historical endpoint defaults to 10 trades. The requested limit is clamped to the range **1–100**.

Examples:

```bash
curl http://localhost:3001/health

curl http://localhost:3001/prices/AAPL

curl "http://localhost:3001/trades/AAPL?limit=20"
```

## Technology

- Rust / Tokio
- Axum
- Apache Kafka
- `rdkafka`
- PostgreSQL
- SQLx
- Redis
- Docker Compose
- Alpaca Market Data WebSocket API

The local Docker environment currently uses PostgreSQL 18, Redis 8, and Apache Kafka 4.1.0.

## Repository Layout

```text
.
├── crates/
│   ├── market-events/
│   ├── spike-detector/
│   ├── market-producer/
│   ├── market-consumer/
│   ├── query-api/
│   ├── portfolio-domain/
│   └── load-generator/
├── market-investigator/
├── db/
│   └── migrations/
│       ├── 001_create_trades.sql
│       ├── 002_add_trade_date.sql
│       ├── 003_create_pattern_alerts.sql
│       ├── 004_create_investigation_jobs.sql
│       └── 005_add_investigation_results.sql
├── Cargo.toml
└── docker-compose.yml
```

Not every workspace crate is fully implemented yet; this repository is under active development.

## Running Locally

### Prerequisites

You will need:

- Rust toolchain
- Docker with Docker Compose
- Alpaca market-data credentials

Native build dependencies required by the Rust Kafka/SSL stack may also be needed depending on your environment.

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts:

- PostgreSQL on `localhost:5432`
- Redis on `localhost:6379`
- Kafka on `localhost:9092`

### 2. Create the database schema

```bash
docker exec -i market-postgres \
  psql -v ON_ERROR_STOP=1 -U market -d market_data \
  < db/migrations/001_create_trades.sql
docker exec -i market-postgres \
  psql -v ON_ERROR_STOP=1 -U market -d market_data \
  < db/migrations/002_add_trade_date.sql
docker exec -i market-postgres \
  psql -v ON_ERROR_STOP=1 -U market -d market_data \
  < db/migrations/003_create_pattern_alerts.sql
```

### 3. Create the Kafka topic

The applications currently expect a topic named:

```text
stock-trades
```

For the current local setup, it can be created with three partitions:

```bash
docker exec market-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic stock-trades \
  --partitions 3 \
  --replication-factor 1
```

### 4. Configure Alpaca

Copy `.env.example` to `.env`, then configure your credentials and desired symbols. The example uses the local PostgreSQL database; for Neon, use your connection string with its supplied TLS settings. Apply the migrations to the database selected by `DATABASE_URL` using a direct connection.

```bash
cp .env.example .env
```

Example feed settings:

```text
APCA_API_KEY_ID=...
APCA_API_SECRET_KEY=...
ALPACA_FEED=iex
ALPACA_SYMBOLS=AAPL,MSFT,NVDA
```

Do not commit API credentials.

### 5. Start the consumer

```bash
cargo run -p market-consumer
```

### 6. Start the producer

In another terminal:

```bash
cargo run -p market-producer
```

Live Alpaca trades should begin flowing through Kafka into PostgreSQL and Redis while the market-data feed is producing subscribed trade events.

### 7. Start the query API

In another terminal:

```bash
cargo run -p query-api
```

Then query the pipeline:

```bash
curl http://localhost:3001/prices/AAPL
curl "http://localhost:3001/trades/AAPL?limit=10"
```

## Data Model

Historical trades are stored in PostgreSQL with:

- Alpaca trade ID
- symbol
- price
- size
- market timestamp
- exchange
- tape
- local receive timestamp

The primary historical access index is:

```text
(symbol, trade_timestamp DESC)
```

A unique index on:

```text
(symbol, trade_id, trade_date)
```

provides deduplication for replayed events.

Redis stores the latest trade in a hash using keys of the form:

```text
latest:{symbol}
```

with fields for trade ID, price, size, timestamp, exchange, and tape.

## Reliability Model

The consumer writes PostgreSQL before Redis and queues Kafka offset commits after both succeed. Historical inserts are idempotent for `(symbol, trade_id, trade_date)`.

**Recovery is incomplete:** a failed PostgreSQL or Redis batch is logged and cleared, and processing continues. A later successful batch can commit past failed records in the same partition. Asynchronous commit completion is not confirmed. Malformed records are logged and skipped. This implementation does not yet guarantee at-least-once delivery during storage failures.

Redis keeps the last arrival for each symbol within a batch, which can differ from the newest market timestamp when events arrive out of order. The API instead orders PostgreSQL trades by market timestamp and row ID.

## Current Limitations

The current implementation favors clarity over maximum throughput or production hardening.

Known areas to improve include:

- Consumer batches are processed sequentially, with up to 100 messages per batch.
- Configuration such as database, Redis, and Kafka addresses is still hard-coded in several services.
- Kafka storage is not yet backed by a Docker named volume.
- `/health` is currently a process-level response and does not verify downstream dependencies.
- Storage failures need retry or fail-stop handling before consumer offsets can be considered reliable.
- There is not yet structured observability for consumer lag, throughput, or latency.
- Local infrastructure is single-node and intended for development, not high availability.

## Planned Work

Near-term work is focused on distributed-systems behavior rather than adding more CRUD endpoints:

- Extend the existing synthetic load generator and record reproducible benchmark conditions.
- Measure Kafka consumer lag and pipeline throughput.
- Deliberately create backpressure and observe recovery behavior.
- Add failure/recovery tests for batched writes and offset commits.
- Explore parallel consumers across Kafka partitions.
- Test failure scenarios involving PostgreSQL, Redis, Kafka, and process restarts.
- Add Kafka persistence to the local Docker environment.
- Move service configuration into environment-based configuration.
- Extend existing health/readiness checks with operational metrics.
- Extend the current Cloud Run API deployment with a deployment plan for ingestion services.

A later extension may add a Python service for analytics, risk, signals, or AI-oriented workloads while keeping the latency-sensitive ingestion/data path in Rust.

## Project Goals

The goal is not just to move stock prices between technologies. The project is intended to demonstrate and explore practical backend and distributed-systems concerns:

- event-driven service design
- Kafka partitioning and consumer groups
- at-least-once processing and idempotency
- durable history vs. low-latency state
- backpressure and consumer lag
- batching and throughput
- failure recovery
- API design over multiple storage models
- containerized local development
- eventual cloud deployment

## Performance Baseline

Historical measurements from the earlier sequential consumer, before batching. These are development observations, not benchmarks for the current implementation.

Initial single-partition load test:

50,000-message synthetic load test

Load generator:
- ~13,000–14,000 messages/sec

Consumer:
- 1 Kafka partition
- 1 consumer
- Sequential processing
- ~385 messages/sec sustained
- Postgres average: ~1.97 ms/trade
- Redis average: ~0.55 ms/trade


Single partition / single consumer:
~385 trades/sec

Three partitions / three consumers:
~1,000 trades/sec aggregate

Scaling improvement:
~2.6×

Observed bottleneck shift:
Postgres latency increased from ~1.97 ms to ~2.4 ms
under three-consumer load, while Redis remained ~0.55 ms.
## Spike detector

The Rust `spike-detector` consumes `stock-trades` and publishes significant price movements to `price-spikes`. It replaces the retired `pattern-detector` service.

```bash
docker compose up -d kafka kafka-init
cargo run -p spike-detector
```

Ensure the `stock-trades` topic exists and run `market-producer` to supply live trades. Defaults are a 5% threshold over a 10-minute window, with a 10-minute cooldown per symbol and direction. See [the spike detector runbook](docs/spike-detector.md) for configuration, synthetic trades, output inspection, and delivery guarantees.

The detector does not write to PostgreSQL. The [Python market investigator](market-investigator/README.md) consumes `price-spikes` into durable pending investigation jobs; a manual command can generate a cited report, while API integration remains to be implemented. The existing `pattern_alerts` migration and alert endpoints remain available for historical data; they do not receive new spikes.

### Alert history API

The query API preserves these historical alert routes:
- `GET /alerts`: latest alerts across symbols.
- `GET /alerts/{symbol}`: latest alerts for one symbol (normalized to uppercase).

Both routes accept combinable query parameters:
- `limit`: defaults to 20, clamped to 1-100.
- `direction`: exactly `Up` or `Down`.
- `from`, `to`: RFC 3339 timestamps with timezone; from is inclusive and to is exclusive. When both are provided, from must precede to.
- `min_movement_pct`: finite nonnegative absolute percentage threshold, inclusive.

Results are JSON arrays ordered by event timestamp descending, then alert ID descending for ties. No matches returns an empty array. Invalid query values return HTTP 400; database failures return HTTP 500 without internal error details. Response fields are id, symbol, direction, movement_percent, baseline_price, price, window_start, event_timestamp. Prices/percentages are JSON numbers; timestamps are UTC RFC 3339 strings. Kafka metadata stays in storage for troubleshooting.

Examples:

```bash
curl "http://localhost:3001/alerts?limit=20"
curl "http://localhost:3001/alerts/AAPL?direction=Down&min_movement_pct=3"
curl "http://localhost:3001/alerts?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z"
```

After applying the alerts migration, run the route integration test against local PostgreSQL:

```bash
TEST_DATABASE_URL=postgres://market:market@localhost:5432/market_data cargo test -p query-api alert_routes_postgres -- --ignored
```

This test uses a single-connection pool and a session-local temporary table shadowing the public alerts table, leaving live alert history untouched.


### Database connection (local or Neon)

Run `cargo run -p market-consumer` and `cargo run -p query-api` from the
workspace root. Both load the Git-ignored `.env` file and read `DATABASE_URL`;
an existing process environment variable takes precedence. If unset, they use
the existing local PostgreSQL database. SQLx supports TLS with native trust roots;
keep the SSL parameters supplied by Neon. Never commit the connection string.
Redis and Kafka remain local for this stage. The API reads latest prices from PostgreSQL and does not require Redis.

Apply the SQL files in `db/migrations` in numeric order using a direct (unpooled)
database connection, before starting the consumer.

### API configuration

The API reads all price, trade, and alert data from PostgreSQL (Neon when configured).
The consumer still maintains local Redis, but the API has no Redis dependency.
Latest prices are selected by market timestamp descending, then row ID for ties.
Missing symbols return 404. Latest-price responses encode price, size, and trade ID as strings; trade-history responses encode price as a string and size/trade ID as integers.

- `PORT`: defaults to `3001`; binds to `0.0.0.0` for container hosting.
- `CORS_ALLOWED_ORIGINS`: comma-separated dashboard origins; defaults to
  `http://localhost:5173,http://127.0.0.1:5173`. Set the exact Vercel origin when deployed.
- `GET /health`: process liveness.
- `GET /ready`: checks PostgreSQL connectivity; returns 503 on failure.

Run from the project root: `cargo run -p query-api`. Test with:

```bash
curl http://localhost:3001/ready
curl http://localhost:3001/prices/AAPL
curl 'http://localhost:3001/trades/AAPL?limit=5'
curl 'http://localhost:3001/alerts?limit=5'
```

CORS controls browser access, not authentication. These routes expose read-only market data.


## Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Database integration tests are ignored by default and require `TEST_DATABASE_URL`; commands are documented in the relevant sections above. Unit tests do not require live Alpaca credentials. Do not use a production database for integration tests.


## Local investigation API development

Investigation results are served by the existing Rust `query-api`. Python consumes spikes, validates assets, and generates reports; HTTP reads do not call Alpaca or a model. The current development workflow runs locally. Deployment remains a separate step after the feature is ready.

Apply migrations 004 and 005 to the database configured by the root `.env`, then from the repository root run:

```bash
API_BIND_ADDRESS=127.0.0.1 PORT=3001 cargo run -p query-api
```

This runs the API on loopback while reading the existing configured database (Neon when configured). `API_BIND_ADDRESS` defaults to `0.0.0.0` for the existing container setup; the local command explicitly limits binding to this machine.

```bash
curl 'http://127.0.0.1:3001/investigations?limit=20'
curl 'http://127.0.0.1:3001/investigations?symbol=AAPL&status=completed'
curl 'http://127.0.0.1:3001/investigations/ZZZZ-UP-2-1795203467000000000'
```

The list returns a JSON array sorted by ingestion time (`created_at`) descending, then `event_id` descending for ties. `limit` defaults to 20 and is clamped to 1–100. `symbol` is trimmed and normalized to uppercase. `status` accepts `pending`, `running`, `completed`, or `failed`; invalid filters return 400. An unmatched list returns `[]`; an unknown detail ID returns 404. Database failures return 500 with a generic error.

List items contain `event_id`, `symbol`, `status`, the original `spike`, job timestamps, `provider`, `model`, `outcome`, and `has_report`. Detail adds `attempts`, `last_error`, and the complete stored `result` (nullable while pending). `result` includes report text, native provider citation/source blocks in `output`, asset lookup context, and usage. Results are returned as saved; the API does not manufacture structured catalyst or evidence scores from prose. A future dashboard must render the citation metadata as inline links. Internal claim tokens and Kafka positions are omitted.

`completed` means job processing finished, including validation-only outcomes such as `not_found`; it does not mean a market explanation was established. Use `outcome` and `provider` to distinguish those from model reports. Historical `/alerts` endpoints continue to read the legacy table.

For route integration tests, use a separate local test database with all migrations applied:

```bash
TEST_DATABASE_URL=postgres://market:market@localhost:5432/market_data_test cargo test -p query-api -- --include-ignored
```

The route tests use session-local temporary tables. They must not use the runtime `DATABASE_URL` as a substitute for `TEST_DATABASE_URL`.
