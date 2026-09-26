# Real-Time Market Data Pipeline

> **Status:** Active development

A Rust-based real-time market data pipeline that ingests live equity trades from Alpaca, publishes them through Kafka, persists historical data in PostgreSQL, maintains the latest trade state in Redis, and exposes both views through an Axum HTTP API.

The project is intentionally built as a small distributed system rather than a single application. It is being used to explore event-driven architecture, durable vs. low-latency storage, consumer reliability, backpressure, and operational behavior under load.

## Architecture

```text
Alpaca IEX WebSocket
        |
        v
  market-producer
      (Rust)
        |
        v
      Kafka
  stock-trades topic
        |
        v
  market-consumer
      (Rust)
      /     \
     v       v
PostgreSQL  Redis
 historical  latest
  trades     state
     \       /
      \     /
       v   v
      query-api
       (Axum)
      /       \
     v         v
/prices     /trades
```

### Why PostgreSQL and Redis?

The two stores serve different read patterns:

- **PostgreSQL** stores durable historical trade data and supports ordered historical queries.
- **Redis** stores the latest trade for each symbol for low-latency current-state lookups.

This lets the API use the store that best matches each request instead of treating both databases as interchangeable copies.

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
Kafka message
    |
    v
PostgreSQL insert
    |
    v
Redis update
    |
    v
Kafka offset commit
```

Kafka auto-commit is disabled. The consumer commits a message offset only after both PostgreSQL and Redis have succeeded.

PostgreSQL also has a unique index on `(symbol, trade_id)`, and inserts use `ON CONFLICT ... DO NOTHING`. This makes replayed trade events safe for the historical store if a message is processed again before its Kafka offset is successfully committed.

### `query-api`

Axum HTTP service listening on port `3000`.

Shared Redis and PostgreSQL handles are created at startup and provided to request handlers through Axum application state.

Current endpoints:

| Endpoint | Store | Purpose |
| --- | --- | --- |
| `GET /health` | — | Basic process health response |
| `GET /prices/{symbol}` | Redis | Latest known trade for a symbol |
| `GET /trades/{symbol}?limit=N` | PostgreSQL | Most recent historical trades |

The historical endpoint defaults to 10 trades. The requested limit is clamped to the range **1–100**.

Examples:

```bash
curl http://localhost:3000/health

curl http://localhost:3000/prices/AAPL

curl "http://localhost:3000/trades/AAPL?limit=20"
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
│   ├── market-producer/
│   ├── market-consumer/
│   ├── query-api/
│   ├── portfolio-domain/
│   └── load-generator/
├── db/
│   └── migrations/
│       └── 001_create_trades.sql
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
  psql -U market -d market_data \
  < db/migrations/001_create_trades.sql
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

Create a local `.env` file with your credentials and desired symbols:

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
curl http://localhost:3000/prices/AAPL
curl "http://localhost:3000/trades/AAPL?limit=10"
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
(symbol, trade_id)
```

provides deduplication for replayed events.

Redis stores the latest trade in a hash using keys of the form:

```text
latest:{symbol}
```

with fields for trade ID, price, size, timestamp, exchange, and tape.

## Reliability Model

The current consumer provides an **at-least-once-oriented processing design** rather than claiming end-to-end exactly-once delivery.

Important behaviors:

- Kafka automatic offset commits are disabled.
- PostgreSQL is written before Redis.
- The Kafka offset is committed only after both writes succeed.
- PostgreSQL trade inserts are idempotent for the same `(symbol, trade_id)`.
- If PostgreSQL succeeds but Redis or the Kafka commit fails, a replay can safely retry the event without duplicating the PostgreSQL trade.

This is intentionally simple for the first version and provides a base for later failure and recovery testing.

## Current Limitations

The current implementation favors clarity over maximum throughput or production hardening.

Known areas to improve include:

- Consumer processing is currently sequential and persists one trade at a time.
- Configuration such as database, Redis, and Kafka addresses is still hard-coded in several services.
- Kafka storage is not yet backed by a Docker named volume.
- `/health` is currently a process-level response and does not verify downstream dependencies.
- The API obtains a Redis connection from the client for each request.
- There is not yet structured observability for consumer lag, throughput, or latency.
- Local infrastructure is single-node and intended for development, not high availability.

## Planned Work

Near-term work is focused on distributed-systems behavior rather than adding more CRUD endpoints:

- Build out the load generator.
- Measure Kafka consumer lag and pipeline throughput.
- Deliberately create backpressure and observe recovery behavior.
- Batch PostgreSQL writes.
- Explore parallel consumers across Kafka partitions.
- Test failure scenarios involving PostgreSQL, Redis, Kafka, and process restarts.
- Add Kafka persistence to the local Docker environment.
- Move service configuration into environment-based configuration.
- Improve health/readiness checks and observability.
- Deploy the pipeline to AWS.

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

