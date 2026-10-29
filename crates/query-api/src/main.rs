mod alerts;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    postgres: PgPool,
}

#[derive(Serialize)]
struct LatestTrade {
    symbol: String,
    trade_id: String,
    price: String,
    size: String,
    timestamp: String,
    exchange: String,
    tape: String,
}

#[derive(Serialize)]
struct HistoricalTrade {
    trade_id: i64,
    symbol: String,
    price: String,
    size: i64,
    timestamp: String,
    exchange: Option<String>,
    tape: Option<String>,
}

#[derive(Deserialize)]
struct TradeQuery {
    limit: Option<i64>,
}

#[tokio::main]
async fn main() {
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let postgres = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://market:market@localhost:5432/market_data")
        .await
        .unwrap();
    let app_state = AppState {
        redis: redis_client,
        postgres,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/alerts", get(alerts::all))
        .route("/alerts/{symbol}", get(alerts::symbol))
        .route("/prices/{symbol}", get(get_price))
        .route("/trades/{symbol}", get(get_trades))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();

    println!("Query API listening on http://localhost:3001");

    axum::serve(listener, app).await.unwrap();
}

async fn health() -> &'static str {
    "OK"
}

async fn get_trades(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<TradeQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(10).clamp(1, 100);
    let row = sqlx::query(
        r#"
    SELECT
        trade_id,
        symbol,
        price::text AS price,
        size,
        trade_timestamp::text AS timestamp,
        exchange,
        tape
    FROM trades
    WHERE symbol = $1
    ORDER BY trade_timestamp DESC
    LIMIT $2
    "#,
    )
    .bind(symbol.to_uppercase())
    .bind(limit)
    .fetch_all(&state.postgres)
    .await;

    let rows = match row {
        Ok(rows) => rows,

        Err(error) => {
            eprintln!("Postgres query error: {error}");

            return (StatusCode::INTERNAL_SERVER_ERROR, "Postgres query failed").into_response();
        }
    };

    let trades: Vec<HistoricalTrade> = rows
        .into_iter()
        .map(|row| HistoricalTrade {
            trade_id: row.get("trade_id"),
            symbol: row.get("symbol"),
            price: row.get("price"),
            size: row.get("size"),
            timestamp: row.get("timestamp"),
            exchange: row.get("exchange"),
            tape: row.get("tape"),
        })
        .collect();

    Json(trades).into_response()
}

async fn get_price(State(state): State<AppState>, Path(symbol): Path<String>) -> impl IntoResponse {
    let symbol = symbol.to_uppercase();
    let key = format!("latest:{symbol}");

    let mut connection = match state.redis.get_multiplexed_async_connection().await {
        Ok(connection) => connection,

        Err(error) => {
            eprintln!("Redis connection error: {error}");

            return (StatusCode::INTERNAL_SERVER_ERROR, "Redis connection failed").into_response();
        }
    };

    let values: std::collections::HashMap<String, String> = match connection.hgetall(&key).await {
        Ok(values) => values,

        Err(error) => {
            eprintln!("Redis read error: {error}");

            return (StatusCode::INTERNAL_SERVER_ERROR, "Redis read failed").into_response();
        }
    };

    if values.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            format!("No price found for {symbol}"),
        )
            .into_response();
    }

    let trade = LatestTrade {
        symbol,
        trade_id: values
            .get("trade_id")
            .cloned()
            .unwrap_or_else(|| "N/A".into()),
        price: values.get("price").cloned().unwrap_or_else(|| "N/A".into()),
        size: values.get("size").cloned().unwrap_or_else(|| "N/A".into()),
        timestamp: values
            .get("timestamp")
            .cloned()
            .unwrap_or_else(|| "N/A".into()),
        exchange: values
            .get("exchange")
            .cloned()
            .unwrap_or_else(|| "N/A".into()),
        tape: values.get("tape").cloned().unwrap_or_else(|| "N/A".into()),
    };

    Json(trade).into_response()
}
