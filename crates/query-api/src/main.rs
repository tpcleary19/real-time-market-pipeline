use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};

use redis::AsyncCommands;
use serde::Serialize;

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
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

#[tokio::main]
async fn main() {
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let app_state = AppState {
        redis: redis_client,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/prices/{symbol}", get(get_price))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("Query API listening on http://localhost:3000");

    axum::serve(listener, app).await.unwrap();
}

async fn health() -> &'static str {
    "OK"
}

async fn get_price(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    let symbol = symbol.to_uppercase();
    let key = format!("latest:{symbol}");

    let mut connection = match state.redis
        .get_multiplexed_async_connection()
        .await
    {
        Ok(connection) => connection,

        Err(error) => {
            eprintln!("Redis connection error: {error}");

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Redis connection failed",
            )
                .into_response();
        }
    };

    let values: std::collections::HashMap<String, String> = match connection.hgetall(&key).await {
        Ok(values) => values,

        Err(error) => {
            eprintln!("Redis read error: {error}");

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Redis read failed",
            )
                .into_response();
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
        trade_id: values.get("trade_id").cloned().unwrap_or_else(|| "N/A".into()),
        price: values.get("price").cloned().unwrap_or_else(|| "N/A".into()),
        size: values.get("size").cloned().unwrap_or_else(|| "N/A".into()),
        timestamp: values.get("timestamp").cloned().unwrap_or_else(|| "N/A".into()),
        exchange: values.get("exchange").cloned().unwrap_or_else(|| "N/A".into()),
        tape: values.get("tape").cloned().unwrap_or_else(|| "N/A".into()),
    };

    Json(trade).into_response()
}