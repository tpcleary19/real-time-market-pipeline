mod alerts;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderValue, Method, StatusCode},
    response::IntoResponse,
    routing::get,
};

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct AppState {
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
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => panic!("Failed to load .env; check its format and permissions"),
    }
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://market:market@localhost:5432/market_data".into());
    let postgres = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();
    let app_state = AppState { postgres };

    let origins = std::env::var("CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:5173,http://127.0.0.1:5173".into());
    let origins: Vec<HeaderValue> = origins
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().parse().expect("Invalid CORS_ALLOWED_ORIGINS"))
        .collect();
    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET]);
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/alerts", get(alerts::all))
        .route("/alerts/{symbol}", get(alerts::symbol))
        .route("/prices/{symbol}", get(get_price))
        .route("/trades/{symbol}", get(get_trades))
        .with_state(app_state)
        .layer(cors);

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3001".into())
        .parse()
        .expect("PORT must be a valid port number");
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port))
        .await
        .unwrap();

    println!(
        "Query API listening on port {}",
        listener.local_addr().unwrap().port()
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await
        .unwrap();
}

async fn health() -> &'static str {
    "OK"
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    match sqlx::query("SELECT 1").execute(&state.postgres).await {
        Ok(_) => (StatusCode::OK, "OK"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "Database unavailable"),
    }
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
    ORDER BY trade_timestamp DESC, id DESC
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
    let result = sqlx::query(
        "SELECT symbol, trade_id::text AS trade_id, price::text AS price,
         size::text AS size, trade_timestamp AS timestamp, exchange, tape
         FROM trades WHERE symbol = $1
         ORDER BY trade_timestamp DESC, id DESC LIMIT 1",
    )
    .bind(&symbol)
    .fetch_optional(&state.postgres)
    .await;
    match result {
        Ok(Some(row)) => Json(LatestTrade {
            symbol: row.get("symbol"),
            trade_id: row.get("trade_id"),
            price: row.get("price"),
            size: row.get("size"),
            timestamp: row
                .get::<chrono::DateTime<chrono::Utc>, _>("timestamp")
                .to_rfc3339(),
            exchange: row
                .get::<Option<String>, _>("exchange")
                .unwrap_or_else(|| "N/A".into()),
            tape: row
                .get::<Option<String>, _>("tape")
                .unwrap_or_else(|| "N/A".into()),
        })
        .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("No price found for {symbol}"),
        )
            .into_response(),
        Err(error) => {
            eprintln!("Postgres price query error: {error}");
            (StatusCode::INTERNAL_SERVER_ERROR, "Postgres query failed").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL; uses a session-local temporary table"]
    async fn latest_price_uses_postgres_event_order() -> Result<(), Box<dyn std::error::Error>> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&std::env::var("TEST_DATABASE_URL")?)
            .await?;
        sqlx::query("CREATE TEMP TABLE trades (LIKE public.trades INCLUDING ALL)")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO trades (id,trade_id,symbol,price,size,trade_timestamp,trade_date) VALUES
            (1,101,'AAPL',100.25,5,'2026-10-29 12:01Z','2026-10-29'),
            (2,102,'AAPL',99,2,'2026-10-29 12:00Z','2026-10-29'),
            (3,103,'AAPL',101.50,7,'2026-10-29 12:01Z','2026-10-29')",
        )
        .execute(&pool)
        .await?;
        let state = AppState {
            postgres: pool.clone(),
        };
        let response = get_price(State(state.clone()), Path("aapl".into()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 10000).await?;
        let trade: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(trade["symbol"], "AAPL");
        assert_eq!(trade["trade_id"], "103");
        assert_eq!(trade["size"], "7");
        assert_eq!(trade["exchange"], "N/A");
        chrono::DateTime::parse_from_rfc3339(trade["timestamp"].as_str().unwrap())?;
        let response = get_price(State(state), Path("UNKNOWN".into()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        pool.close().await;
        Ok(())
    }
}
