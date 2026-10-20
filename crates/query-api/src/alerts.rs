use crate::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Row};

#[derive(Default, Deserialize)]
pub(crate) struct AlertQuery {
    limit: Option<i64>,
    direction: Option<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    min_movement_pct: Option<f64>,
}

impl AlertQuery {
    fn validate(&self) -> Result<(), &'static str> {
        if self
            .direction
            .as_deref()
            .is_some_and(|d| !matches!(d, "Up" | "Down"))
        {
            return Err("direction must be Up or Down");
        }
        if matches!((self.from, self.to), (Some(from), Some(to)) if from >= to) {
            return Err("from must be earlier than to");
        }
        if self
            .min_movement_pct
            .is_some_and(|v| !v.is_finite() || v < 0.0)
        {
            return Err("min_movement_pct must be finite and nonnegative");
        }
        Ok(())
    }
    fn limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }
}

#[derive(Serialize)]
struct HistoricalAlert {
    id: i64,
    symbol: String,
    direction: String,
    movement_percent: f64,
    baseline_price: f64,
    price: f64,
    window_start: DateTime<Utc>,
    event_timestamp: DateTime<Utc>,
}

pub(crate) async fn all(
    State(state): State<AppState>,
    Query(query): Query<AlertQuery>,
) -> Response {
    history(state, None, query).await
}

pub(crate) async fn symbol(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<AlertQuery>,
) -> Response {
    history(state, Some(symbol.to_uppercase()), query).await
}

async fn history(state: AppState, symbol: Option<String>, query: AlertQuery) -> Response {
    if let Err(message) = query.validate() {
        return (StatusCode::BAD_REQUEST, message).into_response();
    }
    let mut sql = QueryBuilder::<Postgres>::new(
        "SELECT id, symbol, direction, movement_percent, baseline_price, price,
         window_start, event_timestamp FROM pattern_alerts WHERE TRUE",
    );
    if let Some(symbol) = symbol {
        sql.push(" AND symbol = ").push_bind(symbol);
    }
    if let Some(direction) = &query.direction {
        sql.push(" AND direction = ").push_bind(direction);
    }
    if let Some(from) = query.from {
        sql.push(" AND event_timestamp >= ").push_bind(from);
    }
    if let Some(to) = query.to {
        sql.push(" AND event_timestamp < ").push_bind(to);
    }
    if let Some(minimum) = query.min_movement_pct {
        sql.push(" AND ABS(movement_percent) >= ")
            .push_bind(minimum);
    }
    sql.push(" ORDER BY event_timestamp DESC, id DESC LIMIT ")
        .push_bind(query.limit());
    let rows = match sql.build().fetch_all(&state.postgres).await {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("Postgres alert query error: {error}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Postgres query failed").into_response();
        }
    };
    let alerts: Vec<_> = rows
        .into_iter()
        .map(|r| HistoricalAlert {
            id: r.get("id"),
            symbol: r.get("symbol"),
            direction: r.get("direction"),
            movement_percent: r.get("movement_percent"),
            baseline_price: r.get("baseline_price"),
            price: r.get("price"),
            window_start: r.get("window_start"),
            event_timestamp: r.get("event_timestamp"),
        })
        .collect();
    Json(alerts).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::Request,
        routing::get,
    };
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    #[test]
    fn validates_filters_and_caps_limit() {
        assert_eq!(AlertQuery::default().limit(), 20);
        assert_eq!(
            AlertQuery {
                limit: Some(1000),
                ..Default::default()
            }
            .limit(),
            100
        );
        assert_eq!(
            AlertQuery {
                limit: Some(0),
                ..Default::default()
            }
            .limit(),
            1
        );
        for value in [-1.0, f64::NAN, f64::INFINITY] {
            assert!(
                AlertQuery {
                    min_movement_pct: Some(value),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            AlertQuery {
                direction: Some("sideways".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        let now = Utc::now();
        assert!(
            AlertQuery {
                from: Some(now),
                to: Some(now),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL and applied migration; uses an isolated temporary table"]
    async fn alert_routes_postgres_filters_and_validation() -> Result<(), Box<dyn std::error::Error>>
    {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&std::env::var("TEST_DATABASE_URL")?)
            .await?;
        sqlx::query("CREATE TEMP TABLE pattern_alerts (LIKE public.pattern_alerts INCLUDING ALL)")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO pattern_alerts
            (id,symbol,direction,movement_percent,baseline_price,price,window_start,event_timestamp,kafka_topic,kafka_partition,kafka_offset)
            VALUES
            (1,'AAPL','Up',2,100,102,'2026-10-20 12:00Z','2026-10-20 12:01Z','test',0,1),
            (2,'AAPL','Down',-3,100,97,'2026-10-20 12:00Z','2026-10-20 12:02Z','test',0,2),
            (3,'MSFT','Up',4,100,104,'2026-10-20 12:00Z','2026-10-20 12:02Z','test',0,3)"
        ).execute(&pool).await?;
        let app = Router::new()
            .route("/alerts", get(all))
            .route("/alerts/{symbol}", get(symbol))
            .with_state(AppState {
                redis: redis::Client::open("redis://127.0.0.1/")?,
                postgres: pool.clone(),
            });
        for (uri, expected) in [
            ("/alerts", vec![3, 2, 1]),
            ("/alerts/aapl", vec![2, 1]),
            ("/alerts/UNKNOWN", vec![]),
            ("/alerts?direction=Up", vec![3, 1]),
            ("/alerts?min_movement_pct=3", vec![3, 2]),
            (
                "/alerts/aapl?direction=Down&min_movement_pct=3&limit=1",
                vec![2],
            ),
            (
                "/alerts?from=2026-10-20T12:01:00Z&to=2026-10-20T12:02:00Z",
                vec![1],
            ),
            ("/alerts?limit=1", vec![3]),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
            let rows: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
            assert_eq!(
                rows.iter()
                    .map(|r| r["id"].as_i64().unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{uri}"
            );
            if let Some(row) = rows.first() {
                assert!(row["movement_percent"].is_number());
                assert!(row["event_timestamp"].as_str().unwrap().ends_with('Z'));
            }
        }
        for uri in [
            "/alerts?direction=Other",
            "/alerts?min_movement_pct=-1",
            "/alerts?min_movement_pct=NaN",
            "/alerts?from=bad",
            "/alerts?limit=bad",
            "/alerts?from=2026-10-21T00:00:00Z&to=2026-10-20T00:00:00Z",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
        pool.close().await;
        Ok(())
    }
}
