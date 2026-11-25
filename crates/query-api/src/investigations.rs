use crate::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Row, postgres::PgRow};

#[derive(Default, Deserialize)]
pub(crate) struct InvestigationQuery {
    symbol: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
}

impl InvestigationQuery {
    fn validate(&self) -> Result<(), &'static str> {
        if self
            .status
            .as_deref()
            .is_some_and(|s| !matches!(s, "pending" | "running" | "completed" | "failed"))
        {
            return Err("status must be pending, running, completed, or failed");
        }
        if self
            .symbol
            .as_ref()
            .is_some_and(|s| s.trim().is_empty() || s.len() > 32)
        {
            return Err("symbol must contain 1 to 32 bytes");
        }
        Ok(())
    }

    fn limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }
}

#[derive(Serialize)]
struct InvestigationSummary {
    event_id: String,
    symbol: String,
    status: String,
    spike: Value,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    outcome: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    has_report: bool,
}

impl InvestigationSummary {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            event_id: row.try_get("event_id")?,
            symbol: row.try_get("symbol")?,
            status: row.try_get("status")?,
            spike: row.try_get("spike")?,
            created_at: row.try_get("created_at")?,
            started_at: row.try_get("started_at")?,
            completed_at: row.try_get("completed_at")?,
            outcome: row.try_get("outcome")?,
            provider: row.try_get("provider")?,
            model: row.try_get("model")?,
            has_report: row.try_get("has_report")?,
        })
    }
}

#[derive(Serialize)]
struct InvestigationDetail {
    #[serde(flatten)]
    summary: InvestigationSummary,
    attempts: i32,
    last_error: Option<String>,
    result: Option<Value>,
}

const SUMMARY_COLUMNS: &str = "event_id, symbol, status, spike, created_at, started_at,
    completed_at, result->>'outcome' AS outcome, result->>'provider' AS provider,
    result->>'model' AS model,
    COALESCE(length(result->>'report') > 0, false) AS has_report";

pub(crate) async fn all(
    State(state): State<AppState>,
    Query(query): Query<InvestigationQuery>,
) -> Response {
    if let Err(message) = query.validate() {
        return (StatusCode::BAD_REQUEST, message).into_response();
    }
    let mut sql = QueryBuilder::<Postgres>::new(format!(
        "SELECT {SUMMARY_COLUMNS} FROM investigation_jobs WHERE TRUE"
    ));
    if let Some(symbol) = &query.symbol {
        sql.push(" AND symbol = ")
            .push_bind(symbol.trim().to_uppercase());
    }
    if let Some(status) = &query.status {
        sql.push(" AND status = ").push_bind(status);
    }
    sql.push(" ORDER BY created_at DESC, event_id DESC LIMIT ")
        .push_bind(query.limit());
    let result = match sql.build().fetch_all(&state.postgres).await {
        Ok(rows) => rows
            .iter()
            .map(InvestigationSummary::from_row)
            .collect::<Result<Vec<_>, _>>(),
        Err(error) => Err(error),
    };
    match result {
        Ok(rows) => Json(rows).into_response(),
        Err(error) => database_error(error),
    }
}

pub(crate) async fn detail(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Response {
    let mut sql = QueryBuilder::<Postgres>::new(format!(
        "SELECT {SUMMARY_COLUMNS}, attempts, last_error, result
         FROM investigation_jobs WHERE event_id = "
    ));
    sql.push_bind(event_id);
    let row = match sql.build().fetch_optional(&state.postgres).await {
        Ok(Some(row)) => row,
        Ok(None) => return (StatusCode::NOT_FOUND, "Investigation not found").into_response(),
        Err(error) => return database_error(error),
    };
    let result = (|| -> Result<InvestigationDetail, sqlx::Error> {
        Ok(InvestigationDetail {
            summary: InvestigationSummary::from_row(&row)?,
            attempts: row.try_get("attempts")?,
            last_error: row.try_get("last_error")?,
            result: row.try_get("result")?,
        })
    })();
    match result {
        Ok(detail) => Json(detail).into_response(),
        Err(error) => database_error(error),
    }
}

fn database_error(error: sqlx::Error) -> Response {
    eprintln!("Postgres investigation query error: {error}");
    (StatusCode::INTERNAL_SERVER_ERROR, "Postgres query failed").into_response()
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
    fn validates_status_symbol_and_limit() {
        assert_eq!(InvestigationQuery::default().limit(), 20);
        for (requested, expected) in [(-1, 1), (0, 1), (1000, 100)] {
            assert_eq!(
                InvestigationQuery {
                    limit: Some(requested),
                    ..Default::default()
                }
                .limit(),
                expected
            );
        }
        for status in ["pending", "running", "completed", "failed"] {
            assert!(
                InvestigationQuery {
                    status: Some(status.into()),
                    ..Default::default()
                }
                .validate()
                .is_ok()
            );
        }
        assert!(
            InvestigationQuery {
                status: Some("unknown".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            InvestigationQuery {
                symbol: Some(" ".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires separate TEST_DATABASE_URL; uses a session-local temporary table"]
    async fn investigation_routes_postgres() -> Result<(), Box<dyn std::error::Error>> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&std::env::var("TEST_DATABASE_URL")?)
            .await?;
        sqlx::query(
            "CREATE TEMP TABLE investigation_jobs (LIKE public.investigation_jobs INCLUDING ALL)",
        )
        .execute(&pool)
        .await?;
        let report = serde_json::json!({
            "provider": "anthropic", "model": "test-model", "report": "Possible catalyst, unverified",
            "output": [{"type": "text", "text": "Evidence", "citations": [{"url": "https://example.com/news"}]}]
        });
        sqlx::query("INSERT INTO investigation_jobs
            (event_id, symbol, status, spike, kafka_topic, kafka_partition, kafka_offset, created_at, result, claim_token)
            VALUES ('a','AAPL','pending',$1,'test',0,1,'2026-11-20 12:00Z',NULL,'private-claim'),
                   ('b','AAPL','completed',$1,'test',0,2,'2026-11-20 12:01Z',$2,NULL),
                   ('c','BING','completed',$1,'test',0,3,'2026-11-20 12:01Z',$3,NULL)")
            .bind(serde_json::json!({"direction": "UP", "percentage_change": 5.2}))
            .bind(&report)
            .bind(serde_json::json!({"provider":"none", "outcome":"not_found", "report":"Not found"}))
            .execute(&pool).await?;
        let app = Router::new()
            .route("/investigations", get(all))
            .route("/investigations/{event_id}", get(detail))
            .with_state(AppState {
                postgres: pool.clone(),
            });
        for (uri, ids) in [
            ("/investigations", vec!["c", "b", "a"]),
            ("/investigations?symbol=aapl", vec!["b", "a"]),
            ("/investigations?symbol=AAPL&status=pending", vec!["a"]),
            ("/investigations?status=completed&limit=1", vec!["c"]),
            ("/investigations?symbol=UNKNOWN", vec![]),
            ("/investigations?symbol=AAPL%27%20OR%20TRUE--", vec![]),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let rows: Vec<Value> =
                serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await?)?;
            assert_eq!(
                rows.iter()
                    .map(|r| r["event_id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                ids,
                "{uri}"
            );
            for row in rows {
                assert!(row.get("result").is_none());
                assert!(row.get("claim_token").is_none());
                assert!(row["spike"]["percentage_change"].is_number());
                assert!(row["created_at"].as_str().unwrap().ends_with('Z'));
            }
        }
        for uri in [
            "/investigations?status=unknown",
            "/investigations?limit=abc",
            "/investigations?symbol=%20",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
        for id in ["a", "b", "c"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/investigations/{id}"))
                        .body(Body::empty())?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
            let row: Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await?)?;
            assert!(row.get("claim_token").is_none());
            assert!(row.get("kafka_offset").is_none());
            match id {
                "a" => {
                    assert!(row["result"].is_null());
                    assert_eq!(row["has_report"], false);
                }
                "b" => {
                    assert_eq!(row["result"], report);
                    assert_eq!(row["has_report"], true);
                }
                "c" => {
                    assert_eq!(row["outcome"], "not_found");
                    assert_eq!(row["provider"], "none");
                }
                _ => unreachable!(),
            }
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/investigations/missing")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        pool.close().await;
        Ok(())
    }
}
