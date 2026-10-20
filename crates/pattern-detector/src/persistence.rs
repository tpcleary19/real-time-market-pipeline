use crate::{Alert, Direction};
use sqlx::{Executor, Postgres};

/// Kafka source position makes retries of the same alert insert idempotent.
pub async fn save_alert<'e, E>(
    executor: E,
    alert: &Alert,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let direction = match alert.direction {
        Direction::Up => "Up",
        Direction::Down => "Down",
    };
    sqlx::query(
        "INSERT INTO pattern_alerts
         (symbol, direction, movement_percent, baseline_price, price,
          window_start, event_timestamp, kafka_topic, kafka_partition, kafka_offset)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING",
    )
    .bind(&alert.symbol)
    .bind(direction)
    .bind(alert.movement_percent)
    .bind(alert.baseline_price)
    .bind(alert.price)
    .bind(alert.window_start)
    .bind(alert.timestamp)
    .bind(topic)
    .bind(partition)
    .bind(offset)
    .execute(executor)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use sqlx::{Connection, PgConnection, Row};

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL; all changes are rolled back"]
    async fn postgres_round_trip_and_retry_deduplication() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut connection = PgConnection::connect(&std::env::var("TEST_DATABASE_URL")?).await?;
        let mut tx = connection.begin().await?;
        // Isolated temporary table shadows the live table; never writes production alerts.
        sqlx::query("CREATE TEMP TABLE pattern_alerts (LIKE public.pattern_alerts INCLUDING ALL) ON COMMIT DROP")
            .execute(&mut *tx).await?;
        let alert = Alert {
            symbol: "TEST_PERSISTENCE".into(),
            direction: Direction::Up,
            movement_percent: 2.0,
            baseline_price: 100.0,
            price: 102.0,
            window_start: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            timestamp: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
        };
        save_alert(&mut *tx, &alert, "test-topic", 1, 10).await?;
        save_alert(&mut *tx, &alert, "test-topic", 1, 10).await?;
        save_alert(&mut *tx, &alert, "test-topic", 1, 11).await?;
        let rows = sqlx::query("SELECT * FROM pattern_alerts ORDER BY kafka_offset")
            .fetch_all(&mut *tx)
            .await?;
        assert_eq!(rows.len(), 2);
        let row = &rows[0];
        assert_eq!(row.get::<String, _>("symbol"), alert.symbol);
        assert_eq!(row.get::<String, _>("direction"), "Up");
        assert_eq!(row.get::<f64, _>("movement_percent"), 2.0);
        assert_eq!(row.get::<f64, _>("baseline_price"), 100.0);
        assert_eq!(row.get::<f64, _>("price"), 102.0);
        assert_eq!(
            row.get::<chrono::DateTime<Utc>, _>("window_start"),
            alert.window_start
        );
        assert_eq!(
            row.get::<chrono::DateTime<Utc>, _>("event_timestamp"),
            alert.timestamp
        );
        assert_eq!(row.get::<String, _>("kafka_topic"), "test-topic");
        assert_eq!(row.get::<i32, _>("kafka_partition"), 1);
        assert_eq!(row.get::<i64, _>("kafka_offset"), 10);
        tx.rollback().await?;
        Ok(())
    }
}
