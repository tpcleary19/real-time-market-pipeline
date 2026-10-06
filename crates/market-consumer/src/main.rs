use chrono::Local;
use futures_util::StreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Message, OwnedMessage},
};
use redis::AsyncCommands;
use serde::Deserialize;
use sqlx::{Postgres, QueryBuilder, postgres::PgPoolOptions};
use tokio::time::{Duration, Instant};

fn timestamp() -> String {
    Local::now().format("%m-%d %H:%M:%S").to_string()
}

#[derive(Debug, Deserialize)]
struct Trade {
    #[serde(rename = "i")]
    trade_id: i64,

    #[serde(rename = "S")]
    symbol: String,

    #[serde(rename = "p")]
    price: f64,

    #[serde(rename = "s")]
    size: i64,

    #[serde(rename = "t")]
    timestamp: chrono::DateTime<chrono::Utc>,

    #[serde(rename = "x")]
    exchange: Option<String>,

    #[serde(rename = "z")]
    tape: Option<String>,
}

async fn process_batch(batch: &[OwnedMessage], pool: &sqlx::PgPool) {
    let mut trades = Vec::new();

    for message in batch {
        let Some(payload) = message.payload_view::<str>() else {
            continue;
        };

        let payload = match payload {
            Ok(payload) => payload,
            Err(error) => {
                eprintln!("{} Invalid UTF-8 payload: {error}", timestamp());
                continue;
            }
        };

        let trade: Trade = match serde_json::from_str(payload) {
            Ok(trade) => trade,
            Err(error) => {
                eprintln!("{} Invalid trade: {error}", timestamp());
                eprintln!("{} Payload: {payload}", timestamp());
                continue;
            }
        };

        trades.push(trade);
    }

    if !trades.is_empty() {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
        INSERT INTO trades
            (trade_id, symbol, price, size, trade_timestamp, exchange, tape)
        "#,
        );

        query_builder.push_values(&trades, |mut row, trade| {
            row.push_bind(trade.trade_id)
                .push_bind(&trade.symbol)
                .push_bind(trade.price)
                .push_bind(trade.size)
                .push_bind(&trade.timestamp)
                .push_bind(&trade.exchange)
                .push_bind(&trade.tape);
        });

        query_builder.push(
            r#"
        ON CONFLICT (symbol, trade_id)
        WHERE trade_id IS NOT NULL
        DO NOTHING
        "#,
        );

        let start = Instant::now();

        let result = query_builder.build().execute(pool).await;

        match result {
            Ok(result) => {
                println!(
                    "{} POSTGRES BATCH trades={} rows_affected={} elapsed={:.2?}",
                    timestamp(),
                    trades.len(),
                    result.rows_affected(),
                    start.elapsed()
                );
            }

            Err(error) => {
                eprintln!("{} Postgres batch insert failed: {}", timestamp(), error);
            }
        }
    }
    println!(
        "{} PROCESS batch_size={} trades={}",
        timestamp(),
        batch.len(),
        trades.len()
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "postgres://market:market@localhost:5432/market_data";

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    println!("{} Connected to Postgres.", timestamp());
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;

    let mut redis_connection = redis_client.get_multiplexed_async_connection().await?;

    println!("{} Connected to Redis.", timestamp());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "market-consumer")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .create()?;

    consumer.subscribe(&["stock-trades"])?;

    //    println!("{} Subscribed to Kafka topic stock-trades.", timestamp());

    let mut batch: Vec<OwnedMessage> = Vec::new();
    let mut stream = consumer.stream();
    let mut trade_count: u64 = 0;
    let mut postgres_total_us: u128 = 0;
    let mut redis_total_us: u128 = 0;
    let mut batch_start = Instant::now();
    let mut received: u64 = 0;
    let mut flushed: u64 = 0;
    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                eprintln!("{} Kafka error: {error}", timestamp());
                continue;
            }
        };

        loop {
            if batch.is_empty() {
                match stream.next().await {
                    Some(Ok(message)) => {
                        batch.push(message.detach());
                        received += 1;
                    }
                    Some(Err(error)) => {
                        eprintln!("{} Kafka error: {error}", timestamp());
                        continue;
                    }
                    None => break,
                }
            }

            let deadline = Instant::now() + Duration::from_millis(100);

            while batch.len() < 100 {
                tokio::select! {
                    message = stream.next() => {
                        match message {
                            Some(Ok(message)) => {
                                batch.push(message.detach());
                                received += 1;
                            }

                            Some(Err(error)) => {
                                eprintln!("{} Kafka error: {error}", timestamp());
                            }

                            None => break,
                        }
                    }

                    _ = tokio::time::sleep_until(deadline) => {
                        break;
                    }
                }
            }
            flushed += batch.len() as u64;
            println!(
                "{} BATCH size={} received={} flushed={}",
                timestamp(),
                batch.len(),
                received,
                flushed
            );
            process_batch(&batch, &pool).await;
            batch.clear();
        }

        let Some(payload) = message.payload_view::<str>() else {
            continue;
        };

        let payload = payload?;

        let trade: Trade = match serde_json::from_str(payload) {
            Ok(trade) => trade,
            Err(error) => {
                eprintln!("{} Invalid trade: {error}", timestamp());
                eprintln!("{} Payload: {payload}", timestamp());
                continue;
            }
        };

        /*        println!(
            "{} Kafka -> {} price={} size={}",
            timestamp(),
            trade.symbol,
            trade.price,
            trade.size
        );*/
        let postgres_start = Instant::now();
        let result = sqlx::query(
            r#"
            INSERT INTO trades
                (trade_id, symbol, price, size, trade_timestamp, exchange, tape)
            VALUES
                ($1, $2, $3, $4, $5::timestamptz, $6, $7)
            ON CONFLICT (symbol, trade_id)
            WHERE trade_id IS NOT NULL
            DO NOTHING
            "#,
        )
        .bind(&trade.trade_id)
        .bind(&trade.symbol)
        .bind(trade.price)
        .bind(trade.size)
        .bind(&trade.timestamp)
        .bind(&trade.exchange)
        .bind(&trade.tape)
        .execute(&pool)
        .await;

        let postgres_elapsed = postgres_start.elapsed();
        if postgres_elapsed.as_millis() > 20 {
            println!("{} SLOW Postgres: {:?}", timestamp(), postgres_elapsed);
        }
        postgres_total_us += postgres_elapsed.as_micros();
        match result {
            Ok(_) => {
                //                println!("{} Postgres <- {}", timestamp(), trade.symbol);

                // Update Redis only after Postgres succeeded.
                let redis_key = format!("latest:{}", trade.symbol);
                let redis_start = Instant::now();
                let redis_result: redis::RedisResult<()> = redis_connection
                    .hset_multiple(
                        &redis_key,
                        &[
                            ("trade_id", trade.trade_id.to_string()),
                            ("price", trade.price.to_string()),
                            ("size", trade.size.to_string()),
                            ("timestamp", trade.timestamp.to_string()),
                            ("exchange", trade.exchange.clone().unwrap_or_default()),
                            ("tape", trade.tape.clone().unwrap_or_default()),
                        ],
                    )
                    .await;
                let redis_elapsed = redis_start.elapsed();

                if redis_elapsed.as_millis() > 20 {
                    println!("{} SLOW Redis: {:?}", timestamp(), redis_elapsed);
                }

                redis_total_us += redis_elapsed.as_micros();
                trade_count += 1;

                match redis_result {
                    Ok(_) => {
                        println!("{} Redis <- {}", timestamp(), trade.symbol);

                        // Only commit after BOTH Postgres and Redis succeeded.
                        if let Err(error) = consumer.commit_message(&message, CommitMode::Async) {
                            eprintln!("{} Kafka commit failed: {error}", timestamp());
                        }
                    }

                    Err(error) => {
                        eprintln!(
                            "{} Redis update failed for {}: {}",
                            timestamp(),
                            trade.symbol,
                            error
                        );
                    }
                }
            }

            Err(error) => {
                eprintln!(
                    "{} Postgres insert failed for {}: {}",
                    timestamp(),
                    trade.symbol,
                    error
                );
            }
        }
        if trade_count % 1000 == 0 {
            let batch_elapsed = batch_start.elapsed().as_secs_f64();
            let throughput = 1000.0 / batch_elapsed;
            println!(
                "{} STATS trades={} throughput={:.0}/sec postgres_avg={:.2}ms redis_avg={:.2}ms",
                timestamp(),
                trade_count,
                throughput,
                postgres_total_us as f64 / trade_count as f64 / 1000.0,
                redis_total_us as f64 / trade_count as f64 / 1000.0,
            );
            batch_start = Instant::now();
        }
    }

    Ok(())
}
