use chrono::Local;
use futures_util::StreamExt;
use market_events::Trade;
use rdkafka::{
    Offset, TopicPartitionList,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Message, OwnedMessage},
};
use sqlx::{Postgres, QueryBuilder, postgres::PgPoolOptions};
use std::collections::HashMap;
use tokio::time::{Duration, Instant};

fn timestamp() -> String {
    Local::now().format("%m-%d %H:%M:%S").to_string()
}

async fn process_batch(
    batch: &[OwnedMessage],
    pool: &sqlx::PgPool,
    redis_connection: &mut redis::aio::MultiplexedConnection,
    consumer: &StreamConsumer,
) {
    let mut trades = Vec::new();
    let mut highest_offsets: HashMap<i32, i64> = HashMap::new();

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
        highest_offsets
            .entry(message.partition())
            .and_modify(|offset| *offset = (*offset).max(message.offset()))
            .or_insert(message.offset());
    }

    if !trades.is_empty() {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
        INSERT INTO trades
        (trade_id, symbol, price, size, trade_timestamp, exchange, tape, trade_date)
        "#,
        );

        query_builder.push_values(&trades, |mut row, trade| {
            row.push_bind(trade.trade_id)
                .push_bind(&trade.symbol)
                .push_bind(trade.price)
                .push_bind(trade.size)
                .push_bind(&trade.timestamp)
                .push_bind(&trade.exchange)
                .push_bind(&trade.tape)
                .push_bind(
                    trade
                        .timestamp
                        .with_timezone(&chrono_tz::America::New_York)
                        .date_naive(),
                );
        });

        query_builder.push(
            r#"
        ON CONFLICT (symbol, trade_id, trade_date)
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

                let mut latest_by_symbol: HashMap<&str, &Trade> = HashMap::new();

                for trade in &trades {
                    latest_by_symbol.insert(&trade.symbol, trade);
                }

                println!(
                    "{} REDIS COLLAPSE trades={} symbols={}",
                    timestamp(),
                    trades.len(),
                    latest_by_symbol.len()
                );

                let mut redis_pipe = redis::pipe();

                for trade in latest_by_symbol.values() {
                    let redis_key = format!("latest:{}", trade.symbol);

                    redis_pipe
                        .cmd("HSET")
                        .arg(&redis_key)
                        .arg("trade_id")
                        .arg(trade.trade_id.to_string())
                        .arg("price")
                        .arg(trade.price.to_string())
                        .arg("size")
                        .arg(trade.size.to_string())
                        .arg("timestamp")
                        .arg(trade.timestamp.to_string())
                        .arg("exchange")
                        .arg(trade.exchange.clone().unwrap_or_default())
                        .arg("tape")
                        .arg(trade.tape.clone().unwrap_or_default())
                        .ignore();
                }

                let redis_start = Instant::now();

                let redis_result: redis::RedisResult<()> =
                    redis_pipe.query_async(redis_connection).await;

                let redis_elapsed = redis_start.elapsed();

                match redis_result {
                    Ok(_) => {
                        println!(
                            "{} REDIS BATCH symbols={} elapsed={:.2?}",
                            timestamp(),
                            latest_by_symbol.len(),
                            redis_elapsed
                        );

                        let mut offsets = TopicPartitionList::new();

                        for (&partition, &offset) in &highest_offsets {
                            if let Err(error) = offsets.add_partition_offset(
                                "stock-trades",
                                partition,
                                Offset::Offset(offset + 1),
                            ) {
                                eprintln!(
                                    "{} Failed to build Kafka commit for partition {}: {}",
                                    timestamp(),
                                    partition,
                                    error
                                );
                                return;
                            }
                        }

                        match consumer.commit(&offsets, CommitMode::Async) {
                            Ok(_) => {
                                println!(
                                    "{} KAFKA COMMIT partitions={} messages={}",
                                    timestamp(),
                                    highest_offsets.len(),
                                    trades.len()
                                );
                            }

                            Err(error) => {
                                eprintln!("{} Kafka commit failed: {}", timestamp(), error);
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("{} Redis pipeline failed: {}", timestamp(), error);
                    }
                }
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
    let mut received: u64 = 0;
    let mut flushed: u64 = 0;

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
        process_batch(&batch, &pool, &mut redis_connection, &consumer).await;
        batch.clear();
    }

    Ok(())
}
