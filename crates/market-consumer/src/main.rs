use futures_util::StreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
};
use redis::AsyncCommands;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use chrono::Local;

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
    timestamp: String,

    #[serde(rename = "x")]
    exchange: Option<String>,

    #[serde(rename = "z")]
    tape: Option<String>,
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

    println!("{} Subscribed to Kafka topic stock-trades.", timestamp());

    let mut stream = consumer.stream();

    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                eprintln!("{} Kafka error: {error}", timestamp());
                continue;
            }
        };

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

        println!(
            "{} Kafka -> {} price={} size={}",
             timestamp(), trade.symbol, trade.price, trade.size
        );
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

        match result {
            Ok(_) => {
                println!("{} Postgres <- {}", timestamp(), trade.symbol);

                // Update Redis only after Postgres succeeded.
                let redis_key = format!("latest:{}", trade.symbol);

                let redis_result: redis::RedisResult<()> = redis_connection
                    .hset_multiple(
                        &redis_key,
                        &[
                            ("trade_id", trade.trade_id.to_string()),
                            ("price", trade.price.to_string()),
                            ("size", trade.size.to_string()),
                            ("timestamp", trade.timestamp.clone()),
                            ("exchange", trade.exchange.clone().unwrap_or_default()),
                            ("tape", trade.tape.clone().unwrap_or_default()),
                        ],
                    )
                    .await;

                match redis_result {
                    Ok(_) => {
                        println!("{} Redis <- {}", timestamp(), trade.symbol);

                        // Only commit after BOTH Postgres and Redis succeeded.
                        if let Err(error) = consumer.commit_message(&message, CommitMode::Async) {
                            eprintln!("{} Kafka commit failed: {error}", timestamp());
                        }
                    }

                    Err(error) => {
                        eprintln!("{} Redis update failed for {}: {}", timestamp(), trade.symbol, error);
                    }
                }
            }

            Err(error) => {
                eprintln!("{} Postgres insert failed for {}: {}", timestamp(), trade.symbol, error);
            }
        }
    }

    Ok(())
}
