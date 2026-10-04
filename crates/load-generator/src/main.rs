use chrono::Utc;
use futures::future::join_all;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde_json::json;
use std::env;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create Kafka producer");

    let trade_count: i64 = env::args()
        .nth(1)
        .unwrap_or_else(|| "10".to_string())
        .parse()
        .expect("Trade count must be a number");

    let starting_trade_id = Utc::now().timestamp_micros();

    let start = Instant::now();
    let mut batch = Vec::new();
    for offset in 0..trade_count {
        let trade_id = starting_trade_id + offset;
        let symbol = format!("LOAD{}", offset % 12);
        let trade = json!({
            "T": "t",
            "S": &symbol,
            "i": trade_id,
            "p": 250.25,
            "s": 100,
            "t": Utc::now().to_rfc3339(),
            "x": "V",
            "z": "C"
        });

        let payload = trade.to_string();
        let producer = producer.clone();

        let delivery = async move {
            producer
                .send(
                    FutureRecord::to("stock-trades")
                        .key(&symbol)
                        .payload(&payload),
                    Duration::from_secs(5),
                )
                .await
                .expect("Failed to send trade to Kafka");
        };

        batch.push(delivery);
        if batch.len() == 100 {
            join_all(batch).await;
            batch = Vec::new();
        }
    }

    if !batch.is_empty() {
        join_all(batch).await;
    }
    let elapsed = start.elapsed();
    let rate = trade_count as f64 / elapsed.as_secs_f64();

    println!(
        "Sent {trade_count} trades in {:.2?} ({rate:.0} trades/sec)",
        elapsed
    );
}
