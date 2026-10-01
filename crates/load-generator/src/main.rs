use chrono::Utc;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create Kafka producer");

    let starting_trade_id = Utc::now().timestamp_micros();

    for offset in 1..=10 {
        let trade_id = starting_trade_id + offset;
        let trade = json!({
            "T": "t",
            "S": "LOAD",
            "i": trade_id,
            "p": 250.25,
            "s": 100,
            "t": Utc::now().to_rfc3339(),
            "x": "V",
            "z": "C"
        });

        let payload = trade.to_string();

        producer
            .send(
                FutureRecord::to("stock-trades")
                    .key("LOAD")
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("Failed to send trade to Kafka");

        println!("Kafka <- {trade}");
    }
}
