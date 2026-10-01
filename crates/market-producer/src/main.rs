use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde_json::json;
use std::{env, time::Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use chrono::Local;

fn timestamp() -> String {
    Local::now().format("%m-%d %H:%M:%S").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load variables from the .env file.
    dotenv().ok();

    let api_key = env::var("APCA_API_KEY_ID")?;
    let api_secret = env::var("APCA_API_SECRET_KEY")?;
    let feed = env::var("ALPACA_FEED").unwrap_or_else(|_| "iex".to_string());

    let symbols = env::var("ALPACA_SYMBOLS")
        .unwrap_or_else(|_| "AAPL".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    let url = format!("wss://stream.data.alpaca.markets/v2/{feed}");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()?;

    println!("{} Kafka producer configured.", timestamp());

    loop {
        println!("Connecting to Alpaca {feed} feed...");

        let (mut socket, _) = match connect_async(&url).await {
            Ok(connection) => connection,

            Err(error) => {
                eprintln!("{} Alpaca connection failed: {error}", timestamp());
                eprintln!("{} Retrying in 5 seconds...", timestamp());

                tokio::time::sleep(Duration::from_secs(5)).await;

                continue;
            }
        };

        println!("{} WebSocket connected.", timestamp());

        // Authenticate with Alpaca.
        let auth = json!({
            "action": "auth",
            "key": api_key,
            "secret": api_secret
        });

        socket.send(Message::Text(auth.to_string().into())).await?;

        // Subscribe to trade events.
        let subscribe = json!({
            "action": "subscribe",
            "trades": symbols
        });

        socket
            .send(Message::Text(subscribe.to_string().into()))
            .await?;

        println!("Subscription request sent.");
        println!("Waiting for Alpaca messages...");

        while let Some(message) = socket.next().await {
            match message {
                Ok(message) => match message {
                    Message::Text(text) => {
                        let events: serde_json::Value = serde_json::from_str(&text)?;

                        if let Some(events) = events.as_array() {
                            for event in events {
                                // Alpaca uses "t" for a trade event.
                                if event.get("T").and_then(|v| v.as_str()) == Some("t") {
                                    let payload = serde_json::to_string(event)?;

                                    let symbol = event
                                        .get("S")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("UNKNOWN");

                                    producer
                                        .send(
                                            FutureRecord::to("stock-trades")
                                                .key(symbol)
                                                .payload(&payload),
                                            Duration::from_secs(5),
                                        )
                                        .await
                                        .map_err(|(e, _)| e)?;

                                    println!("{} Kafka <- {symbol}: {payload}", timestamp());
                                }
                            }
                        }
                    }

                    Message::Ping(data) => {
                        socket.send(Message::Pong(data)).await?;
                    }

                    Message::Close(frame) => {
                        println!("{} Alpaca closed connection: {frame:?}", timestamp());
                        break;
                    }

                    _ => {}
                },
                Err(error) => {
                    eprintln!("{} Alpaca WebSocket error: {error}", timestamp());
                    break;
                }
            }
        }
        println!("{} Alpaca connection ended. Reconnecting in 5 seconds....", timestamp());
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
