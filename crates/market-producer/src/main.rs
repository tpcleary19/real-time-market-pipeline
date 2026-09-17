use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load variables from the .env file.
    dotenv().ok();

    let api_key = env::var("APCA_API_KEY_ID")?;
    let api_secret = env::var("APCA_API_SECRET_KEY")?;
    let feed = env::var("ALPACA_FEED")
        .unwrap_or_else(|_| "iex".to_string());

    let symbols = env::var("ALPACA_SYMBOLS")
        .unwrap_or_else(|_| "AAPL".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    let url = format!("wss://stream.data.alpaca.markets/v2/{feed}");

    println!("Connecting to Alpaca {feed} feed...");

    let (mut socket, _) = connect_async(&url).await?;

    println!("WebSocket connected.");

    // Authenticate with Alpaca.
    let auth = json!({
        "action": "auth",
        "key": api_key,
        "secret": api_secret
    });

    socket
        .send(Message::Text(auth.to_string().into()))
        .await?;

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
        match message? {
            Message::Text(text) => {
                println!("{text}");
            }

            Message::Ping(data) => {
                socket.send(Message::Pong(data)).await?;
            }

            Message::Close(frame) => {
                println!("Alpaca closed connection: {frame:?}");
                break;
            }

            _ => {}
        }
    }

    Ok(())
}