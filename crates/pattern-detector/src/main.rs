use chrono::Local;
use market_events::Trade;
use pattern_detector::Detector;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "pattern-detector")
        .set("bootstrap.servers", "localhost:9092")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .create()?;
    consumer.subscribe(&["stock-trades"])?;
    eprintln!(
        "{} pattern-detector subscribed to stock-trades",
        Local::now()
    );
    let mut detector = Detector::default();
    loop {
        let message = consumer.recv().await?;
        if let Some(payload) = message.payload() {
            match serde_json::from_slice::<Trade>(payload) {
                Ok(trade) => {
                    if let Some(a) = detector.observe(trade) {
                        let mut out = io::stdout().lock();
                        writeln!(
                            out,
                            "{} MOVEMENT symbol={} direction={:?} change_pct={:.4} baseline={} price={} window_start={}",
                            a.timestamp,
                            a.symbol,
                            a.direction,
                            a.movement_percent,
                            a.baseline_price,
                            a.price,
                            a.window_start
                        )?;
                        out.flush()?;
                    }
                }
                Err(e) => eprintln!(
                    "{} Invalid trade partition={} offset={}: {e}",
                    Local::now(),
                    message.partition(),
                    message.offset()
                ),
            }
        } else {
            eprintln!(
                "{} Empty trade partition={} offset={}",
                Local::now(),
                message.partition(),
                message.offset()
            );
        }
        // Poison records are skipped. Only handled records become eligible for periodic commit.
        consumer.store_offset_from_message(&message)?;
    }
}
