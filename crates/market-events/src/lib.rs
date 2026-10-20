#[derive(Debug, Clone, serde::Deserialize)]
pub struct Trade {
    #[serde(rename = "i")]
    pub trade_id: i64,

    #[serde(rename = "S")]
    pub symbol: String,

    #[serde(rename = "p")]
    pub price: f64,

    #[serde(rename = "s")]
    pub size: i64,

    #[serde(rename = "t")]
    pub timestamp: chrono::DateTime<chrono::Utc>,

    #[serde(rename = "x")]
    pub exchange: Option<String>,

    #[serde(rename = "z")]
    pub tape: Option<String>,
}
