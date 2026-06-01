use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    pub ts: String,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turnover: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineResponse {
    pub market: String,
    pub symbol: String,
    pub interval: String,
    pub count: usize,
    pub klines: Vec<Kline>,
}
