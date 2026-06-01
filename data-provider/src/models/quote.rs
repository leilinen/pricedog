use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub name: String,
    pub market: String,
    pub current_price: f64,
    pub prev_close: f64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub volume: f64,
    pub turnover: f64,
    pub change_amount: f64,
    pub change_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turnover_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pe_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circulating_market_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_market_value: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct BatchQuoteRequest {
    pub items: Vec<QuoteItem>,
}

#[derive(Debug, Deserialize)]
pub struct QuoteItem {
    pub symbol: String,
    pub market: String,
}
