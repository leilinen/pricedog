use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotStock {
    pub symbol: String,
    pub market: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turnover: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotBoard {
    pub code: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turnover: Option<f64>,
}
