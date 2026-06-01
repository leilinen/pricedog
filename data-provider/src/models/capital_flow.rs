use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalFlow {
    pub symbol: String,
    pub name: String,
    pub main_net_inflow: f64,
    pub main_net_inflow_pct: f64,
    pub super_net_inflow: f64,
    pub big_net_inflow: f64,
    pub mid_net_inflow: f64,
    pub small_net_inflow: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_net_5d: Option<f64>,
}
