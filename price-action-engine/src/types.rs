use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    // PriceDog backtest data spec:
    // ts is the bar open time in RFC3339 UTC, and the bar becomes fully usable at ts + interval.
    pub ts: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    #[serde(default)]
    pub volume: f64,
    #[serde(default)]
    pub turnover: f64,
}

pub struct KlineFetch {
    pub klines: Vec<Kline>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub market: String,
    #[serde(default)]
    pub name: String,
    pub current_price: f64,
    #[serde(default)]
    pub change_pct: f64,
    #[serde(default)]
    pub change_amount: f64,
    #[serde(default)]
    pub volume: f64,
    #[serde(default)]
    pub turnover: f64,
    #[serde(default)]
    pub open_price: f64,
    #[serde(default)]
    pub high_price: f64,
    #[serde(default)]
    pub low_price: f64,
    #[serde(default)]
    pub prev_close: f64,
    #[serde(default)]
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderStatus {
    pub provider: String,
    pub market: String,
    pub channel: String,
    pub status: String,
    pub last_connected_at: Option<String>,
    pub last_message_at: Option<String>,
    pub last_closed_kline_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Indicators {
    pub ema20: Option<f64>,
    pub ema5: Option<f64>,
    pub atr14: Option<f64>,
    pub atr20: Option<f64>,
    pub ema20_position: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub amplitude: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Signal {
    pub signal_type: String,
    pub direction: String,
    pub score: f64,
    pub ema20_entry_score: f64,
    pub entry_price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub target_price: Option<f64>,
    pub reason: String,
    pub evidence: Value,
}

/// Result of Layer 1+2 evaluation for a breakout bar candidate.
pub struct BreakoutCandidate {
    pub direction: String,
    pub range_high: f64,
    pub range_low: f64,
    pub body_ratio: f64,
    pub close_location: f64,
    pub distance_atr: f64,
    pub breakout_bar_score: f64,
    #[allow(dead_code)]
    pub atr14: f64,
    pub stop_loss: f64,
    pub target_price: f64,
}

// ---------------------------------------------------------------------------
// Signal Bar Model -- signal K-line recognition model
// ---------------------------------------------------------------------------

/// Basic features of a single K-line
pub struct BarFeatures {
    pub body: f64,  // B = C - O (positive=bullish, negative=bearish)
    pub range: f64, // R = H - L
    pub p_b: f64,   // body ratio = |B| / R
    pub p_c: f64,   // close location = (C - L) / R
    pub p_u: f64,   // upper shadow ratio = (H - max(C,O)) / R
    pub p_d: f64,   // lower shadow ratio = (min(C,O) - L) / R
}

/// Market context evaluation result
pub struct ContextResult {
    pub cr: f64,                   // channel ratio = (H_N - L_N) / ATR_M
    pub trend_dir: i32,            // trend direction: 1=bullish, -1=bearish, 0=neutral
    pub ma_cross_dir: Option<i32>, // golden/death cross direction: 1=golden, -1=death, None=not detected
    pub f_ratio: f64,              // bullish/bearish force ratio
    pub is_valid: bool,            // CR >= theta (not narrow channel)
}

#[derive(Debug, Deserialize)]
pub struct KlineQuery {
    pub interval: Option<String>,
    pub limit: Option<usize>,
    pub refresh: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct DataHealthQuery {
    pub interval: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SampleCandidateQuery {
    pub interval: Option<String>,
    pub limit: Option<usize>,
    pub max_candidates: Option<usize>,
    pub model_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SampleCandidate {
    pub ts: String,
    pub candidate_type: String,
    pub direction: String,
    pub label: Option<String>,
    pub current_price: f64,
    pub ema20: Option<f64>,
    pub atr14: Option<f64>,
    pub ema20_position: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub score: Option<f64>,
    pub evidence: Value,
}

#[derive(Debug, Deserialize)]
pub struct BacktestRequest {
    #[serde(default = "default_custom_market")]
    pub market: String,
    pub symbol: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default = "default_model_code")]
    pub model_code: String,
    #[serde(default = "default_strategy_code")]
    pub strategy_code: String,
    #[serde(default = "default_strategy_name")]
    pub strategy_name: String,
    #[serde(default = "default_strategy_version")]
    pub strategy_version: String,
    #[serde(default = "default_max_holding_bars")]
    pub max_holding_bars: usize,
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: f64,
    #[serde(default = "default_true")]
    pub persist: bool,
    #[serde(default)]
    pub klines: Vec<Kline>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestTrade {
    pub ts: String,
    pub direction: String,
    pub signal_type: String,
    pub score: f64,
    pub entry_price: f64,
    pub stop_loss: Option<f64>,
    pub target_price: Option<f64>,
    pub exit_ts: String,
    pub exit_price: f64,
    pub exit_reason: String,
    pub holding_bars: usize,
    pub return_pct: f64,
    pub hit_target: bool,
    pub hit_stop: bool,
    pub signal: String,
    pub reason: String,
    pub evidence: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestSummary {
    pub signal_count: usize,
    pub evaluated_count: usize,
    pub win_rate: f64,
    pub avg_return_pct: f64,
    pub median_return_pct: f64,
    pub hit_target_rate: f64,
    pub hit_stop_rate: f64,
    pub avg_holding_bars: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceCount {
    pub source: String,
    pub rows: i64,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRequest {
    pub market: String,
    pub symbol: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default = "default_model_code")]
    pub model_code: String,
    #[serde(default)]
    pub klines: Vec<Kline>,
    #[serde(default)]
    pub refresh: bool,
    #[serde(default)]
    pub persist_signal: bool,
}

#[derive(Debug, Deserialize)]
pub struct ScanRequest {
    pub items: Vec<ScanItem>,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default = "default_model_code")]
    pub model_code: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub refresh: bool,
    #[serde(default)]
    pub persist_signals: bool,
}

#[derive(Debug, Deserialize)]
pub struct ScanItem {
    pub market: String,
    pub symbol: String,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub ok: bool,
    pub error: String,
}

/// A task discovered from active price alert rules.
pub struct StockKlineTask {
    pub market: String,
    pub symbol: String,
    pub interval: String,
}

// ---------------------------------------------------------------------------
// Evaluate-Rule — batch condition evaluation for price alerts
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct EvaluateRuleRequest {
    pub market: String,
    pub symbol: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub quote: Option<QuoteInput>,
    pub conditions: Vec<RuleCondition>,
    #[serde(default = "default_group_op")]
    pub group_op: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuoteInput {
    #[serde(default)]
    pub current_price: Option<f64>,
    #[serde(default)]
    pub change_pct: Option<f64>,
    #[serde(default)]
    pub turnover: Option<f64>,
    #[serde(default)]
    pub volume: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuleCondition {
    #[serde(rename = "type")]
    pub cond_type: String,
    #[serde(default)]
    pub op: String,
    pub value: Value,
    #[serde(default)]
    pub interval: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConditionResult {
    #[serde(rename = "type")]
    pub cond_type: String,
    pub op: String,
    pub target: Value,
    pub actual: Option<Value>,
    pub matched: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn default_group_op() -> String {
    "and".to_string()
}

// ---------------------------------------------------------------------------
// Serde helpers
// ---------------------------------------------------------------------------

pub fn default_interval() -> String {
    "1d".to_string()
}

pub fn default_limit() -> usize {
    120
}

pub fn default_custom_market() -> String {
    "CUSTOM".to_string()
}

pub fn default_model_code() -> String {
    "pa_breakout_v2".to_string()
}

pub fn default_strategy_code() -> String {
    default_model_code()
}

pub fn default_strategy_name() -> String {
    "PriceDog PA Breakout".to_string()
}

pub fn default_strategy_version() -> String {
    "v1".to_string()
}

pub fn default_max_holding_bars() -> usize {
    48
}

pub fn default_fee_bps() -> f64 {
    10.0
}

pub fn default_slippage_bps() -> f64 {
    5.0
}

pub fn default_true() -> bool {
    true
}
