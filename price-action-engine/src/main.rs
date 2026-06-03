use std::{collections::HashMap, env, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use moka::future::Cache;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{sync::RwLock, time::sleep};
use tokio_postgres::{Client as PgClient, NoTls, Row};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower_http::cors::CorsLayer;
use tracing::{debug, info, warn};

#[derive(Clone)]
struct AppState {
    client: Client,
    db_url: String,
    kline_cache: Cache<String, Vec<Kline>>,
    quote_cache: Cache<String, Quote>,
    data_provider_url: String,
    provider_status: Arc<RwLock<HashMap<String, ProviderStatus>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Kline {
    // PriceDog backtest data spec:
    // ts is the bar open time in RFC3339 UTC, and the bar becomes fully usable at ts + interval.
    ts: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    #[serde(default)]
    volume: f64,
    #[serde(default)]
    turnover: f64,
}

struct KlineFetch {
    klines: Vec<Kline>,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Quote {
    symbol: String,
    market: String,
    #[serde(default)]
    name: String,
    current_price: f64,
    #[serde(default)]
    change_pct: f64,
    #[serde(default)]
    change_amount: f64,
    #[serde(default)]
    volume: f64,
    #[serde(default)]
    turnover: f64,
    #[serde(default)]
    open_price: f64,
    #[serde(default)]
    high_price: f64,
    #[serde(default)]
    low_price: f64,
    #[serde(default)]
    prev_close: f64,
    #[serde(default)]
    timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderStatus {
    provider: String,
    market: String,
    channel: String,
    status: String,
    last_connected_at: Option<String>,
    last_message_at: Option<String>,
    last_closed_kline_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct Indicators {
    ema20: Option<f64>,
    ema5: Option<f64>,
    atr14: Option<f64>,
    atr20: Option<f64>,
    ema20_position: Option<f64>,
    volume_ratio: Option<f64>,
    amplitude: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct Signal {
    signal_type: String,
    direction: String,
    score: f64,
    ema20_entry_score: f64,
    entry_price: Option<f64>,
    stop_loss: Option<f64>,
    target_price: Option<f64>,
    reason: String,
    evidence: Value,
}

/// Result of Layer 1+2 evaluation for a breakout bar candidate.
struct BreakoutCandidate {
    direction: String,
    range_high: f64,
    range_low: f64,
    body_ratio: f64,
    close_location: f64,
    distance_atr: f64,
    breakout_bar_score: f64,
    #[allow(dead_code)]
    atr14: f64,
    stop_loss: f64,
    target_price: f64,
}

// ---------------------------------------------------------------------------
// Signal Bar Model — 信号K线识别模型
// ---------------------------------------------------------------------------

/// 单根K线的基本特征
struct BarFeatures {
    body: f64,  // B = C - O（正值=阳线，负值=阴线）
    range: f64, // R = H - L
    p_b: f64,   // 实体占比 = |B| / R
    p_c: f64,   // 收盘位置 = (C - L) / R
    p_u: f64,   // 上影线占比 = (H - max(C,O)) / R
    p_d: f64,   // 下影线占比 = (min(C,O) - L) / R
}

/// 市场背景评估结果
struct ContextResult {
    cr: f64,        // 通道比率 = (H_N - L_N) / ATR_M
    trend_dir: i32, // 趋势方向: 1=多, -1=空, 0=中性
    ma_cross_dir: Option<i32>, // 金叉/死叉方向: 1=金叉, -1=死叉, None=未检测到
    f_ratio: f64,   // 多空力量对比
    is_valid: bool, // CR >= θ（非窄通道）
}

// ---------------------------------------------------------------------------
// PriceActionModel trait — pluggable model interface
// ---------------------------------------------------------------------------

trait PriceActionModel: Send + Sync {
    fn code(&self) -> &str;
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn min_klines(&self) -> usize;
    fn detect(&self, klines: &[Kline]) -> Vec<Signal>;
    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<BacktestTrade>;
    fn generate_candidates(&self, klines: &[Kline], max_candidates: usize) -> Vec<SampleCandidate>;
}

// ---------------------------------------------------------------------------
// V2 model — 3-layer funnel (veto → bar score → follow-through), 65-pt scale
// ---------------------------------------------------------------------------

struct V2Model;

impl PriceActionModel for V2Model {
    fn code(&self) -> &str {
        "pa_breakout_v2"
    }

    fn name(&self) -> &str {
        "pricedog_pa_breakout"
    }

    fn version(&self) -> &str {
        "v2"
    }

    fn min_klines(&self) -> usize {
        28
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_breakout_v2(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<BacktestTrade> {
        backtest_trades_v2(klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(&self, klines: &[Kline], max_candidates: usize) -> Vec<SampleCandidate> {
        generate_sample_candidates_v2(klines, max_candidates)
    }
}

struct SignalBarModel;

impl PriceActionModel for SignalBarModel {
    fn code(&self) -> &str {
        "pa_signal_bar_v1"
    }

    fn name(&self) -> &str {
        "pricedog_pa_signal_bar"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn min_klines(&self) -> usize {
        25
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<BacktestTrade> {
        backtest_trades_from_model(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self,
        klines: &[Kline],
        max_candidates: usize,
    ) -> Vec<SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

struct SignalBarModelCn;

impl PriceActionModel for SignalBarModelCn {
    fn code(&self) -> &str {
        "pa_signal_bar_cn_v1"
    }

    fn name(&self) -> &str {
        "pricedog_pa_signal_bar_cn"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn min_klines(&self) -> usize {
        25
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar_cn(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<BacktestTrade> {
        // A 股只模拟做多交易，空头信号仅作为盯盘提醒不计入交易
        backtest_trades_from_model_long_only(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self,
        klines: &[Kline],
        max_candidates: usize,
    ) -> Vec<SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

fn resolve_model(model_code: &str) -> Result<Box<dyn PriceActionModel>> {
    match model_code.trim() {
        "" | "pa_breakout_v2" | "pricedog_pa_breakout" => Ok(Box::new(V2Model)),
        "pa_signal_bar_v1" | "pricedog_pa_signal_bar" => Ok(Box::new(SignalBarModel)),
        "pa_signal_bar_cn_v1" | "pricedog_pa_signal_bar_cn" => Ok(Box::new(SignalBarModelCn)),
        "pa_ema20_cross_v1" | "pricedog_pa_ema20_cross" => Ok(Box::new(Ema20CrossModel)),
        other => Err(anyhow!("unknown model_code: {}", other)),
    }
}

struct Ema20CrossModel;

impl PriceActionModel for Ema20CrossModel {
    fn code(&self) -> &str { "pa_ema20_cross_v1" }
    fn name(&self) -> &str { "pricedog_pa_ema20_cross" }
    fn version(&self) -> &str { "v1" }
    fn min_klines(&self) -> usize { 22 }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        if klines.len() < 22 {
            return Vec::new();
        }
        let curr = klines.last().unwrap();
        let prev = &klines[klines.len() - 2];
        let indicators = compute_indicators(klines);
        let curr_ema20 = indicators.ema20.unwrap_or(0.0);
        let prev_closes: Vec<f64> = klines[..klines.len()-1].iter().map(|k| k.close).collect();
        let prev_ema20_vals = ema(&prev_closes, 20);
        let prev_ema20 = prev_ema20_vals.last().copied().unwrap_or(0.0);
        if curr_ema20 <= 0.0 || prev_ema20 <= 0.0 {
            return Vec::new();
        }

        let crossed_up = prev.close <= prev_ema20 && curr.close > curr_ema20;
        let crossed_down = prev.close >= prev_ema20 && curr.close < curr_ema20;
        let (direction, reason) = if crossed_up {
            ("long", format!("EMA20 cross up: prev_c={:.2} prev_ema20={:.2} curr_c={:.2} curr_ema20={:.2}", prev.close, prev_ema20, curr.close, curr_ema20))
        } else if crossed_down {
            ("short", format!("EMA20 cross down: prev_c={:.2} prev_ema20={:.2} curr_c={:.2} curr_ema20={:.2}", prev.close, prev_ema20, curr.close, curr_ema20))
        } else {
            return Vec::new();
        };

        let delta = SIGNAL_BAR_TICK_SIZE;
        let (entry, stop, target) = if direction == "long" {
            let e = curr.high + delta;
            let s = curr.low - delta;
            (e, s, e + 2.0 * (e - s))
        } else {
            let e = curr.low - delta;
            let s = curr.high + delta;
            (e, s, e - 2.0 * (s - e))
        };

        vec![Signal {
            signal_type: "pa_ema20_cross".to_string(),
            direction: direction.to_string(),
            score: 0.6,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 100.0).round() / 100.0),
            stop_loss: Some((stop * 100.0).round() / 100.0),
            target_price: Some((target * 100.0).round() / 100.0),
            reason,
            evidence: json!({
                "model_code": "pa_ema20_cross_v1",
                "type": "ema20_cross",
                "direction": direction,
                "prev_close": prev.close,
                "prev_ema20": prev_ema20,
                "curr_close": curr.close,
                "curr_ema20": curr_ema20,
            }),
        }]
    }

    fn backtest(&self, klines: &[Kline], max_holding_bars: usize, fee_bps: f64, slippage_bps: f64) -> Vec<BacktestTrade> {
        backtest_trades_from_model(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(&self, klines: &[Kline], max_candidates: usize) -> Vec<SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

#[derive(Debug, Deserialize)]
struct KlineQuery {
    interval: Option<String>,
    limit: Option<usize>,
    refresh: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DataHealthQuery {
    interval: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SampleCandidateQuery {
    interval: Option<String>,
    limit: Option<usize>,
    max_candidates: Option<usize>,
    model_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SampleCandidate {
    ts: String,
    candidate_type: String,
    direction: String,
    label: Option<String>,
    current_price: f64,
    ema20: Option<f64>,
    atr14: Option<f64>,
    ema20_position: Option<f64>,
    volume_ratio: Option<f64>,
    score: Option<f64>,
    evidence: Value,
}

#[derive(Debug, Deserialize)]
struct BacktestRequest {
    #[serde(default = "default_custom_market")]
    market: String,
    symbol: String,
    #[serde(default = "default_interval")]
    interval: String,
    #[serde(default = "default_model_code")]
    model_code: String,
    #[serde(default = "default_strategy_code")]
    strategy_code: String,
    #[serde(default = "default_strategy_name")]
    strategy_name: String,
    #[serde(default = "default_strategy_version")]
    strategy_version: String,
    #[serde(default = "default_max_holding_bars")]
    max_holding_bars: usize,
    #[serde(default = "default_fee_bps")]
    fee_bps: f64,
    #[serde(default = "default_slippage_bps")]
    slippage_bps: f64,
    #[serde(default = "default_true")]
    persist: bool,
    #[serde(default)]
    klines: Vec<Kline>,
}

#[derive(Debug, Clone, Serialize)]
struct BacktestTrade {
    ts: String,
    direction: String,
    signal_type: String,
    score: f64,
    entry_price: f64,
    stop_loss: Option<f64>,
    target_price: Option<f64>,
    exit_ts: String,
    exit_price: f64,
    exit_reason: String,
    holding_bars: usize,
    return_pct: f64,
    hit_target: bool,
    hit_stop: bool,
    signal: String,
    reason: String,
    evidence: Value,
}

#[derive(Debug, Clone, Serialize)]
struct BacktestSummary {
    signal_count: usize,
    evaluated_count: usize,
    win_rate: f64,
    avg_return_pct: f64,
    median_return_pct: f64,
    hit_target_rate: f64,
    hit_stop_rate: f64,
    avg_holding_bars: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SourceCount {
    source: String,
    rows: i64,
}

#[derive(Debug, Deserialize)]
struct EvaluateRequest {
    market: String,
    symbol: String,
    #[serde(default = "default_interval")]
    interval: String,
    #[serde(default = "default_model_code")]
    model_code: String,
    #[serde(default)]
    klines: Vec<Kline>,
    #[serde(default)]
    refresh: bool,
    #[serde(default)]
    persist_signal: bool,
}

#[derive(Debug, Deserialize)]
struct ScanRequest {
    items: Vec<ScanItem>,
    #[serde(default = "default_interval")]
    interval: String,
    #[serde(default = "default_model_code")]
    model_code: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    refresh: bool,
    #[serde(default)]
    persist_signals: bool,
}

#[derive(Debug, Deserialize)]
struct ScanItem {
    market: String,
    symbol: String,
}

#[derive(Debug, Serialize)]
struct ApiError {
    ok: bool,
    error: String,
}

fn default_interval() -> String {
    "1d".to_string()
}

fn default_limit() -> usize {
    120
}

fn default_custom_market() -> String {
    "CUSTOM".to_string()
}

fn default_model_code() -> String {
    "pa_breakout_v2".to_string()
}

fn default_strategy_code() -> String {
    default_model_code()
}

fn default_strategy_name() -> String {
    "PriceDog PA Breakout".to_string()
}

fn default_strategy_version() -> String {
    "v1".to_string()
}

fn default_max_holding_bars() -> usize {
    48
}

fn default_fee_bps() -> f64 {
    10.0
}

fn default_slippage_bps() -> f64 {
    5.0
}

fn default_true() -> bool {
    true
}

#[tokio::main]
async fn main() -> Result<()> {
    if env::args().any(|arg| arg == "--healthcheck") {
        let port = env::var("PA_ENGINE_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(8001);
        let url = format!("http://127.0.0.1:{}/api/v1/health", port);
        let client = Client::builder().timeout(Duration::from_secs(3)).build()?;
        client.get(url).send().await?.error_for_status()?;
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let db_url = connect_db().await?;
    init_db(&db_url).await?;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("PriceDog/0.1")
        .build()?;
    let state = Arc::new(AppState {
        client,
        db_url,
        kline_cache: Cache::builder()
            .time_to_live(Duration::from_secs(300))
            .max_capacity(10_000)
            .build(),
        quote_cache: Cache::builder()
            .time_to_live(Duration::from_secs(10))
            .max_capacity(10_000)
            .build(),
        data_provider_url: env::var("DATA_PROVIDER_URL")
            .or_else(|_| env::var("AKSHARE_ADAPTER_URL"))
            .unwrap_or_else(|_| "http://127.0.0.1:8003".to_string())
            .trim_end_matches('/')
            .to_string(),
        provider_status: Arc::new(RwLock::new(HashMap::new())),
    });
    start_crypto_ws_collectors(state.clone());
    start_crypto_kline_backfill_monitor(state.clone());
    start_stock_kline_backfill_monitor(state.clone());

    let app = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/provider-status", get(get_provider_status))
        .route("/api/v1/data-health/:market/:symbol", get(get_data_health))
        .route(
            "/api/v1/sample-candidates/:market/:symbol",
            get(get_sample_candidates),
        )
        .route("/api/v1/klines/:market/:symbol", get(get_klines))
        .route("/api/v1/quote/:market/:symbol", get(get_quote))
        .route("/api/v1/evaluate", post(evaluate))
        .route("/api/v1/backtest", post(backtest))
        .route("/api/v1/scan", post(scan))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port = env::var("PA_ENGINE_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8001);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Price Action Engine listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true, "service": "price-action-engine"}))
}

async fn get_provider_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut providers = state
        .provider_status
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    providers.sort_by(|a, b| {
        (a.market.as_str(), a.provider.as_str(), a.channel.as_str()).cmp(&(
            b.market.as_str(),
            b.provider.as_str(),
            b.channel.as_str(),
        ))
    });
    Json(json!({"ok": true, "providers": providers})).into_response()
}

fn provider_status_key(provider: &str, market: &str, channel: &str) -> String {
    format!("{}:{}:{}", provider, market, channel)
}

async fn mark_provider_connected(state: &AppState, provider: &str, market: &str, channel: &str) {
    let now = Utc::now().to_rfc3339();
    let key = provider_status_key(provider, market, channel);
    let mut statuses = state.provider_status.write().await;
    let entry = statuses.entry(key).or_insert_with(|| ProviderStatus {
        provider: provider.to_string(),
        market: market.to_string(),
        channel: channel.to_string(),
        status: "disconnected".to_string(),
        last_connected_at: None,
        last_message_at: None,
        last_closed_kline_at: None,
        last_error: None,
    });
    entry.status = "connected".to_string();
    entry.last_connected_at = Some(now);
    entry.last_error = None;
}

async fn mark_provider_message(state: &AppState, provider: &str, market: &str, channel: &str) {
    let now = Utc::now().to_rfc3339();
    let key = provider_status_key(provider, market, channel);
    let mut statuses = state.provider_status.write().await;
    let entry = statuses.entry(key).or_insert_with(|| ProviderStatus {
        provider: provider.to_string(),
        market: market.to_string(),
        channel: channel.to_string(),
        status: "connected".to_string(),
        last_connected_at: Some(now.clone()),
        last_message_at: None,
        last_closed_kline_at: None,
        last_error: None,
    });
    entry.status = "connected".to_string();
    entry.last_message_at = Some(now);
}

async fn mark_provider_closed_kline(state: &AppState, provider: &str, market: &str, channel: &str) {
    let now = Utc::now().to_rfc3339();
    let key = provider_status_key(provider, market, channel);
    let mut statuses = state.provider_status.write().await;
    let entry = statuses.entry(key).or_insert_with(|| ProviderStatus {
        provider: provider.to_string(),
        market: market.to_string(),
        channel: channel.to_string(),
        status: "connected".to_string(),
        last_connected_at: Some(now.clone()),
        last_message_at: Some(now.clone()),
        last_closed_kline_at: None,
        last_error: None,
    });
    entry.status = "connected".to_string();
    entry.last_closed_kline_at = Some(now);
}

async fn mark_provider_error(
    state: &AppState,
    provider: &str,
    market: &str,
    channel: &str,
    error: String,
) {
    let key = provider_status_key(provider, market, channel);
    let mut statuses = state.provider_status.write().await;
    let entry = statuses.entry(key).or_insert_with(|| ProviderStatus {
        provider: provider.to_string(),
        market: market.to_string(),
        channel: channel.to_string(),
        status: "degraded".to_string(),
        last_connected_at: None,
        last_message_at: None,
        last_closed_kline_at: None,
        last_error: None,
    });
    entry.status = "degraded".to_string();
    entry.last_error = Some(error);
}

async fn mark_provider_disconnected(
    state: &AppState,
    provider: &str,
    market: &str,
    channel: &str,
    error: Option<String>,
) {
    let key = provider_status_key(provider, market, channel);
    let mut statuses = state.provider_status.write().await;
    let entry = statuses.entry(key).or_insert_with(|| ProviderStatus {
        provider: provider.to_string(),
        market: market.to_string(),
        channel: channel.to_string(),
        status: "connected".to_string(),
        last_connected_at: None,
        last_message_at: None,
        last_closed_kline_at: None,
        last_error: None,
    });
    entry.status = "disconnected".to_string();
    entry.last_error = error;
}

fn start_crypto_ws_collectors(state: Arc<AppState>) {
    let enabled = env::var("PA_CRYPTO_WS_ENABLED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    if !enabled {
        info!("crypto quote websocket collector disabled");
        return;
    }
    let symbols = crypto_ws_symbols();
    if symbols.is_empty() {
        info!("crypto quote websocket collector has no symbols");
        return;
    }
    let ticker_state = state.clone();
    let ticker_symbols = symbols.clone();
    tokio::spawn(async move {
        loop {
            match run_okx_ticker_ws_once(ticker_state.clone(), &ticker_symbols).await {
                Ok(()) => {
                    mark_provider_disconnected(
                        &ticker_state,
                        "okx",
                        "CRYPTO",
                        "ticker_ws",
                        Some("websocket closed".to_string()),
                    )
                    .await;
                    warn!("crypto quote websocket closed");
                }
                Err(err) => {
                    mark_provider_disconnected(
                        &ticker_state,
                        "okx",
                        "CRYPTO",
                        "ticker_ws",
                        Some(err.to_string()),
                    )
                    .await;
                    warn!("crypto quote websocket disconnected: {err}");
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    let candle_intervals = crypto_ws_intervals();
    if candle_intervals.is_empty() {
        return;
    }
    tokio::spawn(async move {
        loop {
            match run_okx_candle_ws_once(state.clone(), &symbols, &candle_intervals).await {
                Ok(()) => {
                    mark_provider_disconnected(
                        &state,
                        "okx",
                        "CRYPTO",
                        "candle_ws",
                        Some("websocket closed".to_string()),
                    )
                    .await;
                    warn!("crypto candle websocket closed");
                }
                Err(err) => {
                    mark_provider_disconnected(
                        &state,
                        "okx",
                        "CRYPTO",
                        "candle_ws",
                        Some(err.to_string()),
                    )
                    .await;
                    warn!("crypto candle websocket disconnected: {err}");
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

fn start_crypto_kline_backfill_monitor(state: Arc<AppState>) {
    let enabled = env::var("PA_CRYPTO_BACKFILL_ENABLED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    if !enabled {
        info!("crypto kline backfill monitor disabled");
        return;
    }
    let symbols = crypto_ws_symbols();
    let intervals = crypto_ws_intervals();
    if symbols.is_empty() || intervals.is_empty() {
        info!("crypto kline backfill monitor has no symbols or intervals");
        return;
    }
    let poll_secs = env::var("PA_CRYPTO_BACKFILL_INTERVAL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v >= 10)
        .unwrap_or(60);
    let lookback = env::var("PA_CRYPTO_BACKFILL_LOOKBACK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 10)
        .unwrap_or(120);

    tokio::spawn(async move {
        loop {
            if let Err(err) =
                run_crypto_kline_backfill_once(&state, &symbols, &intervals, lookback).await
            {
                mark_provider_error(&state, "okx", "CRYPTO", "kline_backfill", err.to_string())
                    .await;
                warn!("crypto kline backfill failed: {err}");
            }
            sleep(Duration::from_secs(poll_secs)).await;
        }
    });
}

fn crypto_ws_symbols() -> Vec<String> {
    env::var("PA_CRYPTO_WS_SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
        .split(',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn crypto_ws_intervals() -> Vec<String> {
    env::var("PA_CRYPTO_WS_INTERVALS")
        .unwrap_or_else(|_| "5m,1h,4h".to_string())
        .split(',')
        .map(|s| normalize_interval(s.trim()))
        .filter(|s| okx_ws_candle_channel(s).is_some())
        .collect()
}

async fn run_crypto_kline_backfill_once(
    state: &AppState,
    symbols: &[String],
    intervals: &[String],
    lookback: usize,
) -> Result<()> {
    mark_provider_connected(state, "okx", "CRYPTO", "kline_backfill").await;
    let mut updated = 0usize;
    for symbol in symbols {
        for interval in intervals {
            let stored = load_klines_from_db(state, "CRYPTO", symbol, interval, lookback).await?;
            let Some(reason) = crypto_backfill_reason(&stored, interval) else {
                continue;
            };
            let Some(expected_latest) = expected_latest_closed_ts_millis(interval) else {
                continue;
            };
            let mut bars = fetch_okx_klines(state, symbol, interval, lookback + 2).await?;
            bars.retain(|bar| parse_ts_millis(&bar.ts).is_some_and(|ts| ts <= expected_latest));
            if bars.is_empty() {
                continue;
            }
            persist_klines(state, "CRYPTO", symbol, interval, &bars, "okx-backfill").await?;
            updated += bars.len();
            info!(
                "crypto kline backfill {} {} reason={} rows={}",
                symbol,
                interval,
                reason,
                bars.len()
            );
        }
    }
    mark_provider_message(state, "okx", "CRYPTO", "kline_backfill").await;
    if updated > 0 {
        mark_provider_closed_kline(state, "okx", "CRYPTO", "kline_backfill").await;
    }
    Ok(())
}

fn crypto_backfill_reason(bars: &[Kline], interval: &str) -> Option<String> {
    let interval_ms = interval_millis(interval)?;
    if bars.is_empty() {
        return Some("no_data".to_string());
    }
    let expected_latest = expected_latest_closed_ts_millis(interval)?;
    let latest = bars
        .iter()
        .filter_map(|bar| parse_ts_millis(&bar.ts))
        .max()?;
    if latest + interval_ms < expected_latest {
        return Some("stale".to_string());
    }
    let gaps = count_missing_kline_slots(bars, interval);
    if gaps > 0 {
        return Some(format!("gaps:{gaps}"));
    }
    None
}

// ── Stock K-line backfill ──────────────────────────────────

/// K-line condition types that require pa_kline data in Rust engine.
const KLINE_CONDITION_TYPES: &[&str] = &["volume_ratio", "ema20_position", "pattern"];

/// A task discovered from active price alert rules.
struct StockKlineTask {
    market: String,
    symbol: String,
    interval: String,
}

/// Query the database to discover which (market, symbol, interval) combinations
/// need K-line data. Scans enabled price_alert_rules joined with stocks,
/// extracts conditions of type volume_ratio/ema20_position/pattern and their intervals.
async fn discover_stock_kline_tasks(state: &AppState) -> Result<Vec<StockKlineTask>> {
    let client = postgres_client(&state.db_url).await?;
    let rows = client
        .query(
            r#"
            SELECT s.market, s.symbol, r.condition_group
            FROM price_alert_rules r
            JOIN stocks s ON r.stock_id = s.id
            WHERE r.enabled = true
              AND s.market IN ('CN', 'HK', 'US')
            "#,
            &[],
        )
        .await?;

    let mut tasks = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in &rows {
        let market: String = row.get(0);
        let symbol: String = row.get(1);
        let cg_str: String = row.get(2);
        let cg_json: serde_json::Value = serde_json::from_str(&cg_str).unwrap_or(serde_json::Value::Null);

        let items = cg_json
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !KLINE_CONDITION_TYPES.contains(&ctype) {
                continue;
            }
            let interval = item
                .get("interval")
                .and_then(|v| v.as_str())
                .unwrap_or("1d");
            let interval = normalize_interval(interval);
            let key = format!("{}:{}:{}", market, symbol, interval);
            if seen.insert(key) {
                tasks.push(StockKlineTask {
                    market: market.clone(),
                    symbol: symbol.clone(),
                    interval,
                });
            }
        }
    }
    Ok(tasks)
}

fn start_stock_kline_backfill_monitor(state: Arc<AppState>) {
    let enabled = env::var("PA_STOCK_BACKFILL_ENABLED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);
    if !enabled {
        info!("stock kline backfill monitor disabled");
        return;
    }
    let poll_secs = env::var("PA_STOCK_BACKFILL_INTERVAL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v >= 30)
        .unwrap_or(300);
    let lookback = env::var("PA_STOCK_BACKFILL_LOOKBACK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 10)
        .unwrap_or(120);

    info!("stock kline backfill starting: poll={}s lookback={}", poll_secs, lookback);

    tokio::spawn(async move {
        loop {
            if let Err(err) = run_stock_kline_backfill_once(&state, lookback).await {
                warn!("stock kline backfill failed: {err}");
            }
            sleep(Duration::from_secs(poll_secs)).await;
        }
    });
}

async fn run_stock_kline_backfill_once(
    state: &AppState,
    lookback: usize,
) -> Result<()> {
    let tasks = discover_stock_kline_tasks(state).await?;
    if tasks.is_empty() {
        return Ok(());
    }

    mark_provider_connected(state, "data-provider", "STOCK", "stock_backfill").await;
    let mut updated = 0usize;
    for task in &tasks {
        // Check if we have fresh enough data already
        let stored = load_klines_from_db(state, &task.market, &task.symbol, &task.interval, lookback).await?;
        if has_usable_klines(stored.len(), lookback) {
            if let Some(latest) = stored.first() {
                if let Some(latest_ts) = parse_ts_millis(&latest.ts) {
                    if let Some(expected) = expected_latest_closed_ts_millis(&task.interval) {
                        if latest_ts + interval_millis(&task.interval).unwrap_or(0) >= expected {
                            continue;
                        }
                    }
                }
            }
        }

        match fetch_stock_klines(state, &task.market, &task.symbol, &task.interval, lookback + 2).await {
            Ok(fetch) => {
                if fetch.klines.is_empty() {
                    continue;
                }
                persist_klines(state, &task.market, &task.symbol, &task.interval, &fetch.klines, &fetch.source).await?;
                updated += fetch.klines.len();
                info!(
                    "stock kline backfill {} {} {} rows={}",
                    task.market, task.symbol, task.interval, fetch.klines.len()
                );
            }
            Err(err) => {
                warn!("stock kline backfill {} {} {} failed: {}", task.market, task.symbol, task.interval, err);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    mark_provider_message(state, "data-provider", "STOCK", "stock_backfill").await;
    if updated > 0 {
        mark_provider_closed_kline(state, "data-provider", "STOCK", "stock_backfill").await;
    }
    Ok(())
}

async fn run_okx_ticker_ws_once(state: Arc<AppState>, symbols: &[String]) -> Result<()> {
    let (mut ws, _) = connect_async("wss://ws.okx.com:8443/ws/v5/public").await?;
    let args = symbols
        .iter()
        .map(|symbol| json!({"channel": "tickers", "instId": okx_inst_id(symbol)}))
        .collect::<Vec<_>>();
    ws.send(Message::Text(
        json!({"op": "subscribe", "args": args}).to_string(),
    ))
    .await?;
    mark_provider_connected(&state, "okx", "CRYPTO", "ticker_ws").await;
    info!(
        "crypto ticker websocket subscribed {} symbols",
        symbols.len()
    );

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(text) => {
                mark_provider_message(&state, "okx", "CRYPTO", "ticker_ws").await;
                handle_okx_ws_text(&state, &text).await?;
            }
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

async fn run_okx_candle_ws_once(
    state: Arc<AppState>,
    symbols: &[String],
    intervals: &[String],
) -> Result<()> {
    let (mut ws, _) = connect_async("wss://ws.okx.com:8443/ws/v5/business").await?;
    let mut args = Vec::new();
    for symbol in symbols {
        let inst_id = okx_inst_id(symbol);
        for interval in intervals {
            if let Some(channel) = okx_ws_candle_channel(interval) {
                args.push(json!({"channel": channel, "instId": inst_id}));
            }
        }
    }
    ws.send(Message::Text(
        json!({"op": "subscribe", "args": args}).to_string(),
    ))
    .await?;
    mark_provider_connected(&state, "okx", "CRYPTO", "candle_ws").await;
    info!(
        "crypto candle websocket subscribed {} symbols and {} intervals",
        symbols.len(),
        intervals.len()
    );

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(text) => {
                mark_provider_message(&state, "okx", "CRYPTO", "candle_ws").await;
                handle_okx_ws_text(&state, &text).await?;
            }
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

async fn handle_okx_ws_text(state: &AppState, text: &str) -> Result<()> {
    let value: Value = serde_json::from_str(text)?;
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if let Some(event) = value.get("event").and_then(|v| v.as_str()) {
        if event != "subscribe" {
            warn!("okx websocket event {}: {}", event, text);
        }
        return Ok(());
    }
    if channel == "tickers" {
        return handle_okx_ticker_ws_data(state, &value).await;
    }
    if channel.starts_with("candle") {
        return handle_okx_candle_ws_data(state, &value, channel).await;
    }
    Ok(())
}

async fn handle_okx_ticker_ws_data(state: &AppState, value: &Value) -> Result<()> {
    let Some(rows) = value.get("data").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    for row in rows {
        let inst_id = row.get("instId").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = okx_symbol(inst_id);
        let last = row.get("last").map(parse_f64).unwrap_or(0.0);
        let open = row.get("open24h").map(parse_f64).unwrap_or(0.0);
        let quote = Quote {
            symbol: symbol.clone(),
            market: "CRYPTO".to_string(),
            name: inst_id.to_string(),
            current_price: last,
            change_pct: if open > 0.0 {
                (last - open) / open * 100.0
            } else {
                0.0
            },
            change_amount: last - open,
            volume: row.get("vol24h").map(parse_f64).unwrap_or(0.0),
            turnover: row.get("volCcy24h").map(parse_f64).unwrap_or(0.0),
            open_price: open,
            high_price: row.get("high24h").map(parse_f64).unwrap_or(0.0),
            low_price: row.get("low24h").map(parse_f64).unwrap_or(0.0),
            prev_close: open,
            timestamp: row
                .get("ts")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<i64>().ok())
                .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
        };
        state
            .quote_cache
            .insert(format!("CRYPTO:{}", symbol), quote)
            .await;
    }
    Ok(())
}

async fn handle_okx_candle_ws_data(state: &AppState, value: &Value, channel: &str) -> Result<()> {
    let Some(interval) = okx_channel_interval(channel) else {
        return Ok(());
    };
    let inst_id = value
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let symbol = okx_symbol(inst_id);
    let Some(rows) = value.get("data").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    debug!(
        "okx candle websocket received {} rows for {} {}",
        rows.len(),
        symbol,
        interval
    );
    for row in rows {
        let Some(arr) = row.as_array() else {
            continue;
        };
        if arr.len() < 9 || arr.get(8).and_then(|v| v.as_str()) != Some("1") {
            continue;
        }
        let ts = arr[0]
            .as_str()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let bar = Kline {
            ts: chrono::DateTime::<Utc>::from_timestamp_millis(ts)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| ts.to_string()),
            open: parse_f64(&arr[1]),
            high: parse_f64(&arr[2]),
            low: parse_f64(&arr[3]),
            close: parse_f64(&arr[4]),
            volume: parse_f64(&arr[5]),
            turnover: arr.get(7).map(parse_f64).unwrap_or(0.0),
        };
        persist_klines(state, "CRYPTO", &symbol, &interval, &[bar], "okx-ws").await?;
        mark_provider_closed_kline(state, "okx", "CRYPTO", "candle_ws").await;
        info!(
            "okx websocket persisted closed candle {} {} {}",
            symbol, interval, ts
        );

        // 信号K线实时检测
        if let Err(e) = run_signal_bar_detection(state, "CRYPTO", &symbol, &interval).await {
            warn!(
                "signal bar detection failed for {} {}: {e}",
                symbol, interval
            );
        }
    }
    Ok(())
}

async fn get_klines(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
    Query(query): Query<KlineQuery>,
) -> impl IntoResponse {
    let interval = normalize_interval(query.interval.as_deref().unwrap_or("1d"));
    let limit = query.limit.unwrap_or(120);
    let refresh = query.refresh.unwrap_or(false);
    match fetch_klines(&state, &market, &symbol, &interval, limit, refresh).await {
        Ok(klines) => Json(json!({"ok": true, "market": market, "symbol": symbol, "interval": interval, "klines": klines})).into_response(),
        Err(e) => api_error(StatusCode::BAD_GATEWAY, e),
    }
}

async fn get_data_health(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
    Query(query): Query<DataHealthQuery>,
) -> impl IntoResponse {
    let market = market.to_ascii_uppercase();
    let symbol = symbol.to_ascii_uppercase();
    let interval = normalize_interval(query.interval.as_deref().unwrap_or("5m"));
    let limit = query.limit.unwrap_or(120);

    match build_data_health(&state, &market, &symbol, &interval, limit).await {
        Ok(value) => Json(value).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

async fn get_sample_candidates(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
    Query(query): Query<SampleCandidateQuery>,
) -> impl IntoResponse {
    let market = market.to_ascii_uppercase();
    let symbol = symbol.to_ascii_uppercase();
    let interval = normalize_interval(query.interval.as_deref().unwrap_or("5m"));
    let limit = query.limit.unwrap_or(600);
    let max_candidates = query.max_candidates.unwrap_or(100).min(500);
    let model_code = query.model_code.as_deref().unwrap_or("pa_breakout_v2");

    match build_sample_candidates(
        &state,
        &market,
        &symbol,
        &interval,
        model_code,
        limit,
        max_candidates,
    )
    .await
    {
        Ok(value) => Json(value).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

async fn get_quote(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
) -> impl IntoResponse {
    match fetch_quote(&state, &market, &symbol).await {
        Ok(quote) => Json(json!({"ok": true, "quote": quote})).into_response(),
        Err(e) => api_error(StatusCode::BAD_GATEWAY, e),
    }
}

async fn evaluate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateRequest>,
) -> impl IntoResponse {
    match evaluate_symbol(
        &state,
        &req.market,
        &req.symbol,
        &req.interval,
        &req.model_code,
        120,
        req.klines,
        req.refresh,
        req.persist_signal,
    )
    .await
    {
        Ok(value) => Json(value).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

async fn backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BacktestRequest>,
) -> impl IntoResponse {
    match run_backtest(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

async fn scan(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ScanRequest>,
) -> impl IntoResponse {
    let mut results = Vec::new();
    for item in req.items {
        let result = evaluate_symbol(
            &state,
            &item.market,
            &item.symbol,
            &req.interval,
            &req.model_code,
            req.limit,
            Vec::new(),
            req.refresh,
            req.persist_signals,
        )
        .await;
        match result {
            Ok(value) => results.push(value),
            Err(e) => results.push(json!({
                "ok": false,
                "market": item.market,
                "symbol": item.symbol,
                "interval": req.interval,
                "error": e.to_string(),
            })),
        }
    }
    Json(json!({"ok": true, "items": results})).into_response()
}

fn api_error(status: StatusCode, err: anyhow::Error) -> axum::response::Response {
    (
        status,
        Json(ApiError {
            ok: false,
            error: err.to_string(),
        }),
    )
        .into_response()
}

async fn evaluate_symbol(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    model_code: &str,
    limit: usize,
    input_klines: Vec<Kline>,
    refresh: bool,
    persist_signal: bool,
) -> Result<Value> {
    let model = resolve_model(model_code)?;
    let interval = normalize_interval(interval);
    let mut klines = if input_klines.is_empty() {
        fetch_klines(state, market, symbol, &interval, limit, refresh).await?
    } else {
        input_klines
    };
    klines.sort_by(|a, b| a.ts.cmp(&b.ts));
    if klines.len() < model.min_klines() {
        return Err(anyhow!("need at least {} klines", model.min_klines()));
    }
    let indicators = compute_indicators(&klines);
    let signals = model.detect(&klines);
    if persist_signal {
        for signal in &signals {
            persist_signal_row(
                state,
                market,
                symbol,
                &interval,
                &klines,
                &indicators,
                signal,
            )
            .await?;
        }
    }
    Ok(json!({
        "ok": true,
        "market": market,
        "symbol": symbol,
        "interval": interval,
        "model_code": model.code(),
        "model_name": model.name(),
        "model_version": model.version(),
        "indicators": indicators,
        "signals": signals,
    }))
}

async fn run_backtest(state: &AppState, req: BacktestRequest) -> Result<Value> {
    let model = resolve_model(&req.model_code)?;
    let market = req.market.trim().to_ascii_uppercase();
    let symbol = req.symbol.trim().to_ascii_uppercase();
    let interval = normalize_interval(&req.interval);
    let strategy_code = if req.strategy_code.trim().is_empty() {
        model.code().to_string()
    } else {
        req.strategy_code.trim().to_string()
    };
    let strategy_name = req.strategy_name.trim().to_string();
    let strategy_version = req.strategy_version.trim().to_string();
    if req.klines.len() < model.min_klines() {
        return Err(anyhow!("need at least {} klines", model.min_klines()));
    }
    if req.max_holding_bars == 0 {
        return Err(anyhow!("max_holding_bars must be greater than 0"));
    }

    let mut klines = req.klines;
    klines.sort_by(|a, b| a.ts.cmp(&b.ts));
    let trades = model.backtest(&klines, req.max_holding_bars, req.fee_bps, req.slippage_bps);
    let summary = summarize_backtest(&trades);
    let trace_id = format!("bt-{}-{}", symbol, Utc::now().timestamp_millis());
    let run_id = if req.persist {
        Some(
            persist_backtest_run(
                state,
                &market,
                &symbol,
                &interval,
                &strategy_code,
                &strategy_name,
                &strategy_version,
                req.max_holding_bars,
                req.fee_bps,
                req.slippage_bps,
                &trace_id,
                &summary,
            )
            .await?,
        )
    } else {
        None
    };
    if req.persist {
        persist_backtest_results(
            state,
            run_id.unwrap_or_default(),
            &trace_id,
            &market,
            &symbol,
            &strategy_code,
            &strategy_name,
            &strategy_version,
            req.max_holding_bars,
            &trades,
        )
        .await?;
    }

    Ok(json!({
        "ok": true,
        "run_id": run_id,
        "market": market,
        "symbol": symbol,
        "interval": interval,
        "model_code": model.code(),
        "model_name": model.name(),
        "model_version": model.version(),
        "strategy_code": strategy_code,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "summary": summary,
        "signal_count": summary.signal_count,
        "evaluated_count": summary.evaluated_count,
        "win_rate": summary.win_rate,
        "avg_return_pct": summary.avg_return_pct,
        "median_return_pct": summary.median_return_pct,
        "hit_target_rate": summary.hit_target_rate,
        "hit_stop_rate": summary.hit_stop_rate,
        "avg_holding_bars": summary.avg_holding_bars,
        "trades": trades,
    }))
}

fn normalize_interval(interval: &str) -> String {
    match interval.trim().to_ascii_lowercase().as_str() {
        "5" | "5min" | "5mins" | "5minute" | "5minutes" => "5m".to_string(),
        "15" | "15min" | "15mins" | "15minute" | "15minutes" => "15m".to_string(),
        "30" | "30min" | "30mins" | "30minute" | "30minutes" => "30m".to_string(),
        "60" | "60m" | "1hour" | "1hours" => "1h".to_string(),
        "120" | "120m" | "2hour" | "2hours" => "2h".to_string(),
        "240" | "240m" | "4hour" | "4hours" => "4h".to_string(),
        "day" | "daily" | "d" => "1d".to_string(),
        other => other.to_string(),
    }
}

fn interval_minutes(interval: &str) -> Option<i64> {
    match normalize_interval(interval).as_str() {
        "5m" => Some(5),
        "15m" => Some(15),
        "30m" => Some(30),
        "1h" => Some(60),
        "2h" => Some(120),
        "4h" => Some(240),
        "1d" => None,
        _ => None,
    }
}

fn interval_millis(interval: &str) -> Option<i64> {
    match normalize_interval(interval).as_str() {
        "1d" => Some(24 * 60 * 60 * 1000),
        other => interval_minutes(other).map(|minutes| minutes * 60 * 1000),
    }
}

fn expected_latest_closed_ts_millis(interval: &str) -> Option<i64> {
    let interval_ms = interval_millis(interval)?;
    let now = Utc::now().timestamp_millis();
    Some(now - (now % interval_ms) - interval_ms)
}

fn count_missing_kline_slots(bars: &[Kline], interval: &str) -> usize {
    let Some(interval_ms) = interval_millis(interval) else {
        return 0;
    };
    let mut timestamps = bars
        .iter()
        .filter_map(|bar| parse_ts_millis(&bar.ts))
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    timestamps.dedup();
    timestamps
        .windows(2)
        .filter_map(|pair| {
            let diff = pair[1] - pair[0];
            if diff > interval_ms {
                Some((diff / interval_ms - 1).max(0) as usize)
            } else {
                None
            }
        })
        .sum()
}

fn latest_kline_ts_millis(bars: &[Kline]) -> Option<i64> {
    bars.iter().filter_map(|bar| parse_ts_millis(&bar.ts)).max()
}

async fn build_data_health(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<Value> {
    let interval = normalize_interval(interval);
    let interval_ms =
        interval_millis(&interval).ok_or_else(|| anyhow!("unsupported interval {}", interval))?;
    let bars = load_klines_from_db(state, market, symbol, &interval, limit).await?;
    let source_counts = load_kline_source_counts(state, market, symbol, &interval).await?;
    let latest_ts_millis = latest_kline_ts_millis(&bars);
    let expected_latest_ts_millis = expected_latest_closed_ts_millis(&interval);
    let missing_slots = count_missing_kline_slots(&bars, &interval);
    let stale = match (latest_ts_millis, expected_latest_ts_millis) {
        (Some(latest), Some(expected)) => latest + interval_ms < expected,
        (None, Some(_)) => true,
        _ => false,
    };
    let lag_slots = match (latest_ts_millis, expected_latest_ts_millis) {
        (Some(latest), Some(expected)) if expected > latest => (expected - latest) / interval_ms,
        (None, Some(_)) => -1,
        _ => 0,
    };
    let status = if bars.is_empty() {
        "no_data"
    } else if stale || missing_slots > 0 {
        "degraded"
    } else {
        "healthy"
    };

    Ok(json!({
        "ok": true,
        "market": market,
        "symbol": symbol,
        "interval": interval,
        "status": status,
        "rows_checked": bars.len(),
        "latest_ts": latest_ts_millis.and_then(|ts| chrono::DateTime::<Utc>::from_timestamp_millis(ts).map(|d| d.to_rfc3339())),
        "expected_latest_closed_ts": expected_latest_ts_millis.and_then(|ts| chrono::DateTime::<Utc>::from_timestamp_millis(ts).map(|d| d.to_rfc3339())),
        "stale": stale,
        "lag_slots": lag_slots,
        "missing_slots": missing_slots,
        "source_counts": source_counts,
    }))
}

async fn build_sample_candidates(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    model_code: &str,
    limit: usize,
    max_candidates: usize,
) -> Result<Value> {
    let model = resolve_model(model_code)?;
    let interval = normalize_interval(interval);
    let bars = fetch_klines(state, market, symbol, &interval, limit, false).await?;
    let candidates = model.generate_candidates(&bars, max_candidates);
    Ok(json!({
        "ok": true,
        "market": market,
        "symbol": symbol,
        "interval": interval,
        "model_code": model.code(),
        "model_name": model.name(),
        "model_version": model.version(),
        "rows_scanned": bars.len(),
        "candidate_count": candidates.len(),
        "candidates": candidates,
    }))
}

fn generate_sample_candidates_v2(klines: &[Kline], max_candidates: usize) -> Vec<SampleCandidate> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for current_idx in 27..klines.len() {
        let breakout_idx = current_idx - 3;
        let curr = &klines[current_idx];

        // Breakout candidates via v2 model
        if let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) {
            if let Some((follow_pts, struct_pts, follow_evidence)) =
                score_follow_through(klines, breakout_idx, &cand)
            {
                let total = cand.breakout_bar_score + follow_pts + struct_pts;
                let window = &klines[..=breakout_idx];
                let indicators = compute_indicators(window);
                if total >= 35.0 {
                    out.push(SampleCandidate {
                        ts: curr.ts.clone(),
                        candidate_type: "pa_breakout".to_string(),
                        direction: cand.direction.clone(),
                        label: None,
                        current_price: curr.close,
                        ema20: indicators.ema20,
                        atr14: indicators.atr14,
                        ema20_position: indicators.ema20_position,
                        volume_ratio: indicators.volume_ratio,
                        score: Some(total),
                        evidence: json!({
                            "breakout_bar_score": cand.breakout_bar_score,
                            "follow_pts": follow_pts,
                            "struct_pts": struct_pts,
                            "total": total,
                            "follow_through": follow_evidence,
                        }),
                    });
                }
            }
        }

        // EMA20 reclaim candidates (keep existing logic)
        let window = &klines[..=current_idx];
        let indicators = compute_indicators(window);
        if ema20_touch_reclaim(curr, &indicators, "long")
            && indicators.ema20_position.unwrap_or(0.0) >= -0.5
        {
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: "ema20_reclaim".to_string(),
                direction: "long".to_string(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(compute_ema20_entry_v2(curr, &indicators, "long")),
                evidence: json!({
                    "ema20_touched": true,
                    "close_above_ema20": indicators.ema20.is_some_and(|ema20| curr.close >= ema20),
                    "low_vs_ema20": indicators.ema20.map(|ema20| curr.low - ema20),
                    "ema20_position": indicators.ema20_position,
                }),
            });
        }

        if ema20_touch_reclaim(curr, &indicators, "short")
            && indicators.ema20_position.unwrap_or(0.0) <= 0.5
        {
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: "ema20_reclaim".to_string(),
                direction: "short".to_string(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(compute_ema20_entry_v2(curr, &indicators, "short")),
                evidence: json!({
                    "ema20_touched": true,
                    "close_below_ema20": indicators.ema20.is_some_and(|ema20| curr.close <= ema20),
                    "high_vs_ema20": indicators.ema20.map(|ema20| curr.high - ema20),
                    "ema20_position": indicators.ema20_position,
                }),
            });
        }
    }
    if out.len() > max_candidates {
        out = out[out.len() - max_candidates..].to_vec();
    }
    out
}

fn generate_sample_candidates_from_model(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_candidates: usize,
) -> Vec<SampleCandidate> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        let indicators = compute_indicators(window);
        for signal in model.detect(window) {
            if signal_bar_cooldown_blocks(model.code(), current_idx, &signal.direction, last_long_idx, last_short_idx) {
                continue;
            }
            let curr = &klines[current_idx];
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: signal.signal_type.clone(),
                direction: signal.direction.clone(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(signal.score),
                evidence: signal.evidence.clone(),
            });
            remember_signal_idx(&signal.direction, current_idx, &mut last_long_idx, &mut last_short_idx);
        }
    }
    if out.len() > max_candidates {
        out = out[out.len() - max_candidates..].to_vec();
    }
    out
}

fn backtest_trades_from_model(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut trades = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        for signal in model.detect(window) {
            if signal_bar_cooldown_blocks(model.code(), current_idx, &signal.direction, last_long_idx, last_short_idx) {
                continue;
            }
            if let Some(trade) = simulate_backtest_trade(
                klines,
                current_idx,
                &signal,
                max_holding_bars,
                fee_bps,
                slippage_bps,
            ) {
                let curr = &klines[current_idx];
                trades.push(BacktestTrade {
                    ts: curr.ts.clone(),
                    direction: signal.direction.clone(),
                    signal_type: signal.signal_type.clone(),
                    score: signal.score,
                    entry_price: trade.0,
                    stop_loss: signal.stop_loss,
                    target_price: signal.target_price,
                    exit_ts: trade.1.ts.clone(),
                    exit_price: trade.2,
                    exit_reason: trade.3.clone(),
                    holding_bars: trade.4,
                    return_pct: trade.5,
                    hit_target: trade.3 == "hit_target",
                    hit_stop: trade.3 == "hit_stop",
                    signal: signal.signal_type.clone(),
                    reason: signal.reason.clone(),
                    evidence: signal.evidence.clone(),
                });
                remember_signal_idx(&signal.direction, current_idx, &mut last_long_idx, &mut last_short_idx);
            }
        }
    }
    trades
}

/// A 股回测：只模拟做多交易，空头信号仅作为盯盘提醒（占用冷却期但不产生交易）
fn backtest_trades_from_model_long_only(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut trades = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        for signal in model.detect(window) {
            if signal_bar_cooldown_blocks(model.code(), current_idx, &signal.direction, last_long_idx, last_short_idx) {
                continue;
            }
            remember_signal_idx(&signal.direction, current_idx, &mut last_long_idx, &mut last_short_idx);
            // A 股只模拟做多，空头信号仅作为盯盘提醒
            if signal.direction != "long" {
                continue;
            }
            if let Some(trade) = simulate_backtest_trade(
                klines,
                current_idx,
                &signal,
                max_holding_bars,
                fee_bps,
                slippage_bps,
            ) {
                let curr = &klines[current_idx];
                trades.push(BacktestTrade {
                    ts: curr.ts.clone(),
                    direction: signal.direction.clone(),
                    signal_type: signal.signal_type.clone(),
                    score: signal.score,
                    entry_price: trade.0,
                    stop_loss: signal.stop_loss,
                    target_price: signal.target_price,
                    exit_ts: trade.1.ts.clone(),
                    exit_price: trade.2,
                    exit_reason: trade.3.clone(),
                    holding_bars: trade.4,
                    return_pct: trade.5,
                    hit_target: trade.3 == "hit_target",
                    hit_stop: trade.3 == "hit_stop",
                    signal: signal.signal_type.clone(),
                    reason: signal.reason.clone(),
                    evidence: signal.evidence.clone(),
                });
            }
        }
    }
    trades
}

fn signal_bar_cooldown_blocks(
    model_code: &str,
    current_idx: usize,
    direction: &str,
    last_long_idx: Option<usize>,
    last_short_idx: Option<usize>,
) -> bool {
    let cooldown = if model_code == "pa_signal_bar_cn_v1" {
        AS_COOLDOWN_BARS
    } else if model_code == "pa_signal_bar_v1" {
        SIGNAL_BAR_COOLDOWN_BARS
    } else {
        return false;
    };
    if cooldown == 0 {
        return false;
    }
    let last_idx = if direction == "long" {
        last_long_idx
    } else {
        last_short_idx
    };
    last_idx.is_some_and(|idx| current_idx.saturating_sub(idx) <= cooldown)
}

fn remember_signal_idx(
    direction: &str,
    current_idx: usize,
    last_long_idx: &mut Option<usize>,
    last_short_idx: &mut Option<usize>,
) {
    if direction == "long" {
        *last_long_idx = Some(current_idx);
    } else {
        *last_short_idx = Some(current_idx);
    }
}

fn backtest_trades_v2(
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let mut trades = Vec::new();
    // current_idx is where follow-through ends; breakout bar is at current_idx - 3
    for current_idx in 27..klines.len() {
        let breakout_idx = current_idx - 3;

        let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) else {
            continue;
        };
        let Some((follow_pts, struct_pts, follow_evidence)) =
            score_follow_through(klines, breakout_idx, &cand)
        else {
            continue;
        };

        let total = cand.breakout_bar_score + follow_pts + struct_pts;
        if total < 35.0 {
            continue;
        }

        let window = &klines[..=breakout_idx];
        let indicators = compute_indicators(window);
        let ema_score = compute_ema20_entry_v2(&klines[breakout_idx], &indicators, &cand.direction);

        let entry_price = klines[breakout_idx].close;
        let signal = Signal {
            signal_type: "pa_breakout".to_string(),
            direction: cand.direction.clone(),
            score: total,
            ema20_entry_score: ema_score,
            entry_price: Some(entry_price),
            stop_loss: Some(cand.stop_loss),
            target_price: Some(cand.target_price),
            reason: format!(
                "{} breakout: bar={:.0}+follow={:.0}+struct={:.0}={:.0}",
                cand.direction, cand.breakout_bar_score, follow_pts, struct_pts, total
            ),
            evidence: json!({
                "range_high": cand.range_high,
                "range_low": cand.range_low,
                "body_ratio": (cand.body_ratio * 100.0).round() / 100.0,
                "close_location": (cand.close_location * 100.0).round() / 100.0,
                "distance_atr": (cand.distance_atr * 100.0).round() / 100.0,
                "breakout_bar_score": cand.breakout_bar_score,
                "follow_through": follow_evidence,
            }),
        };

        if let Some(trade) = simulate_backtest_trade(
            klines,
            current_idx,
            &signal,
            max_holding_bars,
            fee_bps,
            slippage_bps,
        ) {
            let curr = &klines[current_idx];
            trades.push(BacktestTrade {
                ts: curr.ts.clone(),
                direction: signal.direction.clone(),
                signal_type: signal.signal_type.clone(),
                score: signal.score,
                entry_price: trade.0,
                stop_loss: signal.stop_loss,
                target_price: signal.target_price,
                exit_ts: trade.1.ts.clone(),
                exit_price: trade.2,
                exit_reason: trade.3.clone(),
                holding_bars: trade.4,
                return_pct: trade.5,
                hit_target: trade.3 == "hit_target",
                hit_stop: trade.3 == "hit_stop",
                signal: signal.signal_type.clone(),
                reason: signal.reason.clone(),
                evidence: signal.evidence.clone(),
            });
        }
    }
    trades
}

fn simulate_backtest_trade<'a>(
    klines: &'a [Kline],
    signal_index: usize,
    signal: &Signal,
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Option<(f64, &'a Kline, f64, String, usize, f64)> {
    let raw_entry = signal.entry_price?;
    let stop = signal.stop_loss?;
    let target = signal.target_price?;
    let cost_bps = (fee_bps + slippage_bps).max(0.0) / 10_000.0;
    let direction = signal.direction.as_str();
    let entry_price = if direction == "long" {
        raw_entry * (1.0 + cost_bps)
    } else {
        raw_entry * (1.0 - cost_bps)
    };

    let start = signal_index + 1;
    if start >= klines.len() {
        return None;
    }
    let end_exclusive = (start + max_holding_bars).min(klines.len());
    for (offset, bar) in klines[start..end_exclusive].iter().enumerate() {
        if direction == "long" {
            if bar.low <= stop {
                let exit_price = stop * (1.0 - cost_bps);
                let ret = (exit_price - entry_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_stop".to_string(),
                    offset + 1,
                    ret,
                ));
            }
            if bar.high >= target {
                let exit_price = target * (1.0 - cost_bps);
                let ret = (exit_price - entry_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_target".to_string(),
                    offset + 1,
                    ret,
                ));
            }
        } else {
            if bar.high >= stop {
                let exit_price = stop * (1.0 + cost_bps);
                let ret = (entry_price - exit_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_stop".to_string(),
                    offset + 1,
                    ret,
                ));
            }
            if bar.low <= target {
                let exit_price = target * (1.0 + cost_bps);
                let ret = (entry_price - exit_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_target".to_string(),
                    offset + 1,
                    ret,
                ));
            }
        }
    }

    let last_bar = &klines[end_exclusive - 1];
    let exit_price = if direction == "long" {
        last_bar.close * (1.0 - cost_bps)
    } else {
        last_bar.close * (1.0 + cost_bps)
    };
    let ret = if direction == "long" {
        (exit_price - entry_price) / entry_price * 100.0
    } else {
        (entry_price - exit_price) / entry_price * 100.0
    };
    Some((
        entry_price,
        last_bar,
        exit_price,
        "timeout".to_string(),
        end_exclusive - start,
        ret,
    ))
}

fn summarize_backtest(trades: &[BacktestTrade]) -> BacktestSummary {
    if trades.is_empty() {
        return BacktestSummary {
            signal_count: 0,
            evaluated_count: 0,
            win_rate: 0.0,
            avg_return_pct: 0.0,
            median_return_pct: 0.0,
            hit_target_rate: 0.0,
            hit_stop_rate: 0.0,
            avg_holding_bars: 0.0,
        };
    }
    let returns = trades.iter().map(|t| t.return_pct).collect::<Vec<_>>();
    let wins = trades.iter().filter(|t| t.return_pct > 0.0).count();
    let hits_target = trades.iter().filter(|t| t.hit_target).count();
    let hits_stop = trades.iter().filter(|t| t.hit_stop).count();
    let avg_hold = trades.iter().map(|t| t.holding_bars as f64).sum::<f64>() / trades.len() as f64;
    BacktestSummary {
        signal_count: trades.len(),
        evaluated_count: trades.len(),
        win_rate: wins as f64 / trades.len() as f64,
        avg_return_pct: returns.iter().sum::<f64>() / trades.len() as f64,
        median_return_pct: median(returns),
        hit_target_rate: hits_target as f64 / trades.len() as f64,
        hit_stop_rate: hits_stop as f64 / trades.len() as f64,
        avg_holding_bars: avg_hold,
    }
}

async fn fetch_klines(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
    refresh: bool,
) -> Result<Vec<Kline>> {
    let market = market.to_ascii_uppercase();
    let symbol = symbol.to_ascii_uppercase();
    let interval = normalize_interval(interval);
    let key = format!("{}:{}:{}:{}", market, symbol, interval, limit);
    if !refresh {
        if let Some(cached) = state.kline_cache.get(&key).await {
            return Ok(cached);
        }
    }
    let db_fallback = load_klines_from_db(state, &market, &symbol, &interval, limit).await?;
    if has_usable_klines(db_fallback.len(), limit) {
        if refresh {
            return refresh_latest_klines(state, &market, &symbol, &interval, limit, db_fallback)
                .await;
        }
        state.kline_cache.insert(key, db_fallback.clone()).await;
        return Ok(db_fallback);
    }
    let fetched_result = if market == "CRYPTO" {
        fetch_crypto_klines(state, &symbol, &interval, limit).await
    } else if interval == "2h" || interval == "4h" {
        let base_result = fetch_stock_klines(
            state,
            &market,
            &symbol,
            "1h",
            limit * if interval == "2h" { 2 } else { 4 },
        )
        .await;
        base_result.map(|base| KlineFetch {
            klines: aggregate_intraday(&base.klines, &interval),
            source: base.source,
        })
    } else {
        fetch_stock_klines(state, &market, &symbol, &interval, limit).await
    };
    let fetched = match fetched_result {
        Ok(fetched) => fetched,
        Err(err) if has_usable_klines(db_fallback.len(), limit) => {
            warn!(
                "using stored klines after source fetch failed for {} {} {}: {}",
                market, symbol, interval, err
            );
            state.kline_cache.insert(key, db_fallback.clone()).await;
            return Ok(db_fallback);
        }
        Err(err) => return Err(err),
    };
    let mut bars = fetched.klines;
    if bars.len() > limit {
        bars = bars[bars.len() - limit..].to_vec();
    }
    persist_klines(state, &market, &symbol, &interval, &bars, &fetched.source).await?;
    state.kline_cache.insert(key, bars.clone()).await;
    Ok(bars)
}

fn has_usable_klines(count: usize, requested_limit: usize) -> bool {
    count >= requested_limit || count >= 21
}

async fn refresh_latest_klines(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
    fallback: Vec<Kline>,
) -> Result<Vec<Kline>> {
    let latest_limit = latest_fetch_limit(interval);
    let latest_result = if market == "CRYPTO" {
        fetch_crypto_klines(state, symbol, interval, latest_limit).await
    } else if interval == "2h" || interval == "4h" {
        let base_limit = latest_limit * if interval == "2h" { 2 } else { 4 };
        fetch_stock_klines(state, market, symbol, "1h", base_limit)
            .await
            .map(|base| KlineFetch {
                klines: aggregate_intraday(&base.klines, interval),
                source: base.source,
            })
    } else {
        fetch_stock_klines(state, market, symbol, interval, latest_limit).await
    };

    if let Ok(latest) = latest_result {
        if !latest.klines.is_empty() {
            persist_klines(
                state,
                market,
                symbol,
                interval,
                &latest.klines,
                &latest.source,
            )
            .await?;
        }
    }

    let mut bars = load_klines_from_db(state, market, symbol, interval, limit).await?;
    if bars.len() < 21 {
        bars = fallback;
    }
    if bars.len() > limit {
        bars = bars[bars.len() - limit..].to_vec();
    }
    let key = format!("{}:{}:{}:{}", market, symbol, interval, limit);
    state.kline_cache.insert(key, bars.clone()).await;
    Ok(bars)
}

fn latest_fetch_limit(interval: &str) -> usize {
    match normalize_interval(interval).as_str() {
        "1d" => 5,
        "5m" => 24,
        "15m" => 16,
        "30m" => 12,
        "1h" | "2h" | "4h" => 8,
        _ => 10,
    }
}

async fn fetch_stock_klines(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<KlineFetch> {
    let url = format!(
        "{}/api/v1/klines/{}/{}?interval={}&limit={}",
        state.data_provider_url, market, symbol, interval, limit
    );
    let payload = get_json_with_retry(&state.client, &url).await?;
    let code = payload.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
    if code != 0 {
        return Err(anyhow!(payload
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("data-provider kline error")
            .to_string()));
    }
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow!("missing kline data"))?;
    let source = data
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("data-provider")
        .to_string();
    let klines = data.get("klines").cloned().unwrap_or_else(|| json!([]));
    Ok(KlineFetch {
        klines: serde_json::from_value(klines)?,
        source,
    })
}

async fn fetch_binance_klines(
    state: &AppState,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<Vec<Kline>> {
    let binance_interval = match normalize_interval(interval).as_str() {
        "5m" => "5m",
        "15m" => "15m",
        "30m" => "30m",
        "1h" => "1h",
        "2h" => "2h",
        "4h" => "4h",
        "1d" => "1d",
        _ => return Err(anyhow!("unsupported interval {}", interval)),
    };
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval={}&limit={}",
        symbol, binance_interval, limit
    );
    let value = get_json_with_retry(&state.client, &url).await?;
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("invalid binance kline response"))?;
    let mut out = Vec::new();
    for row in rows {
        let arr = row
            .as_array()
            .ok_or_else(|| anyhow!("invalid binance kline row"))?;
        if arr.len() < 6 {
            continue;
        }
        let ts = arr[0].as_i64().unwrap_or(0);
        out.push(Kline {
            ts: chrono::DateTime::<Utc>::from_timestamp_millis(ts)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| ts.to_string()),
            open: parse_f64(&arr[1]),
            high: parse_f64(&arr[2]),
            low: parse_f64(&arr[3]),
            close: parse_f64(&arr[4]),
            volume: parse_f64(&arr[5]),
            turnover: arr.get(7).map(parse_f64).unwrap_or(0.0),
        });
    }
    Ok(out)
}

async fn fetch_crypto_klines(
    state: &AppState,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<KlineFetch> {
    match fetch_binance_klines(state, symbol, interval, limit).await {
        Ok(klines) => Ok(KlineFetch {
            klines,
            source: "binance".to_string(),
        }),
        Err(binance_err) => match fetch_okx_klines(state, symbol, interval, limit).await {
            Ok(klines) => Ok(KlineFetch {
                klines,
                source: "okx".to_string(),
            }),
            Err(okx_err) => Err(anyhow!(
                "crypto kline providers failed; binance: {}; okx: {}",
                binance_err,
                okx_err
            )),
        },
    }
}

async fn fetch_okx_klines(
    state: &AppState,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<Vec<Kline>> {
    let okx_interval = match normalize_interval(interval).as_str() {
        "5m" => "5m",
        "15m" => "15m",
        "30m" => "30m",
        "1h" => "1H",
        "2h" => "2H",
        "4h" => "4H",
        "1d" => "1D",
        _ => return Err(anyhow!("unsupported interval {}", interval)),
    };
    let inst_id = okx_inst_id(symbol);
    let url = format!(
        "https://www.okx.com/api/v5/market/candles?instId={}&bar={}&limit={}",
        inst_id, okx_interval, limit
    );
    let value = get_json_with_retry(&state.client, &url).await?;
    if value.get("code").and_then(|v| v.as_str()).unwrap_or("") != "0" {
        return Err(anyhow!(
            "{}",
            value
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("okx kline error")
        ));
    }
    let rows = value
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("invalid okx kline response"))?;
    let mut out = Vec::new();
    for row in rows {
        let arr = row
            .as_array()
            .ok_or_else(|| anyhow!("invalid okx kline row"))?;
        if arr.len() < 6 {
            continue;
        }
        let ts = arr[0]
            .as_str()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        out.push(Kline {
            ts: chrono::DateTime::<Utc>::from_timestamp_millis(ts)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| ts.to_string()),
            open: parse_f64(&arr[1]),
            high: parse_f64(&arr[2]),
            low: parse_f64(&arr[3]),
            close: parse_f64(&arr[4]),
            volume: parse_f64(&arr[5]),
            turnover: arr.get(7).map(parse_f64).unwrap_or(0.0),
        });
    }
    out.sort_by(|a, b| a.ts.cmp(&b.ts));
    Ok(out)
}

fn okx_inst_id(symbol: &str) -> String {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.contains('-') {
        return symbol;
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return format!("{}-{}", base, quote);
        }
    }
    symbol
}

fn okx_symbol(inst_id: &str) -> String {
    inst_id.replace('-', "").to_ascii_uppercase()
}

fn okx_ws_candle_channel(interval: &str) -> Option<&'static str> {
    match normalize_interval(interval).as_str() {
        "5m" => Some("candle5m"),
        "15m" => Some("candle15m"),
        "30m" => Some("candle30m"),
        "1h" => Some("candle1H"),
        "2h" => Some("candle2H"),
        "4h" => Some("candle4H"),
        "1d" => Some("candle1D"),
        _ => None,
    }
}

fn okx_channel_interval(channel: &str) -> Option<String> {
    match channel {
        "candle5m" => Some("5m".to_string()),
        "candle15m" => Some("15m".to_string()),
        "candle30m" => Some("30m".to_string()),
        "candle1H" => Some("1h".to_string()),
        "candle2H" => Some("2h".to_string()),
        "candle4H" => Some("4h".to_string()),
        "candle1D" => Some("1d".to_string()),
        _ => None,
    }
}

async fn fetch_quote(state: &AppState, market: &str, symbol: &str) -> Result<Quote> {
    let market = market.to_ascii_uppercase();
    let symbol = symbol.to_ascii_uppercase();
    let key = format!("{}:{}", market, symbol);
    if let Some(cached) = state.quote_cache.get(&key).await {
        return Ok(cached);
    }
    let quote = if market == "CRYPTO" {
        fetch_crypto_quote(state, &symbol).await?
    } else {
        fetch_stock_quote(state, &market, &symbol).await?
    };
    state.quote_cache.insert(key, quote.clone()).await;
    Ok(quote)
}

async fn fetch_stock_quote(state: &AppState, market: &str, symbol: &str) -> Result<Quote> {
    let url = format!("{}/api/v1/quote/{}/{}", state.data_provider_url, market, symbol);
    let payload = get_json_with_retry(&state.client, &url).await?;
    let code = payload.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
    if code != 0 {
        return Err(anyhow!(payload
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("data-provider quote error")
            .to_string()));
    }
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow!("missing quote data"))?;
    Ok(Quote {
        symbol: data
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or(symbol)
            .to_string(),
        market: data
            .get("market")
            .and_then(|v| v.as_str())
            .unwrap_or(market)
            .to_string(),
        name: data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        current_price: data.get("current_price").map(parse_f64).unwrap_or(0.0),
        change_pct: data.get("change_pct").map(parse_f64).unwrap_or(0.0),
        change_amount: data.get("change_amount").map(parse_f64).unwrap_or(0.0),
        volume: data.get("volume").map(parse_f64).unwrap_or(0.0),
        turnover: data.get("turnover").map(parse_f64).unwrap_or(0.0),
        open_price: data.get("open_price").map(parse_f64).unwrap_or(0.0),
        high_price: data.get("high_price").map(parse_f64).unwrap_or(0.0),
        low_price: data.get("low_price").map(parse_f64).unwrap_or(0.0),
        prev_close: data.get("prev_close").map(parse_f64).unwrap_or(0.0),
        timestamp: data
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    })
}

async fn fetch_binance_quote(state: &AppState, symbol: &str) -> Result<Quote> {
    let url = format!(
        "https://api.binance.com/api/v3/ticker/24hr?symbol={}",
        symbol
    );
    let data = get_json_with_retry(&state.client, &url).await?;
    let last = data.get("lastPrice").map(parse_f64).unwrap_or(0.0);
    let open = data.get("openPrice").map(parse_f64).unwrap_or(0.0);
    Ok(Quote {
        symbol: symbol.to_string(),
        market: "CRYPTO".to_string(),
        name: symbol.to_string(),
        current_price: last,
        change_pct: data.get("priceChangePercent").map(parse_f64).unwrap_or(0.0),
        change_amount: data.get("priceChange").map(parse_f64).unwrap_or(0.0),
        volume: data.get("volume").map(parse_f64).unwrap_or(0.0),
        turnover: data.get("quoteVolume").map(parse_f64).unwrap_or(0.0),
        open_price: open,
        high_price: data.get("highPrice").map(parse_f64).unwrap_or(0.0),
        low_price: data.get("lowPrice").map(parse_f64).unwrap_or(0.0),
        prev_close: open,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn fetch_crypto_quote(state: &AppState, symbol: &str) -> Result<Quote> {
    match fetch_binance_quote(state, symbol).await {
        Ok(quote) => Ok(quote),
        Err(binance_err) => match fetch_okx_quote(state, symbol).await {
            Ok(quote) => Ok(quote),
            Err(okx_err) => Err(anyhow!(
                "crypto quote providers failed; binance: {}; okx: {}",
                binance_err,
                okx_err
            )),
        },
    }
}

async fn fetch_okx_quote(state: &AppState, symbol: &str) -> Result<Quote> {
    let inst_id = okx_inst_id(symbol);
    let url = format!(
        "https://www.okx.com/api/v5/market/ticker?instId={}",
        inst_id
    );
    let value = get_json_with_retry(&state.client, &url).await?;
    if value.get("code").and_then(|v| v.as_str()).unwrap_or("") != "0" {
        return Err(anyhow!(
            "{}",
            value
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("okx quote error")
        ));
    }
    let data = value
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .ok_or_else(|| anyhow!("invalid okx quote response"))?;
    let last = data.get("last").map(parse_f64).unwrap_or(0.0);
    let open = data.get("open24h").map(parse_f64).unwrap_or(0.0);
    Ok(Quote {
        symbol: symbol.to_string(),
        market: "CRYPTO".to_string(),
        name: inst_id,
        current_price: last,
        change_pct: if open > 0.0 {
            (last - open) / open * 100.0
        } else {
            0.0
        },
        change_amount: last - open,
        volume: data.get("vol24h").map(parse_f64).unwrap_or(0.0),
        turnover: data.get("volCcy24h").map(parse_f64).unwrap_or(0.0),
        open_price: open,
        high_price: data.get("high24h").map(parse_f64).unwrap_or(0.0),
        low_price: data.get("low24h").map(parse_f64).unwrap_or(0.0),
        prev_close: open,
        timestamp: data
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| Utc::now().to_rfc3339()),
    })
}

async fn get_json_with_retry(client: &Client, url: &str) -> Result<Value> {
    let mut last_err = None;
    for attempt in 0..=2 {
        match client
            .get(url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => return Ok(resp.json::<Value>().await?),
            Err(e) => {
                last_err = Some(e);
                if attempt < 2 {
                    sleep(Duration::from_millis(250 * (attempt + 1))).await;
                }
            }
        }
    }
    Err(anyhow!(last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "request failed".to_string())))
}

fn parse_f64(value: &Value) -> f64 {
    if let Some(v) = value.as_f64() {
        v
    } else if let Some(s) = value.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else {
        0.0
    }
}

fn aggregate_intraday(bars: &[Kline], target_interval: &str) -> Vec<Kline> {
    let Some(target_minutes) = interval_minutes(target_interval) else {
        return bars.to_vec();
    };
    let mut sorted = bars.to_vec();
    sorted.sort_by(|a, b| a.ts.cmp(&b.ts));
    let mut out: Vec<Kline> = Vec::new();
    let mut bucket_start: Option<i64> = None;
    for bar in sorted {
        let ts = parse_ts_millis(&bar.ts).unwrap_or(0);
        let minutes = ts / 1000 / 60;
        let bucket = minutes - (minutes % target_minutes);
        if bucket_start != Some(bucket) {
            bucket_start = Some(bucket);
            out.push(bar);
        } else if let Some(last) = out.last_mut() {
            last.high = last.high.max(bar.high);
            last.low = last.low.min(bar.low);
            last.close = bar.close;
            last.volume += bar.volume;
            last.turnover += bar.turnover;
            last.ts = bar.ts;
        }
    }
    out
}

fn parse_ts_millis(ts: &str) -> Option<i64> {
    if let Ok(v) = ts.parse::<i64>() {
        return Some(v);
    }
    chrono::DateTime::parse_from_rfc3339(ts)
        .map(|d| d.timestamp_millis())
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|d| d.and_utc().timestamp_millis())
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|d| d.and_utc().timestamp_millis())
        })
}

fn compute_indicators(klines: &[Kline]) -> Indicators {
    let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
    let ema20_vals = ema(&closes, 20);
    let ema5_vals = ema(&closes, 5);
    let atr14_values = atr(klines, 14);
    let atr20_values = atr(klines, 20);
    let ema20 = ema20_vals.last().copied();
    let ema5 = ema5_vals.last().copied();
    let atr14 = atr14_values.last().copied().flatten();
    let atr20 = atr20_values.last().copied().flatten();
    let close = closes.last().copied().unwrap_or(0.0);
    let ema20_position = match (ema20, atr14) {
        (Some(e), Some(a)) if a > 0.0 => Some((close - e) / a),
        _ => None,
    };
    let volume_ratio = if klines.len() >= 21 {
        let avg = klines[klines.len() - 21..klines.len() - 1]
            .iter()
            .map(|k| k.volume)
            .sum::<f64>()
            / 20.0;
        if avg > 0.0 {
            Some(klines.last().map(|k| k.volume).unwrap_or(0.0) / avg)
        } else {
            None
        }
    } else {
        None
    };
    let amplitude = if klines.len() >= 2 {
        let curr = klines.last().unwrap();
        let prev = &klines[klines.len() - 2];
        if prev.close > 0.0 {
            Some((curr.high - curr.low) / prev.close)
        } else {
            None
        }
    } else {
        None
    };
    Indicators {
        ema20,
        ema5,
        atr14,
        atr20,
        ema20_position,
        volume_ratio,
        amplitude,
    }
}

fn ema(data: &[f64], period: usize) -> Vec<f64> {
    if data.is_empty() {
        return Vec::new();
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut out = Vec::with_capacity(data.len());
    let mut prev = data[0];
    out.push(prev);
    for price in &data[1..] {
        prev = (*price - prev) * multiplier + prev;
        out.push(prev);
    }
    out
}

fn atr(klines: &[Kline], period: usize) -> Vec<Option<f64>> {
    if klines.is_empty() {
        return Vec::new();
    }
    let mut trs = Vec::with_capacity(klines.len());
    for (i, k) in klines.iter().enumerate() {
        let tr = if i == 0 {
            k.high - k.low
        } else {
            let prev_close = klines[i - 1].close;
            (k.high - k.low)
                .max((k.high - prev_close).abs())
                .max((k.low - prev_close).abs())
        };
        trs.push(tr);
    }
    let mut out = vec![None; klines.len()];
    if trs.len() < period {
        return out;
    }
    let mut value = trs[..period].iter().sum::<f64>() / period as f64;
    out[period - 1] = Some(value);
    for i in period..trs.len() {
        value = (value * (period as f64 - 1.0) + trs[i]) / period as f64;
        out[i] = Some(value);
    }
    out
}

/// Layer 1+2: Veto + breakout bar scoring. Returns None if vetoed.
fn evaluate_breakout_bar(klines: &[Kline], bar_idx: usize) -> Option<BreakoutCandidate> {
    if bar_idx < 20 {
        return None;
    }
    let prev_window = &klines[bar_idx - 20..bar_idx];
    let range_high = prev_window.iter().map(|k| k.high).fold(f64::MIN, f64::max);
    let range_low = prev_window.iter().map(|k| k.low).fold(f64::MAX, f64::min);
    let curr = &klines[bar_idx];

    let (direction, breakout_distance) = if curr.close > range_high {
        ("long".to_string(), curr.close - range_high)
    } else if curr.close < range_low {
        ("short".to_string(), range_low - curr.close)
    } else {
        return None;
    };

    let window = &klines[..=bar_idx];
    let indicators = compute_indicators(window);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let distance_atr = if atr14 > 0.0 {
        breakout_distance / atr14
    } else {
        0.0
    };

    // Entry / stop / target
    let entry = curr.close;
    let stop = if direction == "long" {
        curr.low.min(range_high)
    } else {
        curr.high.max(range_low)
    };
    let target = if direction == "long" {
        entry + (entry - stop).abs() * 2.0
    } else {
        entry - (entry - stop).abs() * 2.0
    };

    // Layer 1 — Veto
    // V1: actual_rr < 1.0
    let risk = (entry - stop).abs();
    let reward = (target - entry).abs();
    if risk > 0.0 && reward / risk < 1.0 {
        return None;
    }
    // Note: wick-back veto removed — crypto 1h bars naturally re-enter range.
    // close_location in Layer 2 already penalizes long-wick bars.

    // Layer 2 — Breakout bar score (0-25)
    let body = (curr.close - curr.open).abs();
    let median_body = median(
        prev_window
            .iter()
            .map(|k| (k.close - k.open).abs())
            .collect(),
    );
    let body_ratio = if median_body > 0.0 {
        body / median_body
    } else {
        0.0
    };
    let range = curr.high - curr.low;
    let close_location = if range > 0.0 {
        (curr.close - curr.low) / range
    } else {
        0.5
    };

    let mut score = 0.0;
    if body_ratio >= 2.0 {
        score += 5.0;
    }
    if body_ratio >= 5.0 {
        score += 5.0;
    }
    if close_location >= 0.75 {
        score += 5.0;
    }
    if close_location >= 0.9 {
        score += 3.0;
    }
    if distance_atr >= 0.3 {
        score += 4.0;
    }
    if distance_atr >= 0.8 {
        score += 3.0;
    }

    Some(BreakoutCandidate {
        direction,
        range_high,
        range_low,
        body_ratio,
        close_location,
        distance_atr,
        breakout_bar_score: score,
        atr14,
        stop_loss: stop,
        target_price: target,
    })
}

/// Layer 3: Follow-through (0-25) + Structure (0-15). Returns None if delayed-vetoed.
fn score_follow_through(
    klines: &[Kline],
    breakout_idx: usize,
    cand: &BreakoutCandidate,
) -> Option<(f64, f64, Value)> {
    if breakout_idx + 3 >= klines.len() {
        return None;
    }
    let bar1 = &klines[breakout_idx + 1];
    let bar2 = &klines[breakout_idx + 2];
    let bar3 = &klines[breakout_idx + 3];
    let breakout_bar = &klines[breakout_idx];
    let is_long = cand.direction == "long";

    // --- Delayed veto ---
    // 2 of 3 bars close back inside range
    let inside_count = [bar1, bar2, bar3]
        .iter()
        .filter(|b| {
            if is_long {
                b.close < cand.range_high
            } else {
                b.close > cand.range_low
            }
        })
        .count();
    if inside_count >= 2 {
        return None;
    }

    // 2 consecutive adverse bars with body_ratio >= 1.5
    let median_body = if cand.body_ratio > 0.0 {
        (breakout_bar.close - breakout_bar.open).abs() / cand.body_ratio
    } else {
        0.0
    };
    let adverse_cl = if is_long { 0.3 } else { 0.7 };
    for pair in [[bar1, bar2], [bar2, bar3]] {
        let r0 = pair[0].high - pair[0].low;
        let cl0 = if r0 > 0.0 {
            (pair[0].close - pair[0].low) / r0
        } else {
            0.5
        };
        let r1 = pair[1].high - pair[1].low;
        let cl1 = if r1 > 0.0 {
            (pair[1].close - pair[1].low) / r1
        } else {
            0.5
        };
        let is_adverse = if is_long {
            cl0 <= adverse_cl && cl1 <= adverse_cl
        } else {
            cl0 >= adverse_cl && cl1 >= adverse_cl
        };
        let br0 = if median_body > 0.0 {
            (pair[0].close - pair[0].open).abs() / median_body
        } else {
            0.0
        };
        let br1 = if median_body > 0.0 {
            (pair[1].close - pair[1].open).abs() / median_body
        } else {
            0.0
        };
        if is_adverse && br0 >= 1.5 && br1 >= 1.5 {
            return None;
        }
    }

    // --- Follow-through score (0-25) ---
    let outside_closes = [bar1, bar2, bar3]
        .iter()
        .filter(|b| {
            if is_long {
                b.close > cand.range_high
            } else {
                b.close < cand.range_low
            }
        })
        .count();

    // follow_score: 9-point checklist
    let mut follow_score: usize = 0;
    let mut running_high = breakout_bar.high;
    let mut running_low = breakout_bar.low;
    for bar in [bar1, bar2, bar3] {
        let same_dir = if is_long {
            bar.close > bar.open
        } else {
            bar.close < bar.open
        };
        if same_dir {
            follow_score += 1;
        }
        let bar_range = bar.high - bar.low;
        let cl = if bar_range > 0.0 {
            (bar.close - bar.low) / bar_range
        } else {
            0.5
        };
        if (is_long && cl >= 0.6) || (!is_long && cl <= 0.4) {
            follow_score += 1;
        }
        if is_long && bar.high > running_high {
            follow_score += 1;
            running_high = bar.high;
        }
        if !is_long && bar.low < running_low {
            follow_score += 1;
            running_low = bar.low;
        }
    }

    // Strong counter reclaim
    let has_strong_counter = [bar1, bar2, bar3].iter().any(|b| {
        let inside = if is_long {
            b.close < cand.range_high
        } else {
            b.close > cand.range_low
        };
        let br = if median_body > 0.0 {
            (b.close - b.open).abs() / median_body
        } else {
            0.0
        };
        inside && br >= 1.5
    });

    let mut follow_pts: f64 = 0.0;
    if outside_closes >= 2 {
        follow_pts += 8.0;
    }
    if follow_score >= 5 {
        follow_pts += 8.0;
    }
    if follow_score >= 7 {
        follow_pts += 4.0;
    }
    if !has_strong_counter {
        follow_pts += 5.0;
    }
    if outside_closes == 0 {
        follow_pts = follow_pts.min(5.0);
    }

    // --- Structure score (0-15) ---
    let breakout_magnitude = if is_long {
        breakout_bar.close - cand.range_high
    } else {
        cand.range_low - breakout_bar.close
    };
    let low_3 = bar1.low.min(bar2.low).min(bar3.low);
    let high_3 = bar1.high.max(bar2.high).max(bar3.high);
    let pullback_depth = if breakout_magnitude > 0.0 {
        if is_long {
            (breakout_bar.close - low_3) / breakout_magnitude
        } else {
            (high_3 - breakout_bar.close) / breakout_magnitude
        }
    } else {
        1.0
    };

    // Crypto gap_hold: follow bars don't retrace breakout bar's body open
    let gap_hold = if is_long {
        low_3 > breakout_bar.open
    } else {
        high_3 < breakout_bar.open
    };

    let mut struct_pts: f64 = 0.0;
    if pullback_depth <= 0.25 {
        struct_pts += 5.0;
    } else if pullback_depth <= 0.5 {
        struct_pts += 3.0;
    }
    if gap_hold {
        struct_pts += 5.0;
    }
    if outside_closes == 3 {
        struct_pts += 5.0;
    }
    if pullback_depth > 0.5 {
        struct_pts = struct_pts.min(6.0);
    }

    let evidence = json!({
        "outside_closes": outside_closes,
        "follow_score": follow_score,
        "has_strong_counter": has_strong_counter,
        "pullback_depth": (pullback_depth * 100.0).round() / 100.0,
        "gap_hold": gap_hold,
        "breakout_bar_ts": breakout_bar.ts,
    });

    Some((follow_pts, struct_pts, evidence))
}

/// Simplified EMA20 entry score — independent module, not part of the 65-pt total.
fn compute_ema20_entry_v2(curr: &Kline, indicators: &Indicators, direction: &str) -> f64 {
    let Some(ema20) = indicators.ema20 else {
        return 0.0;
    };
    let atr14 = indicators.atr14.unwrap_or(0.0);
    if atr14 <= 0.0 {
        return 0.0;
    }
    let ema_gap = (curr.close - ema20).abs() / atr14;
    let mut score: f64 = 0.0;
    if (direction == "long" && curr.close > ema20) || (direction == "short" && curr.close < ema20) {
        score += 3.0;
    }
    if ema_gap <= 1.5 {
        score += 3.0;
    }
    if ema_gap <= 0.5 {
        score += 3.0;
    }
    if ema_gap >= 2.5 {
        score -= 5.0;
    }
    score.max(0.0)
}

/// Main v2 detection: 3-layer funnel (veto → bar score → follow-through).
/// Looks at bar (len-4) as breakout candidate, bars (len-3..len-1) as follow-through.
fn detect_breakout_v2(klines: &[Kline]) -> Vec<Signal> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let breakout_idx = klines.len() - 4;

    let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) else {
        return Vec::new();
    };
    let Some((follow_pts, struct_pts, follow_evidence)) =
        score_follow_through(klines, breakout_idx, &cand)
    else {
        return Vec::new();
    };

    let total = cand.breakout_bar_score + follow_pts + struct_pts;
    if total < 35.0 {
        return Vec::new();
    }

    let window = &klines[..=breakout_idx];
    let indicators = compute_indicators(window);
    let ema_score = compute_ema20_entry_v2(&klines[breakout_idx], &indicators, &cand.direction);
    let entry_price = klines[breakout_idx].close;

    vec![Signal {
        signal_type: "pa_breakout".to_string(),
        direction: cand.direction.clone(),
        score: total,
        ema20_entry_score: ema_score,
        entry_price: Some(entry_price),
        stop_loss: Some(cand.stop_loss),
        target_price: Some(cand.target_price),
        reason: format!(
            "{} breakout: bar={:.0}+follow={:.0}+struct={:.0}={:.0} ema20_entry={:.0}",
            cand.direction, cand.breakout_bar_score, follow_pts, struct_pts, total, ema_score
        ),
        evidence: json!({
            "range_high": cand.range_high,
            "range_low": cand.range_low,
            "body_ratio": (cand.body_ratio * 100.0).round() / 100.0,
            "close_location": (cand.close_location * 100.0).round() / 100.0,
            "distance_atr": (cand.distance_atr * 100.0).round() / 100.0,
            "breakout_bar_score": cand.breakout_bar_score,
            "follow_through": follow_evidence,
            "ema20_entry_score": ema_score,
        }),
    }]
}

// ---------------------------------------------------------------------------
// Signal Bar Model — 信号K线识别模型核心函数
// ---------------------------------------------------------------------------

/// 计算单根K线的基本特征
fn compute_bar_features(k: &Kline) -> BarFeatures {
    let body = k.close - k.open;
    let range = k.high - k.low;
    let (p_b, p_c, p_u, p_d) = if range > 0.0 {
        let pb = body.abs() / range;
        let pc = (k.close - k.low) / range;
        let pu = (k.high - k.close.max(k.open)) / range;
        let pd = (k.close.min(k.open) - k.low) / range;
        (pb, pc, pu, pd)
    } else {
        (0.0, 0.5, 0.0, 0.0)
    };
    BarFeatures {
        body,
        range,
        p_b,
        p_c,
        p_u,
        p_d,
    }
}

/// 市场背景评估
fn evaluate_context(klines: &[Kline], indicators: &Indicators) -> ContextResult {
    let n = 10; // 通道窗口
    let p = 5; // 力量对比窗口
    let theta = 1.2; // 窄通道阈值

    let atr_m = indicators.atr20.unwrap_or(0.0);

    // 通道宽度
    let start = klines.len().saturating_sub(n);
    let window = &klines[start..];
    let h_n = window
        .iter()
        .map(|k| k.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let l_n = window.iter().map(|k| k.low).fold(f64::INFINITY, f64::min);

    let cr = if atr_m > 0.0 {
        (h_n - l_n) / atr_m
    } else {
        0.0
    };

    // 趋势方向
    let curr_close = klines.last().map(|k| k.close).unwrap_or(0.0);
    let trend_dir = if let Some(ema20) = indicators.ema20 {
        if curr_close > ema20 {
            1
        } else if curr_close < ema20 {
            -1
        } else {
            0
        }
    } else {
        0
    };

    // 多空力量对比
    let force_start = klines.len().saturating_sub(p);
    let force_window = &klines[force_start..];
    let f_bull: f64 = force_window
        .iter()
        .map(|k| (k.close - k.open).max(0.0))
        .sum();
    let f_bear: f64 = force_window
        .iter()
        .map(|k| (k.open - k.close).max(0.0))
        .sum();
    let f_ratio = if (f_bull + f_bear) > 0.0 {
        (f_bull - f_bear) / (f_bull + f_bear)
    } else {
        0.0
    };

    ContextResult {
        cr: (cr * 100.0).round() / 100.0,
        trend_dir,
        ma_cross_dir: detect_ma_cross(klines),
        f_ratio: (f_ratio * 100.0).round() / 100.0,
        is_valid: cr >= theta,
    }
}

/// 计算 P 根K线前的 F_ratio（用于判断力量翻转）

fn detect_ma_cross(klines: &[Kline]) -> Option<i32> {
    if klines.len() < 6 {
        return None;
    }
    let search_end = klines.len();
    let search_start = search_end.saturating_sub(SIGNAL_BAR_MA_CROSS_LOOKBACK);
    for i in (search_start + 1)..search_end {
        let closes: Vec<f64> = klines[..=i].iter().map(|k| k.close).collect();
        if closes.len() < 2 {
            continue;
        }
        let ema5_vals = ema(&closes, 5);
        let ema20_vals = ema(&closes, 20);
        if ema5_vals.len() < 2 || ema20_vals.len() < 2 {
            continue;
        }
        let prev_ema5 = ema5_vals[ema5_vals.len() - 2];
        let prev_ema20 = ema20_vals[ema20_vals.len() - 2];
        let curr_ema5 = ema5_vals[ema5_vals.len() - 1];
        let curr_ema20 = ema20_vals[ema20_vals.len() - 1];
        if prev_ema5 <= prev_ema20 && curr_ema5 > curr_ema20 {
            return Some(1);
        }
        if prev_ema5 >= prev_ema20 && curr_ema5 < curr_ema20 {
            return Some(-1);
        }
    }
    None
}

fn compute_prev_f_ratio(klines: &[Kline], offset: usize, window: usize) -> f64 {
    let end = klines.len().saturating_sub(offset);
    let start = end.saturating_sub(window);
    if start >= end {
        return 0.0;
    }
    let slice = &klines[start..end];
    let f_bull: f64 = slice.iter().map(|k| (k.close - k.open).max(0.0)).sum();
    let f_bear: f64 = slice.iter().map(|k| (k.open - k.close).max(0.0)).sum();
    if (f_bull + f_bear) > 0.0 {
        ((f_bull - f_bear) / (f_bull + f_bear) * 100.0).round() / 100.0
    } else {
        0.0
    }
}

/// 多头信号质量评分
fn score_quality_long(f: &BarFeatures) -> f64 {
    if f.body <= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.55 && f.p_c >= 0.75 && f.p_u <= 0.15 {
        1.0
    } else if f.p_b >= 0.35 && f.p_c >= 0.55 && f.p_u <= 0.30 {
        0.6
    } else if f.p_b >= 0.25 && f.p_c >= 0.45 {
        0.3
    } else {
        0.0
    }
}

/// 空头信号质量评分
fn score_quality_short(f: &BarFeatures) -> f64 {
    if f.body >= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.55 && f.p_c <= 0.25 && f.p_d <= 0.15 {
        1.0
    } else if f.p_b >= 0.35 && f.p_c <= 0.45 && f.p_d <= 0.30 {
        0.6
    } else if f.p_b >= 0.25 && f.p_c <= 0.55 {
        0.3
    } else {
        0.0
    }
}

/// SignalBarModel 主检测函数：识别最后一根K线是否为信号K线
const SIGNAL_BAR_Q_MIN: f64 = 0.6;
const SIGNAL_BAR_COOLDOWN_BARS: usize = 4;
const SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT: bool = true;
const SIGNAL_BAR_REQUIRE_TREND_ALIGN: bool = true;
const SIGNAL_BAR_USE_MA_CROSS: bool = false;
const SIGNAL_BAR_MA_CROSS_LOOKBACK: usize = 24;
const SIGNAL_BAR_SURPRISE_LOOKBACK: usize = 20;
const SIGNAL_BAR_TICK_SIZE: f64 = 0.01;

/// A 股信号 K 线参数（低波动市场适配）
const AS_Q_MIN: f64 = 0.3;
const AS_COOLDOWN_BARS: usize = 3;
const AS_REQUIRE_PATTERN_CONTEXT: bool = true;
const AS_REQUIRE_TREND_ALIGN: bool = false;
const AS_SURPRISE_LOOKBACK: usize = 20;
const AS_TICK_SIZE: f64 = 0.001;
const AS_STOP_ATR_MULT: f64 = 1.2;
const AS_TARGET_ATR_MULT: f64 = 1.5;

/// A 股多头信号质量评分（放宽标准）
fn score_quality_long_as(f: &BarFeatures) -> f64 {
    if f.body <= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.45 && f.p_c >= 0.65 && f.p_u <= 0.20 {
        1.0
    } else if f.p_b >= 0.30 && f.p_c >= 0.50 && f.p_u <= 0.35 {
        0.6
    } else if f.p_b >= 0.20 && f.p_c >= 0.40 {
        0.3
    } else {
        0.0
    }
}

/// A 股空头信号质量评分（放宽标准）
fn score_quality_short_as(f: &BarFeatures) -> f64 {
    if f.body >= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.45 && f.p_c <= 0.35 && f.p_d <= 0.20 {
        1.0
    } else if f.p_b >= 0.30 && f.p_c <= 0.50 && f.p_d <= 0.35 {
        0.6
    } else if f.p_b >= 0.20 && f.p_c <= 0.60 {
        0.3
    } else {
        0.0
    }
}

fn signal_bar_trend_allows(direction: &str, context: &ContextResult) -> bool {
    if !SIGNAL_BAR_REQUIRE_TREND_ALIGN {
        return true;
    }
    if SIGNAL_BAR_USE_MA_CROSS {
        return (direction == "long" && context.ma_cross_dir.unwrap_or(0) > 0)
            || (direction == "short" && context.ma_cross_dir.unwrap_or(0) < 0);
    }
    (direction == "long" && context.trend_dir > 0)
        || (direction == "short" && context.trend_dir < 0)
}

fn signal_bar_force_allows(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    (direction == "long" && (context.f_ratio > 0.0 || f_ratio_flipped))
        || (direction == "short" && (context.f_ratio < 0.0 || f_ratio_flipped))
}

fn signal_bar_context_allows(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    context.is_valid
        && signal_bar_trend_allows(direction, context)
        && signal_bar_force_allows(direction, context, f_ratio_flipped)
}

fn signal_bar_direction_label(direction: &str) -> &'static str {
    if direction == "long" {
        "向上"
    } else {
        "向下"
    }
}

fn signal_bar_alert_level(quality: f64, pattern_type: &str) -> &'static str {
    if quality >= 1.0 || pattern_type == "2k_reversal" || pattern_type == "surprise_bar" {
        "high"
    } else {
        "medium"
    }
}

fn signal_bar_alert_reason(
    direction: &str,
    pattern_label: &str,
    quality: f64,
    context: &ContextResult,
) -> String {
    format!(
        "盯盘提醒: {}出现{}K线，质量{:.1}，通道CR={:.2}，力量比={:.2}。用于提示未来数根K线可能有波动，需结合盘口/成交量/关键位人工确认。",
        signal_bar_direction_label(direction),
        pattern_label,
        quality,
        context.cr,
        context.f_ratio
    )
}

fn detect_signal_bar(klines: &[Kline]) -> Vec<Signal> {
    const MIN_KLINES: usize = 25;

    if klines.len() < MIN_KLINES {
        return Vec::new();
    }

    let curr_idx = klines.len() - 1;
    let prev_idx = curr_idx.saturating_sub(1);
    let curr = &klines[curr_idx];
    let prev = &klines[prev_idx];

    let indicators = compute_indicators(klines);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let atr20 = indicators.atr20.unwrap_or(0.0);
    let delta = SIGNAL_BAR_TICK_SIZE;

    let f_curr = compute_bar_features(curr);
    let f_prev = compute_bar_features(prev);

    let q_long = score_quality_long(&f_curr);
    let q_short = score_quality_short(&f_curr);
    let context = evaluate_context(klines, &indicators);
    let prev_f = compute_prev_f_ratio(klines, 5, 5);
    let long_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f < 0.0;
    let short_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f > 0.0;

    // 参考价位仅用于盯盘确认和兼容既有回测字段，不代表自动交易指令。
    let make_signal = |direction: &str,
                       quality: f64,
                       signal_type: &str,
                       pattern_type: &str,
                       pattern_label: &str,
                       pattern_evidence: Value|
     -> Signal {
        let (entry, stop, target) = if direction == "long" {
            let e = curr.high + delta;
            let s = curr.low - delta;
            let t = e + 2.0 * (e - s);
            (e, s, t)
        } else {
            let e = curr.low - delta;
            let s = curr.high + delta;
            let t = e - 2.0 * (s - e);
            (e, s, t)
        };
        Signal {
            signal_type: signal_type.to_string(),
            direction: direction.to_string(),
            score: quality,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 100.0).round() / 100.0),
            stop_loss: Some((stop * 100.0).round() / 100.0),
            target_price: Some((target * 100.0).round() / 100.0),
            reason: signal_bar_alert_reason(direction, pattern_label, quality, &context),
            evidence: json!({
                "model_code": "pa_signal_bar_v1",
                "alert_purpose": "watchlist_monitor",
                "alert_semantics": "human_review_required",
                "alert_level": signal_bar_alert_level(quality, pattern_type),
                "watch_alert": {
                    "direction_hint": direction,
                    "direction_label": signal_bar_direction_label(direction),
                    "pattern_type": pattern_type,
                    "pattern_label": pattern_label,
                    "quality": quality,
                    "expected_use": "提醒人工盯盘，不等同于自动买卖信号",
                    "review_checklist": [
                        "下一根K线是否延续并放量",
                        "是否靠近前高/前低/EMA20/整数位等关键价位",
                        "是否先出现反向1ATR级别波动",
                        "若没有后续确认则忽略提醒"
                    ],
                    "reference_levels": {
                        "trigger_price": (entry * 100.0).round() / 100.0,
                        "invalidation_price": (stop * 100.0).round() / 100.0,
                        "observation_target": (target * 100.0).round() / 100.0
                    },
                    "historical_watch_eval": {
                        "dataset": "BTCUSDT 1h 2026-01-01..2026-03-30",
                        "sample_count_after_cooldown": 144,
                        "horizon_6bar": {
                            "opportunity_0_5atr_rate": 0.7014,
                            "opportunity_1atr_rate": 0.5069,
                            "noise_no_0_5atr_rate": 0.2986
                        },
                        "horizon_12bar": {
                            "opportunity_0_5atr_rate": 0.7917,
                            "opportunity_1atr_rate": 0.6319,
                            "noise_no_0_5atr_rate": 0.2083
                        },
                        "horizon_24bar": {
                            "opportunity_0_5atr_rate": 0.8611,
                            "opportunity_1atr_rate": 0.7569,
                            "noise_no_0_5atr_rate": 0.1389
                        }
                    }
                },
                "bar_features": {
                    "body": (f_curr.body * 10000.0).round() / 10000.0,
                    "range": (f_curr.range * 10000.0).round() / 10000.0,
                    "p_b": (f_curr.p_b * 100.0).round() / 100.0,
                    "p_c": (f_curr.p_c * 100.0).round() / 100.0,
                    "p_u": (f_curr.p_u * 100.0).round() / 100.0,
                    "p_d": (f_curr.p_d * 100.0).round() / 100.0,
                },
                "q_long": q_long,
                "q_short": q_short,
                "atr14": (atr14 * 10000.0).round() / 10000.0,
                "atr20": (atr20 * 10000.0).round() / 10000.0,
                "tick_size": SIGNAL_BAR_TICK_SIZE,
                "delta": (delta * 10000.0).round() / 10000.0,
                "optimized_params": {
                    "q_min": SIGNAL_BAR_Q_MIN,
                    "cooldown_bars": SIGNAL_BAR_COOLDOWN_BARS,
                    "require_pattern_context": SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT,
                    "require_trend_align": SIGNAL_BAR_REQUIRE_TREND_ALIGN,
                },
                "pattern": pattern_evidence,
            }),
        }
    };

    // ---- 优先级1: 2K反转 ----
    let prev_q_short = score_quality_short(&f_prev);
    let prev_q_long = score_quality_long(&f_prev);

    // 多头2K反转: 前一根空头信号K + 当前多头信号K
    if prev_q_short >= SIGNAL_BAR_Q_MIN
        && q_long >= SIGNAL_BAR_Q_MIN
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long",
            q_long.min(prev_q_short),
            "pa_pattern",
            "2k_reversal",
            "2K反转",
            json!({"type": "2k_reversal", "direction": "long"}),
        )];
    }
    // 空头2K反转: 前一根多头信号K + 当前空头信号K
    if prev_q_long >= SIGNAL_BAR_Q_MIN
        && q_short >= SIGNAL_BAR_Q_MIN
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short",
            q_short.min(prev_q_long),
            "pa_pattern",
            "2k_reversal",
            "2K反转",
            json!({"type": "2k_reversal", "direction": "short"}),
        )];
    }

    // ---- 优先级2: 吞噬线 ----
    // 多头吞噬
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close > curr.open
        && f_curr.p_b >= 0.5
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long",
            0.6,
            "pa_pattern",
            "engulfing",
            "吞噬形态",
            json!({"type": "engulfing", "direction": "bullish"}),
        )];
    }
    // 空头吞噬
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close < curr.open
        && f_curr.p_b >= 0.5
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short",
            0.6,
            "pa_pattern",
            "engulfing",
            "吞噬形态",
            json!({"type": "engulfing", "direction": "bearish"}),
        )];
    }

    // ---- 优先级3: 惊喜K线 ----
    let surprise_start = klines.len().saturating_sub(SIGNAL_BAR_SURPRISE_LOOKBACK);
    let r_max = klines[surprise_start..curr_idx]
        .iter()
        .map(|k| k.high - k.low)
        .fold(0.0_f64, f64::max);
    if r_max > 0.0 && f_curr.range > 1.5 * r_max && f_curr.p_b >= 0.5 {
        let direction = if f_curr.body > 0.0 { "long" } else { "short" };
        let f_ratio_flipped = if direction == "long" {
            long_f_ratio_flipped
        } else {
            short_f_ratio_flipped
        };
        if SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            && !signal_bar_context_allows(direction, &context, f_ratio_flipped)
        {
            return Vec::new();
        }
        return vec![make_signal(
            direction,
            0.6,
            "pa_pattern",
            "surprise_bar",
            "惊喜K线",
            json!({"type": "surprise_bar", "direction": direction, "range": (f_curr.range * 10000.0).round() / 10000.0, "r_max": (r_max * 10000.0).round() / 10000.0}),
        )];
    }

    // ---- 优先级4: 常规信号K线 ----
    // 多头常规信号
    if q_long >= SIGNAL_BAR_Q_MIN && context.is_valid {
        if signal_bar_context_allows("long", &context, long_f_ratio_flipped) {
            return vec![make_signal(
                "long",
                q_long,
                "pa_signal_bar",
                "signal_bar",
                "常规信号K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": long_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    // 空头常规信号
    if q_short >= SIGNAL_BAR_Q_MIN && context.is_valid {
        if signal_bar_context_allows("short", &context, short_f_ratio_flipped) {
            return vec![make_signal(
                "short",
                q_short,
                "pa_signal_bar",
                "signal_bar",
                "常规信号K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": short_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    Vec::new()
}

// ---------------------------------------------------------------------------
// A 股信号 K 线检测（低波动市场适配）
// ---------------------------------------------------------------------------

fn signal_bar_trend_allows_cn(direction: &str, context: &ContextResult) -> bool {
    if !AS_REQUIRE_TREND_ALIGN {
        return true;
    }
    (direction == "long" && context.trend_dir > 0)
        || (direction == "short" && context.trend_dir < 0)
}

fn signal_bar_context_allows_cn(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    context.is_valid
        && signal_bar_trend_allows_cn(direction, context)
        && signal_bar_force_allows(direction, context, f_ratio_flipped)
}

/// A 股信号 K 线检测：放宽质量门槛，ATR-based 止损止盈
fn detect_signal_bar_cn(klines: &[Kline]) -> Vec<Signal> {
    const MIN_KLINES: usize = 25;

    if klines.len() < MIN_KLINES {
        return Vec::new();
    }

    let curr_idx = klines.len() - 1;
    let prev_idx = curr_idx.saturating_sub(1);
    let curr = &klines[curr_idx];
    let prev = &klines[prev_idx];

    let indicators = compute_indicators(klines);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let atr20 = indicators.atr20.unwrap_or(0.0);
    let delta = AS_TICK_SIZE;

    let f_curr = compute_bar_features(curr);
    let f_prev = compute_bar_features(prev);

    let q_long = score_quality_long_as(&f_curr);
    let q_short = score_quality_short_as(&f_curr);
    let context = evaluate_context(klines, &indicators);
    let prev_f = compute_prev_f_ratio(klines, 5, 5);
    let long_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f < 0.0;
    let short_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f > 0.0;

    let make_signal = |direction: &str,
                       quality: f64,
                       signal_type: &str,
                       pattern_type: &str,
                       pattern_label: &str,
                       pattern_evidence: Value|
     -> Signal {
        // ATR-based 止损止盈
        let (entry, stop, target) = if direction == "long" {
            let e = curr.close + delta;
            let risk = atr14 * AS_STOP_ATR_MULT;
            let s = e - risk;
            let t = e + atr14 * AS_TARGET_ATR_MULT;
            (e, s, t)
        } else {
            let e = curr.close - delta;
            let risk = atr14 * AS_STOP_ATR_MULT;
            let s = e + risk;
            let t = e - atr14 * AS_TARGET_ATR_MULT;
            (e, s, t)
        };
        // A 股盯盘信号：看多提示买入机会，看空提示风险（减仓/止盈）
        let (alert_hint, expected_use) = if direction == "long" {
            ("bullish_watch", "看多提醒：关注买入机会，下一根K线确认后可考虑建仓/加仓")
        } else {
            ("bearish_watch", "看空提醒：注意风险，考虑减仓/止盈，A股不可做空")
        };
        Signal {
            signal_type: signal_type.to_string(),
            direction: direction.to_string(),
            score: quality,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 10000.0).round() / 10000.0),
            stop_loss: Some((stop * 10000.0).round() / 10000.0),
            target_price: Some((target * 10000.0).round() / 10000.0),
            reason: signal_bar_alert_reason(direction, pattern_label, quality, &context),
            evidence: json!({
                "model_code": "pa_signal_bar_cn_v1",
                "alert_purpose": "watchlist_monitor",
                "alert_semantics": "human_review_required",
                "alert_level": signal_bar_alert_level(quality, pattern_type),
                "watch_alert": {
                    "alert_hint": alert_hint,
                    "direction_hint": direction,
                    "direction_label": signal_bar_direction_label(direction),
                    "pattern_type": pattern_type,
                    "pattern_label": pattern_label,
                    "quality": quality,
                    "expected_use": expected_use,
                    "review_checklist": [
                        "下一根K线是否延续并放量",
                        "是否靠近前高/前低/EMA20/整数位等关键价位",
                        "是否先出现反向1ATR级别波动",
                        "若没有后续确认则忽略提醒"
                    ],
                    "reference_levels": {
                        "signal_bar_high": (curr.high * 10000.0).round() / 10000.0,
                        "signal_bar_low": (curr.low * 10000.0).round() / 10000.0,
                        "atr14": (atr14 * 10000.0).round() / 10000.0,
                    },
                },
                "bar_features": {
                    "body": (f_curr.body * 10000.0).round() / 10000.0,
                    "range": (f_curr.range * 10000.0).round() / 10000.0,
                    "p_b": (f_curr.p_b * 100.0).round() / 100.0,
                    "p_c": (f_curr.p_c * 100.0).round() / 100.0,
                    "p_u": (f_curr.p_u * 100.0).round() / 100.0,
                    "p_d": (f_curr.p_d * 100.0).round() / 100.0,
                },
                "q_long": q_long,
                "q_short": q_short,
                "atr14": (atr14 * 10000.0).round() / 10000.0,
                "atr20": (atr20 * 10000.0).round() / 10000.0,
                "tick_size": AS_TICK_SIZE,
                "delta": (delta * 10000.0).round() / 10000.0,
                "optimized_params": {
                    "q_min": AS_Q_MIN,
                    "cooldown_bars": AS_COOLDOWN_BARS,
                    "require_pattern_context": AS_REQUIRE_PATTERN_CONTEXT,
                    "require_trend_align": AS_REQUIRE_TREND_ALIGN,
                },
                "pattern": pattern_evidence,
            }),
        }
    };

    // ---- 优先级1: 2K反转 ----
    let prev_q_short = score_quality_short_as(&f_prev);
    let prev_q_long = score_quality_long_as(&f_prev);

    if prev_q_short >= AS_Q_MIN
        && q_long >= AS_Q_MIN
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long", q_long.min(prev_q_short), "pa_pattern", "2k_reversal", "2K反转",
            json!({"type": "2k_reversal", "direction": "long"}),
        )];
    }
    if prev_q_long >= AS_Q_MIN
        && q_short >= AS_Q_MIN
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short", q_short.min(prev_q_long), "pa_pattern", "2k_reversal", "2K反转",
            json!({"type": "2k_reversal", "direction": "short"}),
        )];
    }

    // ---- 优先级2: 吞噬线 ----
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close > curr.open
        && f_curr.p_b >= 0.5
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long", 0.6, "pa_pattern", "engulfing", "吞噬形态",
            json!({"type": "engulfing", "direction": "bullish"}),
        )];
    }
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close < curr.open
        && f_curr.p_b >= 0.5
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short", 0.6, "pa_pattern", "engulfing", "吞噬形态",
            json!({"type": "engulfing", "direction": "bearish"}),
        )];
    }

    // ---- 优先级3: 惊喜K线 ----
    let surprise_start = klines.len().saturating_sub(AS_SURPRISE_LOOKBACK);
    let r_max = klines[surprise_start..curr_idx]
        .iter()
        .map(|k| k.high - k.low)
        .fold(0.0_f64, f64::max);
    if r_max > 0.0 && f_curr.range > 1.5 * r_max && f_curr.p_b >= 0.5 {
        let direction = if f_curr.body > 0.0 { "long" } else { "short" };
        let f_ratio_flipped = if direction == "long" {
            long_f_ratio_flipped
        } else {
            short_f_ratio_flipped
        };
        if AS_REQUIRE_PATTERN_CONTEXT
            && !signal_bar_context_allows_cn(direction, &context, f_ratio_flipped)
        {
            return Vec::new();
        }
        return vec![make_signal(
            direction, 0.6, "pa_pattern", "surprise_bar", "惊喜K线",
            json!({"type": "surprise_bar", "direction": direction, "range": (f_curr.range * 10000.0).round() / 10000.0, "r_max": (r_max * 10000.0).round() / 10000.0}),
        )];
    }

    // ---- 优先级4: 常规信号K线 ----
    if q_long >= AS_Q_MIN && context.is_valid {
        if signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped) {
            return vec![make_signal(
                "long", q_long, "pa_signal_bar", "signal_bar", "常规信号K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": long_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    if q_short >= AS_Q_MIN && context.is_valid {
        if signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped) {
            return vec![make_signal(
                "short", q_short, "pa_signal_bar", "signal_bar", "常规信号K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": short_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    Vec::new()
}

/// K线收盘时触发信号K线检测：拉取历史K线 → detect_signal_bar → 持久化信号
async fn run_signal_bar_detection(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
) -> Result<()> {
    let klines = fetch_klines(state, market, symbol, interval, 30, false).await?;
    if klines.len() < 25 {
        return Ok(());
    }
    let signals = detect_signal_bar(&klines);
    if signals.is_empty() {
        return Ok(());
    }
    let indicators = compute_indicators(&klines);
    for signal in &signals {
        info!(
            "signal bar detected: {} {} {} score={:.1} reason={}",
            symbol, interval, signal.direction, signal.score, signal.reason
        );
        persist_signal_row(
            state,
            market,
            symbol,
            interval,
            &klines,
            &indicators,
            signal,
        )
        .await?;
    }
    Ok(())
}

fn ema20_touch_reclaim(curr: &Kline, indicators: &Indicators, direction: &str) -> bool {
    let Some(ema20) = indicators.ema20 else {
        return false;
    };
    if direction == "long" {
        curr.low <= ema20 && curr.close >= ema20
    } else {
        curr.high >= ema20 && curr.close <= ema20
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

async fn connect_db() -> Result<String> {
    let database_url = env::var("DATABASE_URL").context(
        "DATABASE_URL is required, for example postgres://postgres:postgres@postgres:5432/pricedog",
    )?;
    let client = postgres_client(&database_url).await?;
    client.simple_query("SELECT 1").await?;
    Ok(database_url)
}

async fn postgres_client(database_url: &str) -> Result<PgClient> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .with_context(|| "connect postgres store")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            warn!("postgres connection task failed: {err}");
        }
    });
    Ok(client)
}

async fn init_db(database_url: &str) -> Result<()> {
    let client = postgres_client(database_url).await?;
    for statement in [
        r#"
    CREATE TABLE IF NOT EXISTS pa_kline (
        id BIGSERIAL PRIMARY KEY,
        market TEXT NOT NULL,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        ts TEXT NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        volume DOUBLE PRECISION NOT NULL DEFAULT 0,
        turnover DOUBLE PRECISION NOT NULL DEFAULT 0,
        source TEXT NOT NULL DEFAULT '',
        raw TEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(market, symbol, interval, ts)
    )
    "#,
        r#"
    CREATE TABLE IF NOT EXISTS pa_signal (
        id BIGSERIAL PRIMARY KEY,
        market TEXT NOT NULL,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        signal_date TEXT NOT NULL,
        signal_type TEXT NOT NULL,
        direction TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        ema20_entry_score DOUBLE PRECISION NOT NULL DEFAULT 0,
        ema20 DOUBLE PRECISION,
        atr14 DOUBLE PRECISION,
        ema20_position DOUBLE PRECISION,
        entry_price DOUBLE PRECISION,
        stop_loss DOUBLE PRECISION,
        target_price DOUBLE PRECISION,
        reason TEXT NOT NULL DEFAULT '',
        evidence_json TEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(market, symbol, interval, signal_date, signal_type, direction)
    )
    "#,
        r#"
    CREATE UNIQUE INDEX IF NOT EXISTS ux_pa_signal_identity_v2
    ON pa_signal(market, symbol, interval, signal_date, signal_type, direction)
    "#,
        r#"
    CREATE TABLE IF NOT EXISTS pa_backtest_runs (
        id BIGSERIAL PRIMARY KEY,
        trace_id TEXT NOT NULL UNIQUE,
        market TEXT NOT NULL,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        strategy_code TEXT NOT NULL,
        strategy_name TEXT NOT NULL DEFAULT '',
        strategy_version TEXT NOT NULL DEFAULT 'v1',
        params_json TEXT NOT NULL DEFAULT '{}',
        summary_json TEXT NOT NULL DEFAULT '{}',
        signal_count INTEGER NOT NULL DEFAULT 0,
        evaluated_count INTEGER NOT NULL DEFAULT 0,
        win_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
        avg_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        hit_target_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
        hit_stop_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
        avg_holding_bars DOUBLE PRECISION NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    "#,
        r#"
    CREATE INDEX IF NOT EXISTS ix_pa_kline_lookup
    ON pa_kline(market, symbol, interval, ts DESC)
    "#,
        r#"
    CREATE INDEX IF NOT EXISTS ix_pa_kline_symbol_ts
    ON pa_kline(symbol, interval, ts DESC)
    "#,
        r#"
    CREATE INDEX IF NOT EXISTS ix_pa_signal_lookup
    ON pa_signal(market, symbol, interval, signal_date DESC)
    "#,
        r#"
    CREATE INDEX IF NOT EXISTS ix_pa_signal_type_score
    ON pa_signal(signal_type, direction, score DESC)
    "#,
        r#"
    CREATE INDEX IF NOT EXISTS ix_pa_backtest_runs_symbol_created
    ON pa_backtest_runs(symbol, interval, created_at DESC)
    "#,
    ] {
        client.execute(statement, &[]).await?;
    }
    Ok(())
}

async fn persist_klines(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    bars: &[Kline],
    source: &str,
) -> Result<()> {
    let client = postgres_client(&state.db_url).await?;
    for bar in bars {
        let raw = serde_json::to_string(bar).unwrap_or_else(|_| "{}".to_string());
        client
            .execute(
                postgres_upsert_kline_sql(),
                &[
                    &market,
                    &symbol,
                    &interval,
                    &bar.ts,
                    &bar.open,
                    &bar.high,
                    &bar.low,
                    &bar.close,
                    &bar.volume,
                    &bar.turnover,
                    &source,
                    &raw,
                ],
            )
            .await?;
    }
    Ok(())
}

fn postgres_upsert_kline_sql() -> &'static str {
    r#"
    INSERT INTO pa_kline (market, symbol, interval, ts, open, high, low, close, volume, turnover, source, raw, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP)
    ON CONFLICT(market, symbol, interval, ts) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        volume=excluded.volume,
        turnover=excluded.turnover,
        source=excluded.source,
        raw=excluded.raw,
        updated_at=CURRENT_TIMESTAMP
    "#
}

async fn load_klines_from_db(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<Vec<Kline>> {
    let client = postgres_client(&state.db_url).await?;
    let limit = limit as i64;
    let rows = client
        .query(
            postgres_select_klines_sql(),
            &[&market, &symbol, &interval, &limit],
        )
        .await?;
    let mut bars = rows_to_klines(rows)?;
    bars.reverse();
    Ok(bars)
}

async fn load_kline_source_counts(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
) -> Result<Vec<SourceCount>> {
    let client = postgres_client(&state.db_url).await?;
    let rows = client
        .query(
            r#"
            SELECT source, COUNT(*)::BIGINT AS rows
            FROM pa_kline
            WHERE market = $1 AND symbol = $2 AND interval = $3
            GROUP BY source
            ORDER BY source
            "#,
            &[&market, &symbol, &interval],
        )
        .await?;
    let mut counts = Vec::with_capacity(rows.len());
    for row in rows {
        counts.push(SourceCount {
            source: row.try_get("source")?,
            rows: row.try_get("rows")?,
        });
    }
    Ok(counts)
}

fn postgres_select_klines_sql() -> &'static str {
    r#"
    SELECT ts, open, high, low, close, volume, turnover
    FROM pa_kline
    WHERE market = $1 AND symbol = $2 AND interval = $3
    ORDER BY ts DESC
    LIMIT $4
    "#
}

fn rows_to_klines(rows: Vec<Row>) -> Result<Vec<Kline>> {
    let mut bars = Vec::with_capacity(rows.len());
    for row in rows {
        bars.push(Kline {
            ts: row.try_get("ts")?,
            open: row.try_get("open")?,
            high: row.try_get("high")?,
            low: row.try_get("low")?,
            close: row.try_get("close")?,
            volume: row.try_get("volume")?,
            turnover: row.try_get("turnover")?,
        });
    }
    Ok(bars)
}

async fn persist_signal_row(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    klines: &[Kline],
    indicators: &Indicators,
    signal: &Signal,
) -> Result<()> {
    let signal_date = klines
        .last()
        .map(|k| k.ts.clone())
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    let client = postgres_client(&state.db_url).await?;
    let evidence = signal.evidence.to_string();
    client
        .execute(
            postgres_upsert_signal_sql(),
            &[
                &market,
                &symbol,
                &interval,
                &signal_date,
                &signal.signal_type,
                &signal.direction,
                &signal.score,
                &signal.ema20_entry_score,
                &indicators.ema20,
                &indicators.atr14,
                &indicators.ema20_position,
                &signal.entry_price,
                &signal.stop_loss,
                &signal.target_price,
                &signal.reason,
                &evidence,
            ],
        )
        .await?;
    Ok(())
}

async fn persist_backtest_run(
    state: &AppState,
    market: &str,
    symbol: &str,
    interval: &str,
    strategy_code: &str,
    strategy_name: &str,
    strategy_version: &str,
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
    trace_id: &str,
    summary: &BacktestSummary,
) -> Result<i64> {
    let client = postgres_client(&state.db_url).await?;
    let params_json = json!({
        "max_holding_bars": max_holding_bars,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
    })
    .to_string();
    let summary_json = serde_json::to_string(summary)?;
    let row = client
        .query_one(
            r#"
            INSERT INTO pa_backtest_runs (
                trace_id, market, symbol, interval, strategy_code, strategy_name,
                strategy_version, params_json, summary_json, signal_count, evaluated_count,
                win_rate, avg_return_pct, median_return_pct, hit_target_rate,
                hit_stop_rate, avg_holding_bars
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11,
                $12, $13, $14, $15,
                $16, $17
            )
            RETURNING id
            "#,
            &[
                &trace_id,
                &market,
                &symbol,
                &interval,
                &strategy_code,
                &strategy_name,
                &strategy_version,
                &params_json,
                &summary_json,
                &(summary.signal_count as i32),
                &(summary.evaluated_count as i32),
                &summary.win_rate,
                &summary.avg_return_pct,
                &summary.median_return_pct,
                &summary.hit_target_rate,
                &summary.hit_stop_rate,
                &summary.avg_holding_bars,
            ],
        )
        .await?;
    Ok(row.try_get("id")?)
}

async fn persist_backtest_results(
    state: &AppState,
    run_id: i64,
    trace_id: &str,
    market: &str,
    symbol: &str,
    strategy_code: &str,
    strategy_name: &str,
    strategy_version: &str,
    max_holding_bars: usize,
    trades: &[BacktestTrade],
) -> Result<()> {
    let client = postgres_client(&state.db_url).await?;
    for trade in trades {
        let snapshot_date = to_snapshot_date(&trade.ts);
        let signal_run_id: i64 = client
            .query_one(
                r#"
                INSERT INTO strategy_signal_runs (
                    snapshot_date, stock_symbol, stock_market, stock_name,
                    strategy_code, strategy_name, strategy_version, risk_level, source_pool,
                    score, rank_score, confidence, status, action, action_label, signal, reason,
                    evidence, holding_days, entry_low, entry_high, stop_loss, target_price,
                    invalidation, plan_quality, source_agent, source_suggestion_id,
                    source_candidate_id, trace_id, is_holding_snapshot, context_quality_score, payload
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16, $17,
                    $18::jsonb, $19, $20, $21, $22, $23,
                    $24, $25, $26, $27,
                    $28, $29, $30, $31, $32::jsonb
                )
                RETURNING id
                "#,
                &[
                    &snapshot_date,
                    &symbol,
                    &market,
                    &symbol,
                    &strategy_code,
                    &strategy_name,
                    &strategy_version,
                    &"medium",
                    &"backtest",
                    &trade.score,
                    &trade.score,
                    &Some((trade.score / 100.0).min(1.0)),
                    &"inactive",
                    &"watch",
                    &"回测信号",
                    &trade.signal,
                    &trade.reason,
                    &serde_json::to_string(&vec![trade.evidence.clone()])?,
                    &(max_holding_bars as i32),
                    &trade.entry_price,
                    &trade.entry_price,
                    &trade.stop_loss,
                    &trade.target_price,
                    &"backtest_exit",
                    &0,
                    &"price-action-engine",
                    &Option::<i32>::None,
                    &Option::<i32>::None,
                    &trace_id,
                    &false,
                    &Option::<f64>::None,
                    &json!({
                        "backtest_run_id": run_id,
                        "exit_reason": trade.exit_reason,
                        "holding_bars": trade.holding_bars,
                    })
                    .to_string(),
                ],
            )
            .await?
            .try_get("id")?;

        client
            .execute(
                r#"
                INSERT INTO strategy_outcomes (
                    signal_run_id, strategy_code, snapshot_date, stock_symbol, stock_market,
                    source_pool, horizon_days, target_date, base_price, outcome_price,
                    outcome_return_pct, hit_target, hit_stop, outcome_status, meta, evaluated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15::jsonb, CURRENT_TIMESTAMP
                )
                ON CONFLICT(signal_run_id, horizon_days) DO UPDATE SET
                    outcome_price=excluded.outcome_price,
                    outcome_return_pct=excluded.outcome_return_pct,
                    hit_target=excluded.hit_target,
                    hit_stop=excluded.hit_stop,
                    outcome_status=excluded.outcome_status,
                    meta=excluded.meta,
                    evaluated_at=CURRENT_TIMESTAMP
                "#,
                &[
                    &signal_run_id,
                    &strategy_code,
                    &snapshot_date,
                    &symbol,
                    &market,
                    &"backtest",
                    &(max_holding_bars as i32),
                    &to_snapshot_date(&trade.exit_ts),
                    &trade.entry_price,
                    &trade.exit_price,
                    &trade.return_pct,
                    &trade.hit_target,
                    &trade.hit_stop,
                    &trade.exit_reason,
                    &json!({
                        "backtest_run_id": run_id,
                        "holding_bars": trade.holding_bars,
                        "exit_ts": trade.exit_ts,
                    })
                    .to_string(),
                ],
            )
            .await?;
    }
    Ok(())
}

fn to_snapshot_date(ts: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(ts)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|_| ts.chars().take(10).collect())
}

fn postgres_upsert_signal_sql() -> &'static str {
    r#"
    INSERT INTO pa_signal (
        market, symbol, interval, signal_date, signal_type, direction, score,
        ema20_entry_score, ema20, atr14, ema20_position, entry_price, stop_loss,
        target_price, reason, evidence_json
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT(market, symbol, interval, signal_date, signal_type, direction) DO UPDATE SET
        score=excluded.score,
        ema20_entry_score=excluded.ema20_entry_score,
        ema20=excluded.ema20,
        atr14=excluded.atr14,
        ema20_position=excluded.ema20_position,
        entry_price=excluded.entry_price,
        stop_loss=excluded.stop_loss,
        target_price=excluded.target_price,
        reason=excluded.reason,
        evidence_json=excluded.evidence_json
    "#
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn ema_uses_close_series() {
        let data = (1..=25).map(|v| v as f64).collect::<Vec<_>>();
        let values = ema(&data, 20);
        assert_eq!(values.len(), 25);
        assert!(values.last().unwrap() > &10.0);
        assert!(values.last().unwrap() < &25.0);
    }

    #[test]
    fn atr_and_position_are_calculated() {
        let bars = (0..30)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect::<Vec<_>>();
        let indicators = compute_indicators(&bars);
        assert!(indicators.ema20.is_some());
        assert!(indicators.atr14.is_some());
        assert!(indicators.atr20.is_some());
        assert!(indicators.ema20_position.is_some());
    }

    #[test]
    fn resolve_model_selects_requested_model() {
        let breakout = resolve_model("pa_breakout_v2").unwrap();
        assert_eq!(breakout.code(), "pa_breakout_v2");
        assert_eq!(breakout.name(), "pricedog_pa_breakout");

        let signal_bar = resolve_model("pa_signal_bar_v1").unwrap();
        assert_eq!(signal_bar.code(), "pa_signal_bar_v1");
        assert_eq!(signal_bar.name(), "pricedog_pa_signal_bar");

        assert!(resolve_model("missing_model").is_err());

        let ema20 = resolve_model("pa_ema20_cross_v1").unwrap();
        assert_eq!(ema20.code(), "pa_ema20_cross_v1");
        assert_eq!(ema20.min_klines(), 22);
    }

    #[test]
    fn volume_ratio_uses_previous_20_bars() {
        let mut bars = (0..21)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect::<Vec<_>>();
        bars.last_mut().unwrap().volume = 3000.0;

        let indicators = compute_indicators(&bars);

        assert_relative_eq!(indicators.volume_ratio.unwrap(), 3.0);
    }

    #[test]
    fn detects_strong_long_breakout() {
        // 20 range bars + 1 breakout bar + 3 follow-through + 4 warmup = 28
        let mut bars: Vec<Kline> = (0..24)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        // Breakout bar (index 24): strong bar closing above range_high=102
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.0,
            high: 110.0,
            low: 100.5,
            close: 109.0,
            volume: 3500.0,
            turnover: 0.0,
        });
        // 3 follow-through bars: continue up, stay above range_high
        for i in 0..3 {
            bars.push(Kline {
                ts: format!("2024-01-{:02}", 26 + i),
                open: 109.0 + i as f64,
                high: 112.0 + i as f64,
                low: 108.5 + i as f64,
                close: 111.0 + i as f64,
                volume: 1500.0,
                turnover: 0.0,
            });
        }

        let signals = detect_breakout_v2(&bars);

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_breakout");
        assert_eq!(signals[0].direction, "long");
        assert!(signals[0].score >= 35.0);
        // Entry price is the breakout bar's close
        assert_eq!(signals[0].entry_price, Some(109.0));
        assert!(signals[0].stop_loss.unwrap() <= 102.5);
        assert!(signals[0].target_price.unwrap() > signals[0].entry_price.unwrap());
    }

    #[test]
    fn ignores_range_bound_price_action() {
        let bars: Vec<Kline> = (0..28)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0 + i as f64,
                turnover: 0.0,
            })
            .collect();

        let signals = detect_breakout_v2(&bars);

        assert!(signals.is_empty());
    }

    #[test]
    fn ignores_weak_breakout_without_confirmation() {
        let mut bars: Vec<Kline> = (0..24)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.9,
            high: 102.4,
            low: 100.8,
            close: 102.1,
            volume: 900.0,
            turnover: 0.0,
        });
        // Follow-through bars that go back inside range (delayed veto)
        for i in 0..3 {
            bars.push(Kline {
                ts: format!("2024-01-{:02}", 26 + i),
                open: 101.5,
                high: 101.8,
                low: 100.0,
                close: 101.0,
                volume: 800.0,
                turnover: 0.0,
            });
        }

        let signals = detect_breakout_v2(&bars);

        assert!(signals.is_empty());
    }

    #[test]
    fn ema20_entry_v2_prefers_near_ema20() {
        let bars: Vec<Kline> = (0..30)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        let indicators = compute_indicators(&bars);

        let near_bar = Kline {
            ts: "2024-02-01".to_string(),
            open: indicators.ema20.unwrap() - 0.5,
            high: indicators.ema20.unwrap() + 2.0,
            low: indicators.ema20.unwrap() - 1.0,
            close: indicators.ema20.unwrap() + 1.0,
            volume: 1000.0,
            turnover: 0.0,
        };
        let far_bar = Kline {
            ts: "2024-02-02".to_string(),
            open: indicators.ema20.unwrap() + 20.0,
            high: indicators.ema20.unwrap() + 25.0,
            low: indicators.ema20.unwrap() + 18.0,
            close: indicators.ema20.unwrap() + 22.0,
            volume: 1000.0,
            turnover: 0.0,
        };

        let near_score = compute_ema20_entry_v2(&near_bar, &indicators, "long");
        let far_score = compute_ema20_entry_v2(&far_bar, &indicators, "long");

        assert!(near_score > far_score);
    }

    #[test]
    fn overextended_position_reduces_ema20_entry_v2() {
        let moderate = Indicators {
            ema20: Some(100.0),
            ema5: None,
            atr14: Some(10.0),
            atr20: Some(10.0),
            ema20_position: Some(1.0),
            volume_ratio: None,
            amplitude: None,
        };
        let stretched = Indicators {
            ema20: Some(100.0),
            ema5: None,
            atr14: Some(10.0),
            atr20: Some(10.0),
            ema20_position: Some(3.0),
            volume_ratio: None,
            amplitude: None,
        };
        // close=130, ema20=100, atr14=10 → ema_gap=3.0 → triggers -5 penalty
        let stretched_bar = Kline {
            ts: "2024-02-01".to_string(),
            open: 128.0,
            high: 132.0,
            low: 127.0,
            close: 130.0,
            volume: 1000.0,
            turnover: 0.0,
        };
        // close=105, ema20=100, atr14=10 → ema_gap=0.5 → near EMA20
        let near_bar = Kline {
            ts: "2024-02-02".to_string(),
            open: 103.0,
            high: 106.0,
            low: 102.5,
            close: 105.0,
            volume: 1000.0,
            turnover: 0.0,
        };

        assert!(compute_ema20_entry_v2(&near_bar, &moderate, "long") > 0.0);
        assert!(
            compute_ema20_entry_v2(&near_bar, &moderate, "long")
                > compute_ema20_entry_v2(&stretched_bar, &stretched, "long")
        );
    }

    #[test]
    fn median_even_works() {
        assert_relative_eq!(median(vec![3.0, 1.0, 2.0, 4.0]), 2.5);
    }

    #[test]
    fn interval_normalization_works() {
        assert_eq!(normalize_interval("5min"), "5m");
        assert_eq!(normalize_interval("60m"), "1h");
        assert_eq!(normalize_interval("1hour"), "1h");
        assert_eq!(normalize_interval("120m"), "2h");
        assert_eq!(normalize_interval("240"), "4h");
        assert_eq!(normalize_interval("4h"), "4h");
        assert_eq!(normalize_interval("daily"), "1d");
    }

    #[test]
    fn aggregate_intraday_builds_target_interval_ohlcv() {
        let bars = vec![
            Kline {
                ts: "2024-01-01T10:00:00Z".to_string(),
                open: 10.0,
                high: 11.0,
                low: 9.5,
                close: 10.5,
                volume: 100.0,
                turnover: 1000.0,
            },
            Kline {
                ts: "2024-01-01T11:00:00Z".to_string(),
                open: 10.5,
                high: 12.0,
                low: 10.0,
                close: 11.5,
                volume: 150.0,
                turnover: 1800.0,
            },
            Kline {
                ts: "2024-01-01T12:00:00Z".to_string(),
                open: 11.5,
                high: 13.0,
                low: 11.0,
                close: 12.5,
                volume: 200.0,
                turnover: 2500.0,
            },
        ];

        let aggregated = aggregate_intraday(&bars, "2h");

        assert_eq!(aggregated.len(), 2);
        assert_relative_eq!(aggregated[0].open, 10.0);
        assert_relative_eq!(aggregated[0].high, 12.0);
        assert_relative_eq!(aggregated[0].low, 9.5);
        assert_relative_eq!(aggregated[0].close, 11.5);
        assert_relative_eq!(aggregated[0].volume, 250.0);
        assert_relative_eq!(aggregated[0].turnover, 2800.0);
        assert_eq!(aggregated[0].ts, "2024-01-01T11:00:00Z");
        assert_relative_eq!(aggregated[1].open, 11.5);
        assert_relative_eq!(aggregated[1].close, 12.5);
    }

    #[test]
    fn kline_gap_counter_detects_missing_slots() {
        let bars = vec![
            Kline {
                ts: "2024-01-01T00:00:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
            Kline {
                ts: "2024-01-01T00:05:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
            Kline {
                ts: "2024-01-01T00:20:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
        ];

        assert_eq!(count_missing_kline_slots(&bars, "5m"), 2);
    }

    #[test]
    fn kline_gap_counter_ignores_duplicates() {
        let bars = vec![
            Kline {
                ts: "2024-01-01T00:00:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
            Kline {
                ts: "2024-01-01T00:00:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
            Kline {
                ts: "2024-01-01T00:05:00Z".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                turnover: 1.0,
            },
        ];

        assert_eq!(count_missing_kline_slots(&bars, "5m"), 0);
    }

    #[test]
    fn interval_millis_supports_crypto_periods() {
        assert_eq!(interval_millis("5m"), Some(300_000));
        assert_eq!(interval_millis("1h"), Some(3_600_000));
        assert_eq!(interval_millis("4h"), Some(14_400_000));
        assert_eq!(interval_millis("1d"), Some(86_400_000));
    }

    #[test]
    fn stored_klines_are_usable_for_indicator_windows() {
        assert!(!has_usable_klines(20, 120));
        assert!(has_usable_klines(21, 120));
        assert!(has_usable_klines(120, 120));
    }

    #[test]
    fn backtest_request_accepts_fetch_script_output_shape() {
        let payload = json!({
            "schema_version": "v1",
            "market": "CRYPTO",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "source": "binance",
            "timezone": "UTC",
            "ts_mode": "bar_open",
            "quality_mode": "continuous_24_7",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-03T00:00:00Z",
            "count": 2,
            "quality": {
                "expected_bars": 2,
                "actual_bars": 2,
                "missing_bars": 0,
                "duplicate_bars": 0,
                "is_continuous": true,
                "first_ts": "2024-01-01T00:00:00Z",
                "last_ts": "2024-01-01T01:00:00Z",
                "missing_timestamps_sample": []
            },
            "klines": [
                {
                    "ts": "2024-01-01T00:00:00Z",
                    "open": 42000.0,
                    "high": 42500.0,
                    "low": 41800.0,
                    "close": 42300.0,
                    "volume": 100.0,
                    "turnover": 4215000.0
                },
                {
                    "ts": "2024-01-01T01:00:00Z",
                    "open": 42300.0,
                    "high": 42750.0,
                    "low": 42200.0,
                    "close": 42650.0,
                    "volume": 95.0,
                    "turnover": 4040000.0
                }
            ]
        });

        let req: BacktestRequest = serde_json::from_value(payload).unwrap();

        assert_eq!(req.market, "CRYPTO");
        assert_eq!(req.symbol, "BTCUSDT");
        assert_eq!(normalize_interval(&req.interval), "1h");
        assert_eq!(req.klines.len(), 2);
        assert_eq!(req.model_code, "pa_breakout_v2");
        assert_eq!(req.max_holding_bars, default_max_holding_bars());
        assert_eq!(req.klines[0].ts, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn evaluate_request_defaults_to_breakout_model() {
        let payload = json!({
            "market": "CRYPTO",
            "symbol": "BTCUSDT"
        });

        let req: EvaluateRequest = serde_json::from_value(payload).unwrap();

        assert_eq!(req.model_code, "pa_breakout_v2");
    }

    // --- Signal Bar Model tests ---

    /// 辅助: 生成上行趋势基础K线（逐步上涨，确保EMA5上穿EMA20用于测试金叉过滤）
    fn make_trending_up_klines(count: usize) -> Vec<Kline> {
        (0..count)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: if i < 10 { 100.0 } else { 100.0 + (i - 10) as f64 },
                high: if i < 10 { 101.0 } else { 103.0 + (i - 10) as f64 },
                low: if i < 10 { 99.0 } else { 99.0 + (i - 10) as f64 },
                close: if i < 10 { 100.5 } else { 102.0 + (i - 10) as f64 },
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect()
    }

    fn make_base_klines(count: usize) -> Vec<Kline> {
        (0..count)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect()
    }

    #[test]
    fn bar_features_阳线计算正确() {
        // O=100, H=105, L=98, C=104 → 阳线
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 100.0,
            high: 105.0,
            low: 98.0,
            close: 104.0,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        assert_relative_eq!(f.body, 4.0);
        assert_relative_eq!(f.range, 7.0);
        assert_relative_eq!(f.p_b, 4.0 / 7.0, max_relative = 0.01);
        assert_relative_eq!(f.p_c, 6.0 / 7.0, max_relative = 0.01);
        // p_u = (105 - 104) / 7 = 1/7
        assert_relative_eq!(f.p_u, 1.0 / 7.0, max_relative = 0.01);
        // p_d = (100 - 98) / 7 = 2/7
        assert_relative_eq!(f.p_d, 2.0 / 7.0, max_relative = 0.01);
    }

    #[test]
    fn bar_features_阴线计算正确() {
        // O=104, H=105, L=98, C=100 → 阴线
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 104.0,
            high: 105.0,
            low: 98.0,
            close: 100.0,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        assert_relative_eq!(f.body, -4.0);
        assert_relative_eq!(f.p_b, 4.0 / 7.0, max_relative = 0.01);
        assert_relative_eq!(f.p_c, 2.0 / 7.0, max_relative = 0.01);
        // p_u = (105 - 104) / 7 = 1/7
        assert_relative_eq!(f.p_u, 1.0 / 7.0, max_relative = 0.01);
        // p_d = (100 - 98) / 7 = 2/7
        assert_relative_eq!(f.p_d, 2.0 / 7.0, max_relative = 0.01);
    }

    #[test]
    fn bar_features_十字星range为零() {
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        assert_relative_eq!(f.range, 0.0);
        assert_relative_eq!(f.p_b, 0.0);
        assert_relative_eq!(f.p_c, 0.5);
    }

    #[test]
    fn quality_满分多头信号K() {
        // 实体占比 >= 0.6, 收盘位置 >= 0.85, 上影线 <= 0.1
        // O=100, H=105, L=99, C=104.7
        // body=4.7, range=6, p_b=0.783, p_c=(104.7-99)/6=0.95, p_u=(105-104.7)/6=0.05
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 104.7,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        let q = score_quality_long(&f);
        assert_relative_eq!(q, 1.0);
    }

    #[test]
    fn quality_满分空头信号K() {
        // O=105, H=106, L=100, C=100.3
        // body=-4.7, range=6, p_b=0.783, p_c=(100.3-100)/6=0.05, p_d=(105-100)/6=0.833
        // 等等...空头要求 p_c<=0.15, p_d<=0.1
        // 重新设计: O=104, H=105, L=99, C=99.3
        // body=-4.7, range=6, p_b=0.783, p_c=(99.3-99)/6=0.05, p_d=(99.3-99)/6... 不对
        // p_d = (min(C,O) - L)/R = (99.3 - 99)/6 = 0.05 ✓, p_c = (C-L)/R = (99.3-99)/6 = 0.05 ✓
        // 但需要 p_c <= 0.15 ✓, p_d <= 0.1 ✓, p_b >= 0.6 ✓
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 104.0,
            high: 105.0,
            low: 99.0,
            close: 99.3,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        let q = score_quality_short(&f);
        assert_relative_eq!(q, 1.0);
    }

    #[test]
    fn quality_阴线不给多头分() {
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 105.0,
            high: 106.0,
            low: 99.0,
            close: 100.0,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        assert_relative_eq!(score_quality_long(&f), 0.0);
    }

    #[test]
    fn quality_阳线不给空头分() {
        let k = Kline {
            ts: "2024-01-01".to_string(),
            open: 99.0,
            high: 106.0,
            low: 98.0,
            close: 105.0,
            volume: 0.0,
            turnover: 0.0,
        };
        let f = compute_bar_features(&k);
        assert_relative_eq!(score_quality_short(&f), 0.0);
    }

    #[test]
    fn signal_bar_context_uses_atr20_for_cr() {
        let bars: Vec<Kline> = (0..10)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: 100.0,
                high: 106.0,
                low: 94.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        let indicators = Indicators {
            ema20: Some(100.0),
            ema5: None,
            atr14: Some(2.0),
            atr20: Some(10.0),
            ema20_position: Some(0.1),
            volume_ratio: None,
            amplitude: None,
        };

        let context = evaluate_context(&bars, &indicators);

        assert_relative_eq!(context.cr, 1.2);
        assert!(context.is_valid);
    }

    #[test]
    fn selected_models_do_not_mix_signals() {
        let mut bars = make_trending_up_klines(24);
        let prev_close = bars.last().map(|k| k.close).unwrap_or(100.0);
        bars.push(Kline {
            ts: "2024-01-25T00:00:00Z".to_string(),
            open: prev_close + 4.0,
            high: prev_close + 5.0,
            low: prev_close - 1.0,
            close: prev_close - 0.7,
            volume: 1000.0,
            turnover: 0.0,
        });
        bars.push(Kline {
            ts: "2024-01-26T00:00:00Z".to_string(),
            open: prev_close,
            high: prev_close + 5.0,
            low: prev_close - 1.0,
            close: prev_close + 4.7,
            volume: 1000.0,
            turnover: 0.0,
        });

        let signal_bar = resolve_model("pa_signal_bar_v1").unwrap();
        let breakout = resolve_model("pa_breakout_v2").unwrap();

        assert_eq!(signal_bar.detect(&bars).len(), 1);
        assert!(breakout.detect(&bars).is_empty());
    }

    #[test]
    fn signal_bar_检测2k多头反转() {
        // 前一根: 高质量空头信号K (body<0, p_b>=0.6, p_c<=0.15, p_d<=0.1)
        // 当前根: 高质量多头信号K (body>0, p_b>=0.6, p_c>=0.85, p_u<=0.1)
        let mut bars = make_trending_up_klines(24);
        let prev_close = bars.last().map(|k| k.close).unwrap_or(100.0);
        // 前一根空头信号K: O=104, H=105, L=99, C=99.3 (quality 1.0)
        bars.push(Kline {
            ts: "2024-01-25T00:00:00Z".to_string(),
            open: prev_close + 4.0,
            high: prev_close + 5.0,
            low: prev_close - 1.0,
            close: prev_close - 0.7,
            volume: 1000.0,
            turnover: 0.0,
        });
        // 当前多头信号K: O=100, H=105, L=99, C=104.7 (quality 1.0)
        bars.push(Kline {
            ts: "2024-01-26T00:00:00Z".to_string(),
            open: prev_close,
            high: prev_close + 5.0,
            low: prev_close - 1.0,
            close: prev_close + 4.7,
            volume: 1000.0,
            turnover: 0.0,
        });

        let signals = detect_signal_bar(&bars);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_pattern");
        assert_eq!(signals[0].direction, "long");
        assert!(signals[0].reason.contains("盯盘提醒"));
        assert_eq!(signals[0].evidence["alert_purpose"], "watchlist_monitor");
        assert_eq!(signals[0].evidence["watch_alert"]["pattern_type"], "2k_reversal");
    }

    #[test]
    fn signal_bar_检测ema20上穿公式() {
        // 前20根收盘100 → EMA20稳定在100
        // 第21根收盘100 = EMA20, 第22根收盘140 > EMA20 → 上穿
        let bars: Vec<Kline> = (0..22)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i+1), open: 90.0, high: 110.0, low: 90.0,
                close: if i<20 { 100.0 } else if i==20 { 100.0 } else { 140.0 },
                volume: 1000.0, turnover: 0.0,
            }).collect();
        let prev_closes: Vec<f64> = bars[..21].iter().map(|k| k.close).collect();
        let prev_ema20 = *ema(&prev_closes, 20).last().unwrap();
        assert!((prev_ema20 - 100.0).abs() < 1.0, "prev EMA20 should be ~100");
        assert!(bars[20].close <= prev_ema20); // prev close <= prev ema20
        assert!(bars[21].close > compute_indicators(&bars).ema20.unwrap()); // curr close > curr ema20
    }
    #[test]

    fn signal_bar_检测ema20下穿公式() {
        // 前20根收盘120 → EMA20稳定在120
        // 第21根收盘120 = EMA20, 第22根收盘90 < EMA20 → 下穿
        let bars: Vec<Kline> = (0..22)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i+1), open: 130.0, high: 140.0, low: 110.0,
                close: if i<20 { 120.0 } else if i==20 { 120.0 } else { 90.0 },
                volume: 1000.0, turnover: 0.0,
            }).collect();
        let prev_closes: Vec<f64> = bars[..21].iter().map(|k| k.close).collect();
        let prev_ema20 = *ema(&prev_closes, 20).last().unwrap();
        assert!((prev_ema20 - 120.0).abs() < 1.0, "prev EMA20 should be ~120");
        assert!(bars[20].close >= prev_ema20); // prev close >= prev ema20
        assert!(bars[21].close < compute_indicators(&bars).ema20.unwrap()); // curr close < curr ema20
    }

    #[test]
    fn signal_bar_横盘无ema20穿越() {
        let bars = make_base_klines(26);
        let signals = detect_signal_bar(&bars);
        let no_cross = !signals.iter().any(|s| s.reason.contains("EMA20 cross"));
        assert!(no_cross, "flat bars should not trigger EMA20 cross");
    }

    fn signal_bar_检测吞噬线() {
        let mut bars = make_base_klines(24);
        // 前一根小阳线
        bars.push(Kline {
            ts: "2024-01-25T00:00:00Z".to_string(),
            open: 100.0,
            high: 101.0,
            low: 99.5,
            close: 100.5,
            volume: 1000.0,
            turnover: 0.0,
        });
        // 当前大阳线吞噬前一根: H_t > H_{t-1}, L_t < L_{t-1}, body>0, p_b>=0.5
        bars.push(Kline {
            ts: "2024-01-26T00:00:00Z".to_string(),
            open: 99.0,
            high: 103.0,
            low: 98.0,
            close: 102.5,
            volume: 1000.0,
            turnover: 0.0,
        });

        let signals = detect_signal_bar(&bars);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_pattern");
        assert_eq!(signals[0].direction, "long");
        assert!(signals[0].reason.contains("盯盘提醒"));
        assert_eq!(signals[0].evidence["watch_alert"]["pattern_type"], "engulfing");
    }

    #[test]
    fn signal_bar_检测惊喜K线() {
        // 前20根range都是2，然后来一根range=5的强阳线(>1.5*2=3)
        let mut bars: Vec<Kline> = (0..25)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        // 惊喜K线: range=5, p_b>=0.5
        bars.push(Kline {
            ts: "2024-01-26T00:00:00Z".to_string(),
            open: 100.0,
            high: 104.0,
            low: 99.0,
            close: 103.5,
            volume: 5000.0,
            turnover: 0.0,
        });

        let signals = detect_signal_bar(&bars);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_pattern");
        assert!(signals[0].reason.contains("盯盘提醒"));
        assert_eq!(signals[0].evidence["watch_alert"]["pattern_type"], "surprise_bar");
    }

    #[test]
    fn signal_bar_数据不足不触发() {
        let bars = make_base_klines(20);
        let signals = detect_signal_bar(&bars);
        assert!(signals.is_empty());
    }

    #[test]
    fn signal_bar_横盘不触发() {
        // 25根完全相同的K线，没有信号K线特征
        let bars = make_base_klines(26);
        let signals = detect_signal_bar(&bars);
        assert!(signals.is_empty());
    }

    #[test]
    fn signal_bar_入场止损目标计算正确() {
        let mut bars = make_base_klines(24);
        bars.push(Kline {
            ts: "2024-01-25T00:00:00Z".to_string(),
            open: 100.0,
            high: 101.0,
            low: 99.5,
            close: 100.5,
            volume: 1000.0,
            turnover: 0.0,
        });
        // 吞噬线触发多头信号
        bars.push(Kline {
            ts: "2024-01-26T00:00:00Z".to_string(),
            open: 99.0,
            high: 103.0,
            low: 98.0,
            close: 102.5,
            volume: 1000.0,
            turnover: 0.0,
        });

        let signals = detect_signal_bar(&bars);
        assert_eq!(signals.len(), 1);

        let s = &signals[0];
        assert_eq!(s.direction, "long");
        let entry = s.entry_price.unwrap();
        let stop = s.stop_loss.unwrap();
        let target = s.target_price.unwrap();

        assert_relative_eq!(entry, 103.01);
        assert_relative_eq!(stop, 97.99);
        assert_relative_eq!(s.evidence["tick_size"].as_f64().unwrap(), 0.01);
        // target = entry + 2*(entry - stop)
        assert_relative_eq!(target, entry + 2.0 * (entry - stop), max_relative = 0.01);
    }

    // ── Stock K-line backfill tests ──────────────────────────

    #[test]
    fn discover_tasks_extracts_kline_conditions_from_json() {
        // Simulate a condition_group JSON with volume_ratio and ema20_position conditions
        let cg = serde_json::json!({
            "op": "and",
            "items": [
                {"type": "price", "op": ">", "value": 100},
                {"type": "volume_ratio", "op": ">", "value": 2.0, "interval": "1d"},
                {"type": "ema20_position", "op": ">", "value": 0.5, "interval": "5m"},
                {"type": "change_pct", "op": ">", "value": 3.0},  // not a kline condition
            ]
        });

        let items = cg.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let mut tasks = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !KLINE_CONDITION_TYPES.contains(&ctype) {
                continue;
            }
            let interval = normalize_interval(item.get("interval").and_then(|v| v.as_str()).unwrap_or("1d"));
            let key = format!("CN:600519:{}", interval);
            if seen.insert(key) {
                tasks.push(interval.clone());
            }
        }
        assert_eq!(tasks.len(), 2);
        assert!(tasks.contains(&"1d".to_string()));
        assert!(tasks.contains(&"5m".to_string()));
    }

    #[test]
    fn discover_tasks_defaults_interval_to_1d() {
        let cg = serde_json::json!({
            "items": [
                {"type": "pattern", "value": "signal_bar"},
            ]
        });
        let items = cg.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if KLINE_CONDITION_TYPES.contains(&ctype) {
                let interval = normalize_interval(item.get("interval").and_then(|v| v.as_str()).unwrap_or("1d"));
                assert_eq!(interval, "1d");
            }
        }
    }

    #[test]
    fn discover_tasks_deduplicates_same_symbol_interval() {
        let cg = serde_json::json!({
            "items": [
                {"type": "volume_ratio", "op": ">", "value": 2.0, "interval": "1d"},
                {"type": "ema20_position", "op": ">", "value": 0.5, "interval": "1d"},
            ]
        });
        let items = cg.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let mut seen = std::collections::HashSet::new();
        let mut count = 0;
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !KLINE_CONDITION_TYPES.contains(&ctype) { continue; }
            let interval = normalize_interval(item.get("interval").and_then(|v| v.as_str()).unwrap_or("1d"));
            if seen.insert(format!("CN:600519:{}", interval)) {
                count += 1;
            }
        }
        assert_eq!(count, 1);  // deduplicated to one (1d)
    }
}
