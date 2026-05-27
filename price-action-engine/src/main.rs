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
    akshare_base_url: String,
    provider_status: Arc<RwLock<HashMap<String, ProviderStatus>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Kline {
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
    atr14: Option<f64>,
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

#[derive(Debug, Deserialize)]
struct KlineQuery {
    interval: Option<String>,
    limit: Option<usize>,
    refresh: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct EvaluateRequest {
    market: String,
    symbol: String,
    #[serde(default = "default_interval")]
    interval: String,
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
        akshare_base_url: env::var("AKSHARE_ADAPTER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8002".to_string())
            .trim_end_matches('/')
            .to_string(),
        provider_status: Arc::new(RwLock::new(HashMap::new())),
    });
    start_crypto_ws_collectors(state.clone());

    let app = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/provider-status", get(get_provider_status))
        .route("/api/v1/klines/:market/:symbol", get(get_klines))
        .route("/api/v1/quote/:market/:symbol", get(get_quote))
        .route("/api/v1/evaluate", post(evaluate))
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
    limit: usize,
    input_klines: Vec<Kline>,
    refresh: bool,
    persist_signal: bool,
) -> Result<Value> {
    let interval = normalize_interval(interval);
    let mut klines = if input_klines.is_empty() {
        fetch_klines(state, market, symbol, &interval, limit, refresh).await?
    } else {
        input_klines
    };
    klines.sort_by(|a, b| a.ts.cmp(&b.ts));
    if klines.len() < 21 {
        return Err(anyhow!("need at least 21 klines"));
    }
    let indicators = compute_indicators(&klines);
    let signals = detect_breakout(&klines, &indicators);
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
        "indicators": indicators,
        "signals": signals,
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
        "{}/klines/{}/{}?interval={}&limit={}",
        state.akshare_base_url, market, symbol, interval, limit
    );
    let payload = get_json_with_retry(&state.client, &url).await?;
    if !payload
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Err(anyhow!(payload
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("akshare error")
            .to_string()));
    }
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow!("missing akshare data"))?;
    let source = data
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("akshare")
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
    let url = format!("{}/quote/{}/{}", state.akshare_base_url, market, symbol);
    let payload = get_json_with_retry(&state.client, &url).await?;
    if !payload
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Err(anyhow!(payload
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("akshare quote error")
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
    let ema = ema(&closes, 20);
    let atr = atr(klines, 14);
    let ema20 = ema.last().copied();
    let atr14 = atr.last().copied().flatten();
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
        atr14,
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

fn detect_breakout(klines: &[Kline], indicators: &Indicators) -> Vec<Signal> {
    if klines.len() < 25 {
        return Vec::new();
    }
    let curr = klines.last().unwrap();
    let prev_window = &klines[klines.len() - 21..klines.len() - 1];
    let range_high = prev_window.iter().map(|k| k.high).fold(f64::MIN, f64::max);
    let range_low = prev_window.iter().map(|k| k.low).fold(f64::MAX, f64::min);
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
    let atr14 = indicators.atr14.unwrap_or(0.0);

    let (direction, breakout_distance, strong_close) = if curr.close > range_high {
        ("long", curr.close - range_high, close_location >= 0.75)
    } else if curr.close < range_low {
        ("short", range_low - curr.close, close_location <= 0.25)
    } else {
        return Vec::new();
    };

    let distance_atr = if atr14 > 0.0 {
        breakout_distance / atr14
    } else {
        0.0
    };
    let ema_score = ema20_entry_score(curr, indicators, direction);
    let mut score = 0.0;
    if body_ratio >= 2.0 {
        score += 15.0;
    }
    if body_ratio >= 5.0 {
        score += 10.0;
    }
    if strong_close {
        score += 20.0;
    }
    if distance_atr >= 0.3 {
        score += 15.0;
    }
    if distance_atr >= 0.8 {
        score += 10.0;
    }
    if indicators.volume_ratio.unwrap_or(0.0) >= 1.5 {
        score += 10.0;
    }
    score += ema_score.min(15.0);

    if score < 45.0 {
        return Vec::new();
    }

    let entry = Some(curr.close);
    let stop = if direction == "long" {
        Some(curr.low.min(range_high))
    } else {
        Some(curr.high.max(range_low))
    };
    let target = match (entry, stop) {
        (Some(e), Some(s)) if direction == "long" => Some(e + (e - s).abs() * 2.0),
        (Some(e), Some(s)) => Some(e - (e - s).abs() * 2.0),
        _ => None,
    };
    vec![Signal {
        signal_type: "pa_breakout".to_string(),
        direction: direction.to_string(),
        score,
        ema20_entry_score: ema_score,
        entry_price: entry,
        stop_loss: stop,
        target_price: target,
        reason: format!(
            "{} breakout: body_ratio={:.2}, distance_atr={:.2}",
            direction, body_ratio, distance_atr
        ),
        evidence: json!({
            "range_high": range_high,
            "range_low": range_low,
            "body_ratio": body_ratio,
            "close_location": close_location,
            "distance_atr": distance_atr,
            "volume_ratio": indicators.volume_ratio,
            "ema20_position": indicators.ema20_position,
        }),
    }]
}

fn ema20_entry_score(curr: &Kline, indicators: &Indicators, direction: &str) -> f64 {
    let Some(ema20) = indicators.ema20 else {
        return 0.0;
    };
    let mut score = 0.0;
    if direction == "long" && curr.close > ema20 {
        score += 4.0;
    }
    if direction == "short" && curr.close < ema20 {
        score += 4.0;
    }
    if let Some(pos) = indicators.ema20_position {
        if direction == "long" && pos > 0.0 {
            score += 3.0;
        }
        if direction == "short" && pos < 0.0 {
            score += 3.0;
        }
        if pos.abs() <= 1.5 {
            score += 2.0;
        }
    }
    score
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
        UNIQUE(market, symbol, interval, signal_date, signal_type)
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

fn postgres_upsert_signal_sql() -> &'static str {
    r#"
    INSERT INTO pa_signal (
        market, symbol, interval, signal_date, signal_type, direction, score,
        ema20_entry_score, ema20, atr14, ema20_position, entry_price, stop_loss,
        target_price, reason, evidence_json
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT(market, symbol, interval, signal_date, signal_type) DO UPDATE SET
        direction=excluded.direction,
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
        assert!(indicators.ema20_position.is_some());
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
        let mut bars = (0..24)
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
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.0,
            high: 110.0,
            low: 100.0,
            close: 109.0,
            volume: 3500.0,
            turnover: 0.0,
        });

        let indicators = compute_indicators(&bars);
        let signals = detect_breakout(&bars, &indicators);

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_breakout");
        assert_eq!(signals[0].direction, "long");
        assert!(signals[0].score >= 45.0);
        assert_eq!(signals[0].entry_price, Some(109.0));
        assert!(signals[0].stop_loss.unwrap() <= 102.0);
        assert!(signals[0].target_price.unwrap() > 109.0);
    }

    #[test]
    fn ignores_range_bound_price_action() {
        let bars = (0..25)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0 + i as f64,
                turnover: 0.0,
            })
            .collect::<Vec<_>>();

        let indicators = compute_indicators(&bars);
        let signals = detect_breakout(&bars, &indicators);

        assert!(signals.is_empty());
    }

    #[test]
    fn ignores_weak_breakout_without_confirmation() {
        let mut bars = (0..24)
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
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.9,
            high: 102.4,
            low: 100.8,
            close: 102.1,
            volume: 900.0,
            turnover: 0.0,
        });

        let indicators = compute_indicators(&bars);
        let signals = detect_breakout(&bars, &indicators);

        assert!(signals.is_empty());
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
    fn stored_klines_are_usable_for_indicator_windows() {
        assert!(!has_usable_klines(20, 120));
        assert!(has_usable_klines(21, 120));
        assert!(has_usable_klines(120, 120));
    }
}
