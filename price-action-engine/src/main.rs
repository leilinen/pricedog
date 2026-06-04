mod backtest;
mod indicators;
mod model;
mod models;
mod types;

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
use serde_json::{json, Value};
use tokio::{sync::RwLock, time::sleep};
use tokio_postgres::{Client as PgClient, NoTls, Row};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower_http::cors::CorsLayer;
use tracing::{debug, info, warn};

use backtest::*;
use indicators::{
    aggregate_intraday, compute_indicators, interval_millis, normalize_interval, parse_f64,
    parse_ts_millis,
};
use model::resolve_model;
use models::signal_bar::detect_signal_bar;
use types::*;

#[derive(Clone)]
struct AppState {
    client: Client,
    db_url: String,
    kline_cache: Cache<String, Vec<Kline>>,
    quote_cache: Cache<String, Quote>,
    data_provider_url: String,
    provider_status: Arc<RwLock<HashMap<String, ProviderStatus>>>,
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
        let cg_json: serde_json::Value =
            serde_json::from_str(&cg_str).unwrap_or(serde_json::Value::Null);

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

    info!(
        "stock kline backfill starting: poll={}s lookback={}",
        poll_secs, lookback
    );

    tokio::spawn(async move {
        loop {
            if let Err(err) = run_stock_kline_backfill_once(&state, lookback).await {
                warn!("stock kline backfill failed: {err}");
            }
            sleep(Duration::from_secs(poll_secs)).await;
        }
    });
}

async fn run_stock_kline_backfill_once(state: &AppState, lookback: usize) -> Result<()> {
    let tasks = discover_stock_kline_tasks(state).await?;
    if tasks.is_empty() {
        return Ok(());
    }

    mark_provider_connected(state, "data-provider", "STOCK", "stock_backfill").await;
    let mut updated = 0usize;
    for task in &tasks {
        // Check if we have fresh enough data already
        let stored =
            load_klines_from_db(state, &task.market, &task.symbol, &task.interval, lookback)
                .await?;
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

        match fetch_stock_klines(
            state,
            &task.market,
            &task.symbol,
            &task.interval,
            lookback + 2,
        )
        .await
        {
            Ok(fetch) => {
                if fetch.klines.is_empty() {
                    continue;
                }
                persist_klines(
                    state,
                    &task.market,
                    &task.symbol,
                    &task.interval,
                    &fetch.klines,
                    &fetch.source,
                )
                .await?;
                updated += fetch.klines.len();
                info!(
                    "stock kline backfill {} {} {} rows={}",
                    task.market,
                    task.symbol,
                    task.interval,
                    fetch.klines.len()
                );
            }
            Err(err) => {
                warn!(
                    "stock kline backfill {} {} {} failed: {}",
                    task.market, task.symbol, task.interval, err
                );
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
    let url = format!(
        "{}/api/v1/quote/{}/{}",
        state.data_provider_url, market, symbol
    );
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
    use indicators::ema;
    use models::breakout_v2::{
        compute_ema20_entry_v2, detect_breakout_v2,
    };
    use models::signal_bar::{
        compute_bar_features, evaluate_context, score_quality_long, score_quality_short,
    };

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
                open: if i < 10 {
                    100.0
                } else {
                    100.0 + (i - 10) as f64
                },
                high: if i < 10 {
                    101.0
                } else {
                    103.0 + (i - 10) as f64
                },
                low: if i < 10 { 99.0 } else { 99.0 + (i - 10) as f64 },
                close: if i < 10 {
                    100.5
                } else {
                    102.0 + (i - 10) as f64
                },
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
        assert_eq!(
            signals[0].evidence["watch_alert"]["pattern_type"],
            "2k_reversal"
        );
    }

    #[test]
    fn signal_bar_检测ema20上穿公式() {
        // 前20根收盘100 → EMA20稳定在100
        // 第21根收盘100 = EMA20, 第22根收盘140 > EMA20 → 上穿
        let bars: Vec<Kline> = (0..22)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: 90.0,
                high: 110.0,
                low: 90.0,
                close: if i < 20 {
                    100.0
                } else if i == 20 {
                    100.0
                } else {
                    140.0
                },
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        let prev_closes: Vec<f64> = bars[..21].iter().map(|k| k.close).collect();
        let prev_ema20 = *ema(&prev_closes, 20).last().unwrap();
        assert!(
            (prev_ema20 - 100.0).abs() < 1.0,
            "prev EMA20 should be ~100"
        );
        assert!(bars[20].close <= prev_ema20); // prev close <= prev ema20
        assert!(bars[21].close > compute_indicators(&bars).ema20.unwrap()); // curr close > curr ema20
    }
    #[test]

    fn signal_bar_检测ema20下穿公式() {
        // 前20根收盘120 → EMA20稳定在120
        // 第21根收盘120 = EMA20, 第22根收盘90 < EMA20 → 下穿
        let bars: Vec<Kline> = (0..22)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}T00:00:00Z", i + 1),
                open: 130.0,
                high: 140.0,
                low: 110.0,
                close: if i < 20 {
                    120.0
                } else if i == 20 {
                    120.0
                } else {
                    90.0
                },
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        let prev_closes: Vec<f64> = bars[..21].iter().map(|k| k.close).collect();
        let prev_ema20 = *ema(&prev_closes, 20).last().unwrap();
        assert!(
            (prev_ema20 - 120.0).abs() < 1.0,
            "prev EMA20 should be ~120"
        );
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
        assert_eq!(
            signals[0].evidence["watch_alert"]["pattern_type"],
            "engulfing"
        );
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
        assert_eq!(
            signals[0].evidence["watch_alert"]["pattern_type"],
            "surprise_bar"
        );
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

        let items = cg
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut tasks = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !KLINE_CONDITION_TYPES.contains(&ctype) {
                continue;
            }
            let interval = normalize_interval(
                item.get("interval")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1d"),
            );
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
        let items = cg
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if KLINE_CONDITION_TYPES.contains(&ctype) {
                let interval = normalize_interval(
                    item.get("interval")
                        .and_then(|v| v.as_str())
                        .unwrap_or("1d"),
                );
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
        let items = cg
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut seen = std::collections::HashSet::new();
        let mut count = 0;
        for item in &items {
            let ctype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !KLINE_CONDITION_TYPES.contains(&ctype) {
                continue;
            }
            let interval = normalize_interval(
                item.get("interval")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1d"),
            );
            if seen.insert(format!("CN:600519:{}", interval)) {
                count += 1;
            }
        }
        assert_eq!(count, 1); // deduplicated to one (1d)
    }
}
