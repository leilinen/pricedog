use crate::error::{err_json, ok_json};
use crate::models::market::{tencent_symbol, MarketCode};
use crate::models::quote::{BatchQuoteRequest, Quote};
use crate::providers::quote::TencentQuoteProvider;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

/// GET /api/v1/quote/:market/:symbol
pub async fn get_quote(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
) -> Response {
    let mk = match MarketCode::from_str(&market) {
        Some(m) => m,
        None => return err_json(axum::http::StatusCode::BAD_REQUEST, "invalid market"),
    };

    let cache_key = format!("{}:{}", market.to_uppercase(), symbol);
    if let Some(cached) = state.quote_cache.get(&cache_key) {
        return ok_json(&cached).into_response();
    }

    let provider = TencentQuoteProvider::new(state.config.http_proxy.clone());
    let tencent_sym = tencent_symbol(&symbol, &mk);
    match provider.fetch(&[tencent_sym], &market).await {
        Ok(mut quotes) => {
            if let Some(q) = quotes.first_mut() {
                q.symbol = symbol.clone();
                q.market = market.to_uppercase();
                state.quote_cache.insert(cache_key, q.clone());
                return ok_json(q).into_response();
            }
            err_json(axum::http::StatusCode::NOT_FOUND, "no quote data")
        }
        Err(e) => {
            tracing::error!("quote fetch error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

/// POST /api/v1/quotes/batch
pub async fn batch_quotes(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchQuoteRequest>,
) -> Response {
    let provider = TencentQuoteProvider::new(state.config.http_proxy.clone());

    let mut by_market: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for item in &req.items {
        by_market
            .entry(item.market.to_uppercase())
            .or_default()
            .push((item.symbol.clone(), item.market.clone()));
    }

    let mut all_quotes: Vec<Quote> = Vec::new();
    for (market, items) in by_market {
        let mk = MarketCode::from_str(&market).unwrap_or(MarketCode::CN);
        let tencent_syms: Vec<String> =
            items.iter().map(|(s, _)| tencent_symbol(s, &mk)).collect();
        let original_syms: Vec<String> = items.iter().map(|(s, _)| s.clone()).collect();

        match provider.fetch(&tencent_syms, &market).await {
            Ok(mut quotes) => {
                for (i, q) in quotes.iter_mut().enumerate() {
                    if i < original_syms.len() {
                        q.symbol = original_syms[i].clone();
                    }
                    q.market = market.clone();
                    let cache_key = format!("{}:{}", market, q.symbol);
                    state.quote_cache.insert(cache_key, q.clone());
                }
                all_quotes.append(&mut quotes);
            }
            Err(e) => {
                tracing::warn!("batch quote error for market {}: {:?}", market, e);
            }
        }
    }

    ok_json(&all_quotes).into_response()
}
