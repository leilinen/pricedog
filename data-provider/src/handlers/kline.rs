use crate::error::{err_json, ok_json};
use crate::models::kline::KlineResponse;
use crate::models::market::MarketCode;
use crate::providers::kline::{eastmoney, stooq, tencent};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct KlineQuery {
    pub interval: Option<String>,
    pub limit: Option<usize>,
}

/// GET /api/v1/klines/:market/:symbol?interval=1d&limit=60
pub async fn get_klines(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
    Query(query): Query<KlineQuery>,
) -> Response {
    let mk = match MarketCode::from_str(&market) {
        Some(m) => m,
        None => return err_json(axum::http::StatusCode::BAD_REQUEST, "invalid market"),
    };

    let interval = query.interval.unwrap_or_else(|| "1d".to_string());
    let limit = query.limit.unwrap_or(60);

    let cache_key = format!("{}:{}:{}", market.to_uppercase(), symbol, interval);
    if let Some(cached) = state.kline_cache.get(&cache_key) {
        return ok_json(&cached).into_response();
    }

    let proxy = state.config.http_proxy.as_deref();
    let klines = fetch_klines_with_fallback(&symbol, &mk, limit, &interval, proxy).await;

    let resp = KlineResponse {
        market: market.to_uppercase(),
        symbol: symbol.clone(),
        interval: interval.clone(),
        count: klines.len(),
        klines,
    };

    state.kline_cache.insert(cache_key, resp.clone());

    ok_json(&resp).into_response()
}

/// Fetch K-lines with provider fallback chain:
/// - Tencent primary for all markets
/// - US: fallback to Stooq if too few bars
/// - CN/HK: fallback to EastMoney for long history
async fn fetch_klines_with_fallback(
    symbol: &str,
    market: &MarketCode,
    days: usize,
    interval: &str,
    proxy: Option<&str>,
) -> Vec<crate::models::kline::Kline> {
    match tencent::fetch_tencent_klines(symbol, market, days, interval, proxy).await {
        Ok(klines) if !klines.is_empty() => {
            // US: Stooq fallback if Tencent returns too few
            if matches!(market, MarketCode::US) && klines.len() < days.min(30).max(10) {
                if let Ok(stooq) = stooq::fetch_stooq_klines(symbol, proxy).await {
                    if !stooq.is_empty() {
                        return tail(&stooq, days);
                    }
                }
            }

            // CN/HK: EastMoney fallback for longer history
            if matches!(market, MarketCode::CN | MarketCode::HK)
                && (days >= 500 || klines.len() < (days as f64 * 0.6) as usize)
            {
                let em_days = days.max(3000).min(20000);
                if let Ok(em) =
                    eastmoney::fetch_eastmoney_klines(symbol, market, em_days, interval, proxy).await
                {
                    if em.len() > klines.len() {
                        return tail(&em, days);
                    }
                }
            }

            klines
        }
        _ => match market {
            MarketCode::US => {
                if let Ok(stooq) = stooq::fetch_stooq_klines(symbol, proxy).await {
                    return tail(&stooq, days);
                }
                vec![]
            }
            MarketCode::CN | MarketCode::HK => {
                let em_days = days.max(3000).min(20000);
                if let Ok(em) =
                    eastmoney::fetch_eastmoney_klines(symbol, market, em_days, interval, proxy).await
                {
                    return tail(&em, days);
                }
                vec![]
            }
            _ => vec![],
        },
    }
}

fn tail(v: &[crate::models::kline::Kline], n: usize) -> Vec<crate::models::kline::Kline> {
    if v.len() > n {
        v[v.len() - n..].to_vec()
    } else {
        v.to_vec()
    }
}
