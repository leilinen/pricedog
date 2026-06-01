use crate::error::{err_json, ok_json};
use crate::providers::discovery::eastmoney;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct DiscoveryQuery {
    pub market: Option<String>,
    pub mode: Option<String>,
    pub limit: Option<usize>,
}

/// GET /api/v1/discovery/stocks?market=CN&mode=turnover&limit=20
pub async fn get_hot_stocks(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DiscoveryQuery>,
) -> Response {
    let market = query.market.as_deref().unwrap_or("CN").to_uppercase();
    let mode = query.mode.as_deref().unwrap_or("turnover");
    let limit = query.limit.unwrap_or(20);
    let proxy = state.config.http_proxy.as_deref();

    match eastmoney::fetch_hot_stocks(&market, mode, limit, proxy).await {
        Ok(stocks) => ok_json(&stocks).into_response(),
        Err(e) => {
            tracing::error!("discovery stocks error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

/// GET /api/v1/discovery/boards?mode=gainers&limit=12
pub async fn get_hot_boards(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DiscoveryQuery>,
) -> Response {
    let mode = query.mode.as_deref().unwrap_or("gainers");
    let limit = query.limit.unwrap_or(12);
    let proxy = state.config.http_proxy.as_deref();

    match eastmoney::fetch_hot_boards(mode, limit, proxy).await {
        Ok(boards) => ok_json(&boards).into_response(),
        Err(e) => {
            tracing::error!("discovery boards error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

/// GET /api/v1/discovery/boards/:board_code/stocks?mode=gainers&limit=20
pub async fn get_board_stocks(
    State(state): State<Arc<AppState>>,
    Path(board_code): Path<String>,
    Query(query): Query<DiscoveryQuery>,
) -> Response {
    let mode = query.mode.as_deref().unwrap_or("gainers");
    let limit = query.limit.unwrap_or(20);
    let proxy = state.config.http_proxy.as_deref();

    match eastmoney::fetch_board_stocks(&board_code, mode, limit, proxy).await {
        Ok(stocks) => ok_json(&stocks).into_response(),
        Err(e) => {
            tracing::error!("board stocks error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}
