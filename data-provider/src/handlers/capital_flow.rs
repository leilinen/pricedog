use crate::error::{err_json, ok_json};
use crate::models::market::MarketCode;
use crate::providers::capital_flow::eastmoney;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

/// GET /api/v1/capital-flow/:market/:symbol
pub async fn get_capital_flow(
    State(state): State<Arc<AppState>>,
    Path((market, symbol)): Path<(String, String)>,
) -> Response {
    let mk = match MarketCode::from_str(&market) {
        Some(m) => m,
        None => return err_json(axum::http::StatusCode::BAD_REQUEST, "invalid market"),
    };

    let proxy = state.config.http_proxy.as_deref();

    match eastmoney::fetch_capital_flow(&symbol, &mk, proxy).await {
        Ok(Some(flow)) => ok_json(&flow).into_response(),
        Ok(None) => err_json(axum::http::StatusCode::NOT_FOUND, "no capital flow data"),
        Err(e) => {
            tracing::error!("capital flow error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}
