use crate::error::{err_json, ok_json};
use crate::providers::events::eastmoney;
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub symbols: Option<String>,
    pub days: Option<u32>,
    pub limit: Option<usize>,
}

/// GET /api/v1/events?symbols=600519,300750&days=7&limit=50
pub async fn get_events(
    State(state): State<Arc<AppState>>,
    Query(query): Query<EventsQuery>,
) -> Response {
    let symbols_str = query.symbols.unwrap_or_default();
    let symbols: Vec<String> = symbols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if symbols.is_empty() {
        return err_json(axum::http::StatusCode::BAD_REQUEST, "symbols required");
    }

    let days = query.days.unwrap_or(7);
    let limit = query.limit.unwrap_or(50);
    let proxy = state.config.http_proxy.as_deref();

    match eastmoney::fetch_events(&symbols, days, limit, proxy).await {
        Ok(items) => ok_json(&items).into_response(),
        Err(e) => {
            tracing::error!("events fetch error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}
