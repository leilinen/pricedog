use crate::error::{err_json, ok_json};
use crate::providers::news;
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct NewsQuery {
    pub symbols: Option<String>,
    pub hours: Option<u32>,
    pub limit: Option<usize>,
}

/// GET /api/v1/news?symbols=600519,300750&hours=24&limit=50
pub async fn get_news(
    State(state): State<Arc<AppState>>,
    Query(query): Query<NewsQuery>,
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

    let hours = query.hours.unwrap_or(24);
    let limit = query.limit.unwrap_or(50);
    let proxy = state.config.http_proxy.as_deref();

    match news::fetch_news(&symbols, hours, limit, proxy).await {
        Ok(items) => ok_json(&items).into_response(),
        Err(e) => {
            tracing::error!("news fetch error: {:?}", e);
            err_json(axum::http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}
