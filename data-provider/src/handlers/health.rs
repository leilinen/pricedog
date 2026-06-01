use crate::error::ok_json;
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use serde_json::json;
use std::sync::Arc;

pub async fn health(State(_state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    ok_json(&json!({
        "status": "ok",
        "service": "data-provider",
    }))
}
