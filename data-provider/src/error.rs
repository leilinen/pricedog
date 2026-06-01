use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub struct AppError(pub anyhow::Error);

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("request error: {:?}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "code": 500,
                "message": self.0.to_string(),
                "data": null,
            })),
        )
            .into_response()
    }
}

/// Helper: build a success JSON response.
pub fn ok_json<T: serde::Serialize>(data: &T) -> axum::Json<serde_json::Value> {
    Json(json!({
        "code": 0,
        "message": "ok",
        "data": data,
    }))
}

/// Helper: build an error JSON response with a given status code.
pub fn err_json(status: StatusCode, msg: &str) -> Response {
    (
        status,
        Json(json!({
            "code": status.as_u16(),
            "message": msg,
            "data": null,
        })),
    )
        .into_response()
}
