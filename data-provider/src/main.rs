mod config;
mod db;
mod error;
mod handlers;
mod models;
mod providers;
mod state;

use axum::{Router, routing::get, routing::post};
use state::AppState;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg = config::Config::from_env();
    tracing::info!("data-provider starting on port {}", cfg.port);

    let db_client = db::connect(&cfg.database_url).await?;
    let state = Arc::new(AppState::new(cfg.clone(), db_client));

    let app = Router::new()
        // Health
        .route("/health", get(handlers::health::health))
        // Quotes
        .route(
            "/api/v1/quote/:market/:symbol",
            get(handlers::quote::get_quote),
        )
        .route("/api/v1/quotes/batch", post(handlers::quote::batch_quotes))
        // K-lines
        .route(
            "/api/v1/klines/:market/:symbol",
            get(handlers::kline::get_klines),
        )
        // News
        .route("/api/v1/news", get(handlers::news::get_news))
        // Events
        .route("/api/v1/events", get(handlers::events::get_events))
        // Capital Flow
        .route(
            "/api/v1/capital-flow/:market/:symbol",
            get(handlers::capital_flow::get_capital_flow),
        )
        // Discovery
        .route(
            "/api/v1/discovery/stocks",
            get(handlers::discovery::get_hot_stocks),
        )
        .route(
            "/api/v1/discovery/boards",
            get(handlers::discovery::get_hot_boards),
        )
        .route(
            "/api/v1/discovery/boards/:board_code/stocks",
            get(handlers::discovery::get_board_stocks),
        )
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", cfg.port)).await?;
    tracing::info!("data-provider listening on {}", cfg.port);
    axum::serve(listener, app).await?;

    Ok(())
}
