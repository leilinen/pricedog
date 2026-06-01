use crate::config::Config;
use crate::models::kline::KlineResponse;
use crate::models::quote::Quote;
use moka::sync::Cache;
use std::sync::Arc;
use tokio_postgres::Client;

/// Shared application state.
pub struct AppState {
    pub config: Config,
    pub db: Arc<tokio::sync::Mutex<Client>>,
    /// Quote cache: key = "market:symbol" → Quote
    pub quote_cache: Cache<String, Quote>,
    /// Kline cache: key = "market:symbol:interval" → KlineResponse (owned)
    pub kline_cache: Cache<String, KlineResponse>,
}

impl AppState {
    pub fn new(config: Config, db: Client) -> Self {
        let quote_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(config.quote_cache_ttl_secs))
            .max_capacity(10_000)
            .build();
        let kline_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(config.kline_cache_ttl_secs))
            .max_capacity(5_000)
            .build();
        Self {
            config,
            db: Arc::new(tokio::sync::Mutex::new(db)),
            quote_cache,
            kline_cache,
        }
    }
}
