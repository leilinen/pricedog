/// Environment-based configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub quote_cache_ttl_secs: u64,
    pub kline_cache_ttl_secs: u64,
    pub http_proxy: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            port: std::env::var("DP_PORT")
                .unwrap_or_else(|_| "8003".into())
                .parse()
                .unwrap_or(8003),
            database_url: std::env::var("DP_DATABASE_URL")
                .unwrap_or_else(|_| "postgres://pricedog:pricedog@127.0.0.1:15432/pricedog".into()),
            quote_cache_ttl_secs: std::env::var("DP_QUOTE_CACHE_TTL_SECS")
                .unwrap_or_else(|_| "10".into())
                .parse()
                .unwrap_or(10),
            kline_cache_ttl_secs: std::env::var("DP_KLINE_CACHE_TTL_SECS")
                .unwrap_or_else(|_| "300".into())
                .parse()
                .unwrap_or(300),
            http_proxy: std::env::var("HTTP_PROXY")
                .or_else(|_| std::env::var("http_proxy"))
                .ok(),
        }
    }
}
