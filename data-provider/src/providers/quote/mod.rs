pub mod tencent;

use crate::models::quote::Quote;

pub struct TencentQuoteProvider {
    pub http_proxy: Option<String>,
}

impl TencentQuoteProvider {
    pub fn new(http_proxy: Option<String>) -> Self {
        Self { http_proxy }
    }

    /// Fetch quotes for the given Tencent-formatted symbols.
    pub async fn fetch(
        &self,
        tencent_symbols: &[String],
        market: &str,
    ) -> anyhow::Result<Vec<Quote>> {
        if tencent_symbols.is_empty() {
            return Ok(vec![]);
        }

        let client = build_reqwest_client(self.http_proxy.as_deref())?;
        let url = format!("http://qt.gtimg.cn/q={}", tencent_symbols.join(","));
        let resp = client.get(&url).timeout(std::time::Duration::from_secs(10)).send().await?;
        let bytes = resp.bytes().await?;
        let (text, _, _) = encoding_rs::GBK.decode(&bytes);

        let mut results = Vec::new();
        for line in text.trim().split(';') {
            if let Some(mut q) = tencent::parse_tencent_line(line) {
                q.market = market.to_uppercase();
                results.push(q);
            }
        }
        Ok(results)
    }
}

/// Build a reqwest::Client with optional proxy.
pub fn build_reqwest_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(p) = proxy {
        if !p.is_empty() {
            let proxy = reqwest::Proxy::all(p)?;
            builder = builder.proxy(proxy);
        }
    }
    Ok(builder.build()?)
}
