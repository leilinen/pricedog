pub mod eastmoney;

use crate::models::news::NewsItem;
use crate::providers::quote::build_reqwest_client;

/// Fetch news from EastMoney announcement API.
/// Ported from Python: news_collector.py::EastMoneyNewsCollector
pub async fn fetch_news(
    symbols: &[String],
    hours: u32,
    limit: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<NewsItem>> {
    // Only A-share 6-digit codes
    let a_share: Vec<&String> = symbols.iter().filter(|s| s.len() == 6 && s.chars().all(|c| c.is_ascii_digit())).collect();
    if a_share.is_empty() {
        return Ok(vec![]);
    }

    let stock_list = a_share.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",");
    let url = format!(
        "https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1&page_size={}&page_index=1&ann_type=A&stock_list={}&f_node=0&s_node=0",
        limit.min(100),
        urlencoding::encode(&stock_list)
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    if data.get("success").and_then(|v| v.as_bool()) != Some(true) {
        return Ok(vec![]);
    }

    let items = data
        .get("data")
        .and_then(|d| d.get("list"))
        .and_then(|l| l.as_array())
        .cloned()
        .unwrap_or_default();

    let cutoff = chrono::Utc::now() - chrono::Duration::hours(hours as i64);
    let mut results = Vec::new();

    for item in &items {
        if let Some(news) = eastmoney::parse_announcement(item, &a_share) {
            // Time filter
            if let Ok(pt) = chrono::NaiveDateTime::parse_from_str(&news.publish_time, "%Y-%m-%d %H:%M:%S") {
                let pt_utc = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(pt, chrono::Utc);
                if pt_utc < cutoff {
                    continue;
                }
            }
            results.push(news);
        }
    }

    // Deduplicate by (source, external_id)
    let mut seen = std::collections::HashSet::new();
    results.retain(|n| seen.insert((n.source.clone(), n.external_id.clone())));

    Ok(results)
}
