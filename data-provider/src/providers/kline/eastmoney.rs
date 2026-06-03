use crate::models::kline::Kline;
use crate::models::market::{eastmoney_secid, MarketCode};
use crate::providers::quote::build_reqwest_client;

/// Map normalized interval to EastMoney klt parameter.
fn eastmoney_klt(interval: &str) -> Option<&'static str> {
    match interval {
        "1d" => Some("101"),
        "5m" => Some("5"),
        "15m" => Some("15"),
        "30m" => Some("30"),
        "1h" => Some("60"),
        _ => None,
    }
}

/// Fetch K-lines from EastMoney API.
/// Supports daily and intraday intervals (5m, 15m, 30m, 1h) for CN/HK markets.
pub async fn fetch_eastmoney_klines(
    symbol: &str,
    market: &MarketCode,
    days: usize,
    interval: &str,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<Kline>> {
    if !matches!(market, MarketCode::CN | MarketCode::HK) {
        return Ok(vec![]);
    }
    let klt = eastmoney_klt(interval).unwrap_or("101");

    let secid = eastmoney_secid(symbol, market);
    let limit = days.max(1200).min(20000);

    let url = format!(
        "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={}&klt={}&fqt=1&lmt={}&end=20500101&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56&ut=fa5fd1943c7b386f172d6893dbfba10b",
        secid, klt, limit
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .header("Referer", "https://quote.eastmoney.com/")
        .timeout(std::time::Duration::from_secs(12))
        .send()
        .await?;

    let payload: serde_json::Value = resp.json().await?;
    let raw = payload
        .get("data")
        .and_then(|d| d.get("klines"))
        .and_then(|k| k.as_array())
        .cloned()
        .unwrap_or_default();

    let mut klines = Vec::new();
    for row in &raw {
        if let Some(s) = row.as_str() {
            let parts: Vec<&str> = s.split(',').collect();
            if parts.len() >= 6 {
                klines.push(Kline {
                    ts: parts[0].to_string(),
                    open: parts[1].parse().unwrap_or(0.0),
                    close: parts[2].parse().unwrap_or(0.0),
                    high: parts[3].parse().unwrap_or(0.0),
                    low: parts[4].parse().unwrap_or(0.0),
                    volume: parts[5].parse().unwrap_or(0.0),
                    turnover: None,
                });
            }
        }
    }

    // Return last `days` bars
    if klines.len() > days {
        Ok(klines.split_off(klines.len() - days))
    } else {
        Ok(klines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eastmoney_klt_maps_intervals() {
        assert_eq!(eastmoney_klt("1d"), Some("101"));
        assert_eq!(eastmoney_klt("5m"), Some("5"));
        assert_eq!(eastmoney_klt("15m"), Some("15"));
        assert_eq!(eastmoney_klt("30m"), Some("30"));
        assert_eq!(eastmoney_klt("1h"), Some("60"));
        assert_eq!(eastmoney_klt("unknown"), None);
    }
}
