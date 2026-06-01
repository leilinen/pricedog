use crate::models::kline::Kline;
use crate::models::market::{tencent_symbol, MarketCode};
use crate::providers::quote::build_reqwest_client;

/// Fetch daily K-lines from Tencent API.
/// Ported from Python: kline_collector.py::KlineCollector.get_klines
pub async fn fetch_tencent_klines(
    symbol: &str,
    market: &MarketCode,
    days: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<Kline>> {
    let tencent_sym = tencent_symbol(symbol, market);
    let param = format!("{},day,,,{},qfq", tencent_sym, days);
    let encoded_param = urlencoding::encode(&param);
    let url = format!(
        "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={}&_var=kline_dayqfq",
        encoded_param
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
    let text = resp.text().await?;

    // Parse JS variable format: kline_dayqfq={...};
    let json_str = match text.find('=') {
        Some(idx) => {
            let mut s = text[idx + 1..].trim().to_string();
            if s.ends_with(';') {
                s.pop();
            }
            s
        }
        None => return Ok(vec![]),
    };

    let data: serde_json::Value = serde_json::from_str(&json_str)?;
    let raw_data = data.get("data");

    let day_data = extract_day_data(raw_data, &tencent_sym);

    let mut klines = Vec::new();
    for item in &day_data {
        if let Some(arr) = item.as_array() {
            if arr.len() >= 5 {
                klines.push(Kline {
                    ts: arr[0].as_str().unwrap_or("").to_string(),
                    open: arr[1].as_f64().unwrap_or(0.0),
                    close: arr[2].as_f64().unwrap_or(0.0),
                    high: arr[3].as_f64().unwrap_or(0.0),
                    low: arr[4].as_f64().unwrap_or(0.0),
                    volume: arr.get(5).and_then(|v| v.as_f64()).unwrap_or(0.0),
                    turnover: arr.get(6).and_then(|v| v.as_f64()),
                });
            }
        }
    }

    Ok(klines)
}

/// Extract day/qfqday array from Tencent response, handling old and new formats.
fn extract_day_data(raw_data: Option<&serde_json::Value>, tencent_sym: &str) -> Vec<serde_json::Value> {
    let data = match raw_data {
        Some(d) => d,
        None => return vec![],
    };

    // New format: data is an array directly
    if let Some(arr) = data.as_array() {
        return arr.clone();
    }

    // Old format: data.{symbol}.day or data.{symbol}.qfqday
    if let Some(obj) = data.as_object() {
        if let Some(stock_data) = obj.get(tencent_sym) {
            if let Some(sd) = stock_data.as_object() {
                if let Some(day) = sd.get("qfqday").or_else(|| sd.get("day")) {
                    if let Some(arr) = day.as_array() {
                        return arr.clone();
                    }
                }
            }
        }
    }

    vec![]
}
