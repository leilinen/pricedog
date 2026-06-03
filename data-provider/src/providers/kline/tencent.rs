use crate::models::kline::Kline;
use crate::models::market::{tencent_symbol, MarketCode};
use crate::providers::quote::build_reqwest_client;

/// Map normalized interval to Tencent API period parameter.
fn tencent_period(interval: &str) -> &'static str {
    match interval {
        "1d" => "day",
        "5m" => "m5",
        "15m" => "m15",
        "30m" => "m30",
        "1h" => "m60",
        _ => "day",
    }
}

/// Parse a JSON value as f64, handling both number and string types.
fn parse_flex(val: Option<&serde_json::Value>) -> f64 {
    match val {
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Fetch K-lines from Tencent API.
/// Supports daily and intraday intervals (5m, 15m, 30m, 1h).
pub async fn fetch_tencent_klines(
    symbol: &str,
    market: &MarketCode,
    days: usize,
    interval: &str,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<Kline>> {
    let period = tencent_period(interval);
    let tencent_sym = tencent_symbol(symbol, market);
    let param = format!("{},{},,,{},qfq", tencent_sym, period, days);
    let encoded_param = urlencoding::encode(&param);
    let url = format!(
        "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={}&_var=kline_{}qfq",
        encoded_param, period
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

    let day_data = extract_day_data(raw_data, &tencent_sym, period);

    let mut klines = Vec::new();
    for item in &day_data {
        if let Some(arr) = item.as_array() {
            if arr.len() >= 5 {
                klines.push(Kline {
                    ts: arr[0].as_str().unwrap_or("").to_string(),
                    open: parse_flex(arr.get(1)),
                    close: parse_flex(arr.get(2)),
                    high: parse_flex(arr.get(3)),
                    low: parse_flex(arr.get(4)),
                    volume: arr.get(5).map_or(0.0, |v| parse_flex(Some(v))),
                    turnover: arr.get(6).map(|v| parse_flex(Some(v))),
                });
            }
        }
    }

    Ok(klines)
}

/// Extract kline array from Tencent response, handling old and new formats.
/// For intraday periods, looks for "m5"/"m15" etc. keys; for daily, "qfqday"/"day".
fn extract_day_data(raw_data: Option<&serde_json::Value>, tencent_sym: &str, period: &str) -> Vec<serde_json::Value> {
    let data = match raw_data {
        Some(d) => d,
        None => return vec![],
    };

    // New format: data is an array directly
    if let Some(arr) = data.as_array() {
        return arr.clone();
    }

    // Old format: data.{symbol}.qfq{period} or data.{symbol}.{period} or data.{symbol}.qfqday/day
    if let Some(obj) = data.as_object() {
        if let Some(stock_data) = obj.get(tencent_sym) {
            if let Some(sd) = stock_data.as_object() {
                // Try qfq+period first (e.g. qfqm5, qfqm60), then plain period, then qfqday/day
                let qfq_key = format!("qfq{}", period);
                let day = sd.get(&qfq_key)
                    .or_else(|| sd.get(period))
                    .or_else(|| sd.get("qfqday"))
                    .or_else(|| sd.get("day"));
                if let Some(day) = day {
                    if let Some(arr) = day.as_array() {
                        return arr.clone();
                    }
                }
            }
        }
    }

    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tencent_period_maps_intervals() {
        assert_eq!(tencent_period("1d"), "day");
        assert_eq!(tencent_period("5m"), "m5");
        assert_eq!(tencent_period("15m"), "m15");
        assert_eq!(tencent_period("30m"), "m30");
        assert_eq!(tencent_period("1h"), "m60");
        assert_eq!(tencent_period("unknown"), "day"); // fallback
    }

    #[test]
    fn parse_flex_handles_string_and_number() {
        let s = serde_json::Value::String("1268.020".into());
        assert_eq!(parse_flex(Some(&s)), 1268.02);

        let n = serde_json::json!(1303.0);
        assert_eq!(parse_flex(Some(&n)), 1303.0);

        assert_eq!(parse_flex(None), 0.0);
    }
}
