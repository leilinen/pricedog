use crate::models::kline::Kline;
use crate::providers::quote::build_reqwest_client;

/// Fetch daily US K-lines from Stooq (CSV format).
/// Ported from Python: kline_collector.py::_fetch_stooq_us_klines
pub async fn fetch_stooq_klines(
    symbol: &str,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<Kline>> {
    let sym = symbol.trim().to_lowercase();
    if sym.is_empty() {
        return Ok(vec![]);
    }

    let stooq_sym = format!("{}.us", sym);
    let url = format!(
        "https://stooq.com/q/d/l/?s={}&i=d",
        urlencoding::encode(&stooq_sym)
    );

    let client = build_reqwest_client(http_proxy)?;
    let mut last_err = None;
    let mut text = String::new();

    for attempt in 0..3u32 {
        let timeout = 12 + attempt * 6;
        match client
            .get(&url)
            .header("User-Agent", "PanWatch/1.0")
            .timeout(std::time::Duration::from_secs(timeout as u64))
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    text = resp.text().await.unwrap_or_default();
                    last_err = None;
                    break;
                } else {
                    last_err = Some(anyhow::anyhow!("HTTP {}", resp.status()));
                }
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(e));
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(400 * (attempt as u64 + 1))).await;
    }

    if last_err.is_some() {
        return Ok(vec![]);
    }

    let lines: Vec<&str> = text.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
    if lines.len() <= 1 {
        return Ok(vec![]);
    }

    let mut klines = Vec::new();
    for line in &lines[1..] {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 6 {
            continue;
        }
        let date = parts[0];
        if date == "Date" || date.is_empty() {
            continue;
        }
        klines.push(Kline {
            ts: date.to_string(),
            open: parts[1].parse().unwrap_or(0.0),
            close: parts[3].parse().unwrap_or(0.0),
            high: parts[2].parse().unwrap_or(0.0),
            low: parts[4].parse().unwrap_or(0.0),
            volume: parts[5].parse().unwrap_or(0.0),
            turnover: None,
        });
    }

    Ok(klines)
}
