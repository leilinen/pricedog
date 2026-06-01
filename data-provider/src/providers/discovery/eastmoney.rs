use crate::models::discovery::{HotBoard, HotStock};
use crate::providers::quote::build_reqwest_client;

const API_URL: &str = "https://push2.eastmoney.com/api/qt/clist/get";

/// Fetch hot stocks by turnover/gainers.
/// Ported from Python: discovery_collector.py::fetch_hot_stocks
pub async fn fetch_hot_stocks(
    market: &str,
    mode: &str,
    limit: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<HotStock>> {
    let fid = if mode == "turnover" { "f6" } else { "f3" };
    let fields = "f12,f14,f2,f3,f6,f5";
    let fs = match market {
        "CN" => "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "HK" => "m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2",
        "US" => "m:105,m:106,m:107",
        _ => return Ok(vec![]),
    };

    let url = format!(
        "{}?pn=1&pz={}&po=1&np=1&fltt=2&invt=2&fid={}&fs={}&fields={}",
        API_URL,
        limit.min(100),
        fid,
        fs,
        fields
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    let diff = data
        .get("data")
        .and_then(|d| d.get("diff"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut results = Vec::new();
    for it in &diff {
        results.push(HotStock {
            symbol: it.get("f12").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            market: market.to_uppercase(),
            name: it.get("f14").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            price: it.get("f2").and_then(|v| v.as_f64()),
            change_pct: it.get("f3").and_then(|v| v.as_f64()),
            turnover: it.get("f6").and_then(|v| v.as_f64()),
            volume: it.get("f5").and_then(|v| v.as_f64()),
        });
    }

    Ok(results)
}

/// Fetch hot industry boards.
/// Ported from Python: discovery_collector.py::fetch_hot_boards
pub async fn fetch_hot_boards(
    mode: &str,
    limit: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<HotBoard>> {
    let fid = if mode == "gainers" || mode == "hot" { "f3" } else { "f6" };
    let url = format!(
        "{}?pn=1&pz={}&po=1&np=1&fltt=2&invt=2&fid={}&fs=m:90+t:2&fields=f12,f14,f2,f3,f4,f6",
        API_URL,
        limit.min(100),
        fid
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    let diff = data
        .get("data")
        .and_then(|d| d.get("diff"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut results = Vec::new();
    for it in &diff {
        results.push(HotBoard {
            code: it.get("f12").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            name: it.get("f14").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            change_pct: it.get("f3").and_then(|v| v.as_f64()),
            change_amount: it.get("f4").and_then(|v| v.as_f64()),
            turnover: it.get("f6").and_then(|v| v.as_f64()),
        });
    }

    Ok(results)
}

/// Fetch stocks within a board.
/// Ported from Python: discovery_collector.py::fetch_board_stocks
pub async fn fetch_board_stocks(
    board_code: &str,
    mode: &str,
    limit: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<HotStock>> {
    let fid = if mode == "gainers" || mode == "hot" { "f3" } else { "f6" };
    let url = format!(
        "{}?pn=1&pz={}&po=1&np=1&fltt=2&invt=2&fid={}&fs=b:{}&fields=f12,f14,f2,f3,f6,f5",
        API_URL,
        limit.min(100),
        fid,
        board_code
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    let diff = data
        .get("data")
        .and_then(|d| d.get("diff"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut results = Vec::new();
    for it in &diff {
        results.push(HotStock {
            symbol: it.get("f12").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            market: "CN".to_string(),
            name: it.get("f14").and_then(|v| v.as_str()).unwrap_or("").trim().to_string(),
            price: it.get("f2").and_then(|v| v.as_f64()),
            change_pct: it.get("f3").and_then(|v| v.as_f64()),
            turnover: it.get("f6").and_then(|v| v.as_f64()),
            volume: it.get("f5").and_then(|v| v.as_f64()),
        });
    }

    Ok(results)
}
