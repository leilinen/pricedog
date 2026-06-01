use crate::models::capital_flow::CapitalFlow;
use crate::models::market::eastmoney_secid;
use crate::models::market::MarketCode;
use crate::providers::quote::build_reqwest_client;

/// Fetch capital flow from EastMoney API.
/// Ported from Python: capital_flow_collector.py::CapitalFlowCollector
pub async fn fetch_capital_flow(
    symbol: &str,
    market: &MarketCode,
    http_proxy: Option<&str>,
) -> anyhow::Result<Option<CapitalFlow>> {
    let secid = eastmoney_secid(symbol, market);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    let url = format!(
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?lmt=0&klt=101&secid={}&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65&ut=b2884a393a59ad64002292a3e90d46a5&_={}",
        secid, ts
    );

    let client = build_reqwest_client(http_proxy)?;
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .header("Referer", "https://quote.eastmoney.com/")
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    let d = match data.get("data") {
        Some(d) => d,
        None => return Ok(None),
    };

    let klines = match d.get("klines").and_then(|k| k.as_array()) {
        Some(k) => k,
        None => return Ok(None),
    };
    if klines.is_empty() {
        return Ok(None);
    }

    // Last line: date,main_net,small_net,mid_net,big_net,super_net,main_pct,...
    let last = klines.last().unwrap().as_str().unwrap_or("");
    let parts: Vec<&str> = last.split(',').collect();
    if parts.len() < 13 {
        return Ok(None);
    }

    // 5-day main net inflow
    let last5 = klines.iter().rev().take(5);
    let mut main_net_5d = 0.0;
    for line in last5 {
        if let Some(s) = line.as_str() {
            let p: Vec<&str> = s.split(',').collect();
            if p.len() >= 2 {
                main_net_5d += safe_f64(p[1]);
            }
        }
    }

    Ok(Some(CapitalFlow {
        symbol: d.get("code").and_then(|v| v.as_str()).unwrap_or(symbol).to_string(),
        name: d.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        main_net_inflow: safe_f64(parts[1]),
        main_net_inflow_pct: safe_f64(parts[6]),
        super_net_inflow: safe_f64(parts[5]),
        big_net_inflow: safe_f64(parts[4]),
        mid_net_inflow: safe_f64(parts[3]),
        small_net_inflow: safe_f64(parts[2]),
        main_net_5d: Some(main_net_5d),
    }))
}

fn safe_f64(val: &str) -> f64 {
    val.trim().parse::<f64>().unwrap_or(0.0)
}
