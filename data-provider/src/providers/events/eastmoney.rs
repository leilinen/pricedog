use crate::models::events::EventItem;
use crate::providers::quote::build_reqwest_client;

/// Fetch corporate events from EastMoney notices API.
/// Ported from Python: events_collector.py::EastMoneyEventsCollector
pub async fn fetch_events(
    symbols: &[String],
    days: u32,
    limit: usize,
    http_proxy: Option<&str>,
) -> anyhow::Result<Vec<EventItem>> {
    let a_share: Vec<&String> = symbols
        .iter()
        .filter(|s| s.len() == 6 && s.chars().all(|c| c.is_ascii_digit()))
        .collect();
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

    let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
    let mut results = Vec::new();

    for item in &items {
        if let Some(ev) = parse_event(item, &a_share) {
            if let Ok(pt) = chrono::NaiveDateTime::parse_from_str(&ev.publish_time, "%Y-%m-%d %H:%M:%S") {
                let pt_utc = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(pt, chrono::Utc);
                if pt_utc < cutoff {
                    continue;
                }
            }
            results.push(ev);
        }
    }

    // Deduplicate
    let mut seen = std::collections::HashSet::new();
    results.retain(|e| seen.insert((e.source.clone(), e.external_id.clone())));

    // Sort by time desc, importance desc
    results.sort_by(|a, b| {
        b.publish_time
            .cmp(&a.publish_time)
            .then_with(|| b.importance.cmp(&a.importance))
    });

    Ok(results)
}

fn parse_event(item: &serde_json::Value, a_share_symbols: &[&String]) -> Option<EventItem> {
    let external_id = item.get("art_code")?.as_str()?.to_string();
    let title = item.get("title")?.as_str()?.trim().to_string();
    if external_id.is_empty() || title.is_empty() {
        return None;
    }

    let codes = item
        .get("codes")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.get("stock_code").and_then(|s| s.as_str()).map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| a_share_symbols.iter().take(1).map(|s| (*s).clone()).collect());

    let notice_date = item.get("notice_date").and_then(|v| v.as_str()).unwrap_or("");
    let publish_time = parse_datetime(notice_date);

    let columns = item.get("columns").and_then(|c| c.as_array()).cloned().unwrap_or_default();
    let column_names: Vec<&str> = columns
        .iter()
        .filter_map(|c| c.get("column_name").and_then(|n| n.as_str()))
        .collect();

    let event_type = guess_event_type(&title, &column_names);
    let importance = guess_importance(&title, &column_names);

    let symbol_for_url = codes.first().map(|s| s.as_str()).unwrap_or("");
    let url = if !symbol_for_url.is_empty() {
        format!(
            "https://data.eastmoney.com/notices/detail/{}/{}.html",
            symbol_for_url, external_id
        )
    } else {
        String::new()
    };

    Some(EventItem {
        source: "eastmoney".to_string(),
        external_id,
        event_type,
        title,
        publish_time,
        symbols: codes,
        importance,
        url,
    })
}

fn parse_datetime(raw: &str) -> String {
    if let Ok(_) = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return raw.to_string();
    }
    if raw.len() >= 10 {
        if let Ok(_) = chrono::NaiveDate::parse_from_str(&raw[..10], "%Y-%m-%d") {
            return format!("{} 00:00:00", &raw[..10]);
        }
    }
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn guess_event_type(title: &str, column_names: &[&str]) -> String {
    let earnings = ["业绩预告", "业绩快报", "年报", "半年报", "季报", "三季报", "一季报"];
    if earnings.iter().any(|k| title.contains(k)) {
        return "earnings".to_string();
    }
    let dividend = ["分红", "派息", "除权", "除息", "送转", "股权登记"];
    if dividend.iter().any(|k| title.contains(k)) {
        return "dividend".to_string();
    }
    let suspension = ["停牌", "复牌"];
    if suspension.iter().any(|k| title.contains(k)) {
        return "suspension".to_string();
    }
    let repurchase = ["回购", "股份回购"];
    if repurchase.iter().any(|k| title.contains(k)) {
        return "repurchase".to_string();
    }
    let financing = ["增发", "配股", "定向增发", "发行"];
    if financing.iter().any(|k| title.contains(k)) {
        return "financing".to_string();
    }
    let insider = ["减持", "增持", "股东", "董监高", "持股变动"];
    if insider.iter().any(|k| title.contains(k)) {
        return "insider".to_string();
    }
    let regulatory = ["诉讼", "仲裁", "立案", "处罚", "监管", "问询函"];
    if regulatory.iter().any(|k| title.contains(k)) {
        return "regulatory".to_string();
    }
    let restructuring = ["重组", "并购", "收购", "出售资产", "重大资产"];
    if restructuring.iter().any(|k| title.contains(k)) {
        return "restructuring".to_string();
    }
    if column_names.iter().any(|c| c.contains("临时公告") || c.contains("重大事项")) {
        return "major".to_string();
    }
    "notice".to_string()
}

fn guess_importance(title: &str, column_names: &[&str]) -> i32 {
    let level_3 = ["重大", "业绩预告", "业绩快报", "年报", "半年报", "重组", "停牌", "复牌"];
    if level_3.iter().any(|k| title.contains(k)) {
        return 3;
    }
    let level_2 = ["季报", "分红", "回购", "增持", "减持", "问询函", "处罚"];
    if level_2.iter().any(|k| title.contains(k)) {
        return 2;
    }
    if column_names.iter().any(|c| c.contains("临时")) {
        return 1;
    }
    0
}
