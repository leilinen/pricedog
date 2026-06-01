use crate::models::news::NewsItem;

/// Parse a single EastMoney announcement into a NewsItem.
/// Ported from Python: news_collector.py::EastMoneyNewsCollector._parse_item
pub fn parse_announcement(
    item: &serde_json::Value,
    a_share_symbols: &[&String],
) -> Option<NewsItem> {
    let external_id = item.get("art_code")?.as_str()?.to_string();
    let title = item.get("title")?.as_str()?.trim().to_string();
    if external_id.is_empty() || title.is_empty() {
        return None;
    }

    // Extract stock codes
    let codes = item
        .get("codes")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.get("stock_code").and_then(|s| s.as_str()).map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| a_share_symbols.iter().take(1).map(|s| (*s).clone()).collect());

    // Parse time
    let notice_date = item
        .get("notice_date")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let publish_time = parse_datetime(notice_date);

    // Importance
    let columns = item.get("columns").and_then(|c| c.as_array()).cloned().unwrap_or_default();
    let column_names: Vec<&str> = columns
        .iter()
        .filter_map(|c| c.get("column_name").and_then(|n| n.as_str()))
        .collect();

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

    Some(NewsItem {
        source: "eastmoney".to_string(),
        external_id,
        title,
        content: String::new(),
        publish_time,
        symbols: codes,
        importance,
        url,
    })
}

fn parse_datetime(raw: &str) -> String {
    // Try full datetime first
    if let Ok(_) = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return raw.to_string();
    }
    // Try date only
    if raw.len() >= 10 {
        if let Ok(_) = chrono::NaiveDate::parse_from_str(&raw[..10], "%Y-%m-%d") {
            return format!("{} 00:00:00", &raw[..10]);
        }
    }
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn guess_importance(title: &str, column_names: &[&str]) -> i32 {
    let keywords_3 = ["重大", "业绩预告", "业绩快报", "年报", "半年报"];
    if keywords_3.iter().any(|k| title.contains(k)) {
        return 3;
    }
    let keywords_2 = ["季报", "分红", "增持", "减持"];
    if keywords_2.iter().any(|k| title.contains(k)) {
        return 2;
    }
    if column_names.iter().any(|c| c.contains("临时")) {
        return 1;
    }
    0
}
