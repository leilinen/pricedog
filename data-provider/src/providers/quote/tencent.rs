use crate::models::quote::Quote;

/// Parse a single line from Tencent quote API response.
/// Ported from Python: akshare_collector.py::_parse_tencent_line
pub fn parse_tencent_line(line: &str) -> Option<Quote> {
    let line = line.trim();
    if !line.contains("=\"\"") && line.contains("=\"") {
        // valid line
    } else if line.contains("=\"\"") || line.is_empty() {
        return None;
    } else {
        return None;
    }

    let sep = line.find("=\"")?;
    let value = &line[sep + 2..];
    let value = value.trim_end_matches('"').trim_end_matches(';');
    let parts: Vec<&str> = value.split('~').collect();
    if parts.len() < 35 {
        return None;
    }

    // Parse turnover from parts[35]
    let turnover = parse_turnover(parts.get(35).copied());

    let symbol = clean_symbol(parts.get(2).unwrap_or(&""));

    Some(Quote {
        symbol,
        name: parts.get(1).unwrap_or(&"").to_string(),
        market: String::new(), // set by caller
        current_price: safe_f64(parts.get(3).copied(), 0.0),
        prev_close: safe_f64(parts.get(4).copied(), 0.0),
        open_price: safe_f64(parts.get(5).copied(), 0.0),
        volume: safe_f64(parts.get(6).copied(), 0.0),
        change_amount: safe_f64(parts.get(31).copied(), 0.0),
        change_pct: safe_f64(parts.get(32).copied(), 0.0),
        high_price: safe_f64(parts.get(33).copied(), 0.0),
        low_price: safe_f64(parts.get(34).copied(), 0.0),
        turnover,
        turnover_rate: opt_f64(parts.get(38).copied()),
        pe_ratio: opt_f64(parts.get(39).copied()),
        circulating_market_value: if parts.len() > 44 {
            opt_f64(parts.get(44).copied())
        } else {
            None
        },
        total_market_value: if parts.len() > 45 {
            opt_f64(parts.get(45).copied())
        } else {
            None
        },
    })
}

fn parse_turnover(raw: Option<&str>) -> f64 {
    let raw = match raw {
        Some(r) => r,
        None => return 0.0,
    };
    if !raw.contains('/') {
        return 0.0;
    }
    let parts: Vec<&str> = raw.split('/').collect();
    if parts.len() >= 3 {
        parts[2].parse::<f64>().unwrap_or(0.0)
    } else {
        0.0
    }
}

/// Clean US stock symbol: AAPL.OQ -> AAPL. Keep index symbols starting with '.'.
fn clean_symbol(raw: &str) -> String {
    if raw.contains('.') && !raw.starts_with('.') {
        raw.split('.').next().unwrap_or(raw).to_string()
    } else {
        raw.to_string()
    }
}

fn safe_f64(val: Option<&str>, default: f64) -> f64 {
    val.and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

fn opt_f64(val: Option<&str>) -> Option<f64> {
    val.and_then(|v| {
        let v = v.trim();
        if v.is_empty() {
            None
        } else {
            v.parse::<f64>().ok()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_symbol() {
        assert_eq!(clean_symbol("AAPL.OQ"), "AAPL");
        assert_eq!(clean_symbol(".IXIC"), ".IXIC");
        assert_eq!(clean_symbol("600519"), "600519");
    }
}
