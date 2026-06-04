use crate::types::{Indicators, Kline};

// ---------------------------------------------------------------------------
// Interval utilities (shared with main.rs)
// ---------------------------------------------------------------------------

pub fn normalize_interval(interval: &str) -> String {
    match interval.trim().to_ascii_lowercase().as_str() {
        "5" | "5min" | "5mins" | "5minute" | "5minutes" => "5m".to_string(),
        "15" | "15min" | "15mins" | "15minute" | "15minutes" => "15m".to_string(),
        "30" | "30min" | "30mins" | "30minute" | "30minutes" => "30m".to_string(),
        "60" | "60m" | "1hour" | "1hours" => "1h".to_string(),
        "120" | "120m" | "2hour" | "2hours" => "2h".to_string(),
        "240" | "240m" | "4hour" | "4hours" => "4h".to_string(),
        "day" | "daily" | "d" => "1d".to_string(),
        other => other.to_string(),
    }
}

pub fn interval_minutes(interval: &str) -> Option<i64> {
    match normalize_interval(interval).as_str() {
        "5m" => Some(5),
        "15m" => Some(15),
        "30m" => Some(30),
        "1h" => Some(60),
        "2h" => Some(120),
        "4h" => Some(240),
        "1d" => None,
        _ => None,
    }
}

pub fn interval_millis(interval: &str) -> Option<i64> {
    match normalize_interval(interval).as_str() {
        "1d" => Some(24 * 60 * 60 * 1000),
        other => interval_minutes(other).map(|minutes| minutes * 60 * 1000),
    }
}

// ---------------------------------------------------------------------------
// Parsing and aggregation
// ---------------------------------------------------------------------------

pub fn parse_f64(value: &serde_json::Value) -> f64 {
    if let Some(v) = value.as_f64() {
        v
    } else if let Some(s) = value.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else {
        0.0
    }
}

pub fn parse_ts_millis(ts: &str) -> Option<i64> {
    if let Ok(v) = ts.parse::<i64>() {
        return Some(v);
    }
    chrono::DateTime::parse_from_rfc3339(ts)
        .map(|d| d.timestamp_millis())
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|d| d.and_utc().timestamp_millis())
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|d| d.and_utc().timestamp_millis())
        })
}

pub fn aggregate_intraday(bars: &[Kline], target_interval: &str) -> Vec<Kline> {
    let Some(target_minutes) = interval_minutes(target_interval) else {
        return bars.to_vec();
    };
    let mut sorted = bars.to_vec();
    sorted.sort_by(|a, b| a.ts.cmp(&b.ts));
    let mut out: Vec<Kline> = Vec::new();
    let mut bucket_start: Option<i64> = None;
    for bar in sorted {
        let ts = parse_ts_millis(&bar.ts).unwrap_or(0);
        let minutes = ts / 1000 / 60;
        let bucket = minutes - (minutes % target_minutes);
        if bucket_start != Some(bucket) {
            bucket_start = Some(bucket);
            out.push(bar);
        } else if let Some(last) = out.last_mut() {
            last.high = last.high.max(bar.high);
            last.low = last.low.min(bar.low);
            last.close = bar.close;
            last.volume += bar.volume;
            last.turnover += bar.turnover;
            last.ts = bar.ts;
        }
    }
    out
}

pub fn compute_indicators(klines: &[Kline]) -> Indicators {
    let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
    let ema20_vals = ema(&closes, 20);
    let ema5_vals = ema(&closes, 5);
    let atr14_values = atr(klines, 14);
    let atr20_values = atr(klines, 20);
    let ema20 = ema20_vals.last().copied();
    let ema5 = ema5_vals.last().copied();
    let atr14 = atr14_values.last().copied().flatten();
    let atr20 = atr20_values.last().copied().flatten();
    let close = closes.last().copied().unwrap_or(0.0);
    let ema20_position = match (ema20, atr14) {
        (Some(e), Some(a)) if a > 0.0 => Some((close - e) / a),
        _ => None,
    };
    let volume_ratio = if klines.len() >= 21 {
        let avg = klines[klines.len() - 21..klines.len() - 1]
            .iter()
            .map(|k| k.volume)
            .sum::<f64>()
            / 20.0;
        if avg > 0.0 {
            Some(klines.last().map(|k| k.volume).unwrap_or(0.0) / avg)
        } else {
            None
        }
    } else {
        None
    };
    let amplitude = if klines.len() >= 2 {
        let curr = klines.last().unwrap();
        let prev = &klines[klines.len() - 2];
        if prev.close > 0.0 {
            Some((curr.high - curr.low) / prev.close)
        } else {
            None
        }
    } else {
        None
    };
    Indicators {
        ema20,
        ema5,
        atr14,
        atr20,
        ema20_position,
        volume_ratio,
        amplitude,
    }
}

pub fn ema(data: &[f64], period: usize) -> Vec<f64> {
    if data.is_empty() {
        return Vec::new();
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut out = Vec::with_capacity(data.len());
    let mut prev = data[0];
    out.push(prev);
    for price in &data[1..] {
        prev = (*price - prev) * multiplier + prev;
        out.push(prev);
    }
    out
}

pub fn atr(klines: &[Kline], period: usize) -> Vec<Option<f64>> {
    if klines.is_empty() {
        return Vec::new();
    }
    let mut trs = Vec::with_capacity(klines.len());
    for (i, k) in klines.iter().enumerate() {
        let tr = if i == 0 {
            k.high - k.low
        } else {
            let prev_close = klines[i - 1].close;
            (k.high - k.low)
                .max((k.high - prev_close).abs())
                .max((k.low - prev_close).abs())
        };
        trs.push(tr);
    }
    let mut out = vec![None; klines.len()];
    if trs.len() < period {
        return out;
    }
    let mut value = trs[..period].iter().sum::<f64>() / period as f64;
    out[period - 1] = Some(value);
    for i in period..trs.len() {
        value = (value * (period as f64 - 1.0) + trs[i]) / period as f64;
        out[i] = Some(value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn ema_uses_close_series() {
        let data = (1..=25).map(|v| v as f64).collect::<Vec<_>>();
        let values = ema(&data, 20);
        assert_eq!(values.len(), 25);
        assert!(values.last().unwrap() > &10.0);
        assert!(values.last().unwrap() < &25.0);
    }

    #[test]
    fn atr_and_position_are_calculated() {
        let bars = (0..30)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect::<Vec<_>>();
        let indicators = compute_indicators(&bars);
        assert!(indicators.ema20.is_some());
        assert!(indicators.atr14.is_some());
        assert!(indicators.atr20.is_some());
        assert!(indicators.ema20_position.is_some());
    }

    #[test]
    fn volume_ratio_uses_previous_20_bars() {
        let mut bars = (0..21)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect::<Vec<_>>();
        bars.last_mut().unwrap().volume = 3000.0;

        let indicators = compute_indicators(&bars);

        assert_relative_eq!(indicators.volume_ratio.unwrap(), 3.0);
    }
}
