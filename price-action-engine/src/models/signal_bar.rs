use crate::backtest::{
    backtest_trades_from_model, generate_sample_candidates_from_model, SIGNAL_BAR_COOLDOWN_BARS,
    SIGNAL_BAR_Q_MIN, SIGNAL_BAR_TICK_SIZE,
};
use crate::indicators::{compute_indicators, ema};
use crate::model::PriceActionModel;
use crate::types::{BarFeatures, ContextResult, Indicators, Kline, Signal};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT: bool = true;
pub const SIGNAL_BAR_REQUIRE_TREND_ALIGN: bool = true;
pub const SIGNAL_BAR_USE_MA_CROSS: bool = false;
pub const SIGNAL_BAR_MA_CROSS_LOOKBACK: usize = 24;
pub const SIGNAL_BAR_SURPRISE_LOOKBACK: usize = 20;

// ---------------------------------------------------------------------------
// SignalBarModel struct + trait impl
// ---------------------------------------------------------------------------

pub struct SignalBarModel;

impl PriceActionModel for SignalBarModel {
    fn code(&self) -> &str {
        "pa_signal_bar_v1"
    }

    fn name(&self) -> &str {
        "pricedog_pa_signal_bar"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn min_klines(&self) -> usize {
        25
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
        backtest_trades_from_model(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self,
        klines: &[Kline],
        max_candidates: usize,
    ) -> Vec<crate::types::SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

// ---------------------------------------------------------------------------
// Signal Bar Model core functions
// ---------------------------------------------------------------------------

/// Compute basic features of a single K-line
pub fn compute_bar_features(k: &Kline) -> BarFeatures {
    let body = k.close - k.open;
    let range = k.high - k.low;
    let (p_b, p_c, p_u, p_d) = if range > 0.0 {
        let pb = body.abs() / range;
        let pc = (k.close - k.low) / range;
        let pu = (k.high - k.close.max(k.open)) / range;
        let pd = (k.close.min(k.open) - k.low) / range;
        (pb, pc, pu, pd)
    } else {
        (0.0, 0.5, 0.0, 0.0)
    };
    BarFeatures {
        body,
        range,
        p_b,
        p_c,
        p_u,
        p_d,
    }
}

/// Market context evaluation
pub fn evaluate_context(klines: &[Kline], indicators: &Indicators) -> ContextResult {
    let n = 10; // channel window
    let p = 5; // force comparison window
    let theta = 1.2; // narrow channel threshold

    let atr_m = indicators.atr20.unwrap_or(0.0);

    // Channel width
    let start = klines.len().saturating_sub(n);
    let window = &klines[start..];
    let h_n = window
        .iter()
        .map(|k| k.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let l_n = window.iter().map(|k| k.low).fold(f64::INFINITY, f64::min);

    let cr = if atr_m > 0.0 {
        (h_n - l_n) / atr_m
    } else {
        0.0
    };

    // Trend direction
    let curr_close = klines.last().map(|k| k.close).unwrap_or(0.0);
    let trend_dir = if let Some(ema20) = indicators.ema20 {
        if curr_close > ema20 {
            1
        } else if curr_close < ema20 {
            -1
        } else {
            0
        }
    } else {
        0
    };

    // Bullish/bearish force ratio
    let force_start = klines.len().saturating_sub(p);
    let force_window = &klines[force_start..];
    let f_bull: f64 = force_window
        .iter()
        .map(|k| (k.close - k.open).max(0.0))
        .sum();
    let f_bear: f64 = force_window
        .iter()
        .map(|k| (k.open - k.close).max(0.0))
        .sum();
    let f_ratio = if (f_bull + f_bear) > 0.0 {
        (f_bull - f_bear) / (f_bull + f_bear)
    } else {
        0.0
    };

    ContextResult {
        cr: (cr * 100.0).round() / 100.0,
        trend_dir,
        ma_cross_dir: detect_ma_cross(klines),
        f_ratio: (f_ratio * 100.0).round() / 100.0,
        is_valid: cr >= theta,
    }
}

pub fn detect_ma_cross(klines: &[Kline]) -> Option<i32> {
    if klines.len() < 6 {
        return None;
    }
    let search_end = klines.len();
    let search_start = search_end.saturating_sub(SIGNAL_BAR_MA_CROSS_LOOKBACK);
    for i in (search_start + 1)..search_end {
        let closes: Vec<f64> = klines[..=i].iter().map(|k| k.close).collect();
        if closes.len() < 2 {
            continue;
        }
        let ema5_vals = ema(&closes, 5);
        let ema20_vals = ema(&closes, 20);
        if ema5_vals.len() < 2 || ema20_vals.len() < 2 {
            continue;
        }
        let prev_ema5 = ema5_vals[ema5_vals.len() - 2];
        let prev_ema20 = ema20_vals[ema20_vals.len() - 2];
        let curr_ema5 = ema5_vals[ema5_vals.len() - 1];
        let curr_ema20 = ema20_vals[ema20_vals.len() - 1];
        if prev_ema5 <= prev_ema20 && curr_ema5 > curr_ema20 {
            return Some(1);
        }
        if prev_ema5 >= prev_ema20 && curr_ema5 < curr_ema20 {
            return Some(-1);
        }
    }
    None
}

pub fn compute_prev_f_ratio(klines: &[Kline], offset: usize, window: usize) -> f64 {
    let end = klines.len().saturating_sub(offset);
    let start = end.saturating_sub(window);
    if start >= end {
        return 0.0;
    }
    let slice = &klines[start..end];
    let f_bull: f64 = slice.iter().map(|k| (k.close - k.open).max(0.0)).sum();
    let f_bear: f64 = slice.iter().map(|k| (k.open - k.close).max(0.0)).sum();
    if (f_bull + f_bear) > 0.0 {
        ((f_bull - f_bear) / (f_bull + f_bear) * 100.0).round() / 100.0
    } else {
        0.0
    }
}

/// Long signal quality score
pub fn score_quality_long(f: &BarFeatures) -> f64 {
    if f.body <= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.55 && f.p_c >= 0.75 && f.p_u <= 0.15 {
        1.0
    } else if f.p_b >= 0.35 && f.p_c >= 0.55 && f.p_u <= 0.30 {
        0.6
    } else if f.p_b >= 0.25 && f.p_c >= 0.45 {
        0.3
    } else {
        0.0
    }
}

/// Short signal quality score
pub fn score_quality_short(f: &BarFeatures) -> f64 {
    if f.body >= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.55 && f.p_c <= 0.25 && f.p_d <= 0.15 {
        1.0
    } else if f.p_b >= 0.35 && f.p_c <= 0.45 && f.p_d <= 0.30 {
        0.6
    } else if f.p_b >= 0.25 && f.p_c <= 0.55 {
        0.3
    } else {
        0.0
    }
}

pub fn signal_bar_trend_allows(direction: &str, context: &ContextResult) -> bool {
    if !SIGNAL_BAR_REQUIRE_TREND_ALIGN {
        return true;
    }
    if SIGNAL_BAR_USE_MA_CROSS {
        return (direction == "long" && context.ma_cross_dir.unwrap_or(0) > 0)
            || (direction == "short" && context.ma_cross_dir.unwrap_or(0) < 0);
    }
    (direction == "long" && context.trend_dir > 0)
        || (direction == "short" && context.trend_dir < 0)
}

pub fn signal_bar_force_allows(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    (direction == "long" && (context.f_ratio > 0.0 || f_ratio_flipped))
        || (direction == "short" && (context.f_ratio < 0.0 || f_ratio_flipped))
}

pub fn signal_bar_context_allows(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    context.is_valid
        && signal_bar_trend_allows(direction, context)
        && signal_bar_force_allows(direction, context, f_ratio_flipped)
}

pub fn signal_bar_direction_label(direction: &str) -> &'static str {
    if direction == "long" {
        "\u{5411}\u{4e0a}"
    } else {
        "\u{5411}\u{4e0b}"
    }
}

pub fn signal_bar_alert_level(quality: f64, pattern_type: &str) -> &'static str {
    if quality >= 1.0 || pattern_type == "2k_reversal" || pattern_type == "surprise_bar" {
        "high"
    } else {
        "medium"
    }
}

pub fn signal_bar_alert_reason(
    direction: &str,
    pattern_label: &str,
    quality: f64,
    context: &ContextResult,
) -> String {
    format!(
        "盯盘提醒: {}出现{}K线，质量{:.1}，通道CR={:.2}，力量比={:.2}。用于提示未来数根K线可能有波动，需结合盘口/成交量/关键位人工确认。",
        signal_bar_direction_label(direction),
        pattern_label,
        quality,
        context.cr,
        context.f_ratio
    )
}

pub fn detect_signal_bar(klines: &[Kline]) -> Vec<Signal> {
    const MIN_KLINES: usize = 25;

    if klines.len() < MIN_KLINES {
        return Vec::new();
    }

    let curr_idx = klines.len() - 1;
    let prev_idx = curr_idx.saturating_sub(1);
    let curr = &klines[curr_idx];
    let prev = &klines[prev_idx];

    let indicators = compute_indicators(klines);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let atr20 = indicators.atr20.unwrap_or(0.0);
    let delta = SIGNAL_BAR_TICK_SIZE;

    let f_curr = compute_bar_features(curr);
    let f_prev = compute_bar_features(prev);

    let q_long = score_quality_long(&f_curr);
    let q_short = score_quality_short(&f_curr);
    let context = evaluate_context(klines, &indicators);
    let prev_f = compute_prev_f_ratio(klines, 5, 5);
    let long_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f < 0.0;
    let short_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f > 0.0;

    // Reference levels are for watchlist confirmation and compatibility with existing backtest fields, not automated trading signals.
    let make_signal = |direction: &str,
                       quality: f64,
                       signal_type: &str,
                       pattern_type: &str,
                       pattern_label: &str,
                       pattern_evidence: Value|
     -> Signal {
        let (entry, stop, target) = if direction == "long" {
            let e = curr.high + delta;
            let s = curr.low - delta;
            let t = e + 2.0 * (e - s);
            (e, s, t)
        } else {
            let e = curr.low - delta;
            let s = curr.high + delta;
            let t = e - 2.0 * (s - e);
            (e, s, t)
        };
        Signal {
            signal_type: signal_type.to_string(),
            direction: direction.to_string(),
            score: quality,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 100.0).round() / 100.0),
            stop_loss: Some((stop * 100.0).round() / 100.0),
            target_price: Some((target * 100.0).round() / 100.0),
            reason: signal_bar_alert_reason(direction, pattern_label, quality, &context),
            evidence: json!({
                "model_code": "pa_signal_bar_v1",
                "alert_purpose": "watchlist_monitor",
                "alert_semantics": "human_review_required",
                "alert_level": signal_bar_alert_level(quality, pattern_type),
                "watch_alert": {
                    "direction_hint": direction,
                    "direction_label": signal_bar_direction_label(direction),
                    "pattern_type": pattern_type,
                    "pattern_label": pattern_label,
                    "quality": quality,
                    "expected_use": "\u{63d0}\u{9192}\u{4eba}\u{5de5}\u{76ef}\u{76d8}\u{ff0c}\u{4e0d}\u{7b49}\u{540c}\u{4e8e}\u{81ea}\u{52a8}\u{4e70}\u{5356}\u{4fe1}\u{53f7}",
                    "review_checklist": [
                        "\u{4e0b}\u{4e00}\u{6839}K\u{7ebf}\u{662f}\u{5426}\u{5ef6}\u{7eed}\u{5e76}\u{653e}\u{91cf}",
                        "\u{662f}\u{5426}\u{9760}\u{8fd1}\u{524d}\u{9ad8}/\u{524d}\u{4f4e}/EMA20/\u{6574}\u{6570}\u{4f4d}\u{7b49}\u{5173}\u{952e}\u{4ef7}\u{4f4d}",
                        "\u{662f}\u{5426}\u{5148}\u{51fa}\u{73b0}\u{53cd}\u{5411}1ATR\u{7ea7}\u{522b}\u{6ce2}\u{52a8}",
                        "\u{82e5}\u{6ca1}\u{6709}\u{540e}\u{7eed}\u{786e}\u{8ba4}\u{5219}\u{5ffd}\u{7565}\u{63d0}\u{9192}"
                    ],
                    "reference_levels": {
                        "trigger_price": (entry * 100.0).round() / 100.0,
                        "invalidation_price": (stop * 100.0).round() / 100.0,
                        "observation_target": (target * 100.0).round() / 100.0
                    },
                    "historical_watch_eval": {
                        "dataset": "BTCUSDT 1h 2026-01-01..2026-03-30",
                        "sample_count_after_cooldown": 144,
                        "horizon_6bar": {
                            "opportunity_0_5atr_rate": 0.7014,
                            "opportunity_1atr_rate": 0.5069,
                            "noise_no_0_5atr_rate": 0.2986
                        },
                        "horizon_12bar": {
                            "opportunity_0_5atr_rate": 0.7917,
                            "opportunity_1atr_rate": 0.6319,
                            "noise_no_0_5atr_rate": 0.2083
                        },
                        "horizon_24bar": {
                            "opportunity_0_5atr_rate": 0.8611,
                            "opportunity_1atr_rate": 0.7569,
                            "noise_no_0_5atr_rate": 0.1389
                        }
                    }
                },
                "bar_features": {
                    "body": (f_curr.body * 10000.0).round() / 10000.0,
                    "range": (f_curr.range * 10000.0).round() / 10000.0,
                    "p_b": (f_curr.p_b * 100.0).round() / 100.0,
                    "p_c": (f_curr.p_c * 100.0).round() / 100.0,
                    "p_u": (f_curr.p_u * 100.0).round() / 100.0,
                    "p_d": (f_curr.p_d * 100.0).round() / 100.0,
                },
                "q_long": q_long,
                "q_short": q_short,
                "atr14": (atr14 * 10000.0).round() / 10000.0,
                "atr20": (atr20 * 10000.0).round() / 10000.0,
                "tick_size": SIGNAL_BAR_TICK_SIZE,
                "delta": (delta * 10000.0).round() / 10000.0,
                "optimized_params": {
                    "q_min": SIGNAL_BAR_Q_MIN,
                    "cooldown_bars": SIGNAL_BAR_COOLDOWN_BARS,
                    "require_pattern_context": SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT,
                    "require_trend_align": SIGNAL_BAR_REQUIRE_TREND_ALIGN,
                },
                "pattern": pattern_evidence,
            }),
        }
    };

    // ---- Priority 1: 2K reversal ----
    let prev_q_short = score_quality_short(&f_prev);
    let prev_q_long = score_quality_long(&f_prev);

    // Bullish 2K reversal: previous bearish signal K + current bullish signal K
    if prev_q_short >= SIGNAL_BAR_Q_MIN
        && q_long >= SIGNAL_BAR_Q_MIN
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long",
            q_long.min(prev_q_short),
            "pa_pattern",
            "2k_reversal",
            "2K\u{53cd}\u{8f6c}",
            json!({"type": "2k_reversal", "direction": "long"}),
        )];
    }
    // Bearish 2K reversal: previous bullish signal K + current bearish signal K
    if prev_q_long >= SIGNAL_BAR_Q_MIN
        && q_short >= SIGNAL_BAR_Q_MIN
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short",
            q_short.min(prev_q_long),
            "pa_pattern",
            "2k_reversal",
            "2K\u{53cd}\u{8f6c}",
            json!({"type": "2k_reversal", "direction": "short"}),
        )];
    }

    // ---- Priority 2: Engulfing ----
    // Bullish engulfing
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close > curr.open
        && f_curr.p_b >= 0.5
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("long", &context, long_f_ratio_flipped))
    {
        return vec![make_signal(
            "long",
            0.6,
            "pa_pattern",
            "engulfing",
            "\u{541e}\u{566c}\u{5f62}\u{6001}",
            json!({"type": "engulfing", "direction": "bullish"}),
        )];
    }
    // Bearish engulfing
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close < curr.open
        && f_curr.p_b >= 0.5
        && (!SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows("short", &context, short_f_ratio_flipped))
    {
        return vec![make_signal(
            "short",
            0.6,
            "pa_pattern",
            "engulfing",
            "\u{541e}\u{566c}\u{5f62}\u{6001}",
            json!({"type": "engulfing", "direction": "bearish"}),
        )];
    }

    // ---- Priority 3: Surprise bar ----
    let surprise_start = klines.len().saturating_sub(SIGNAL_BAR_SURPRISE_LOOKBACK);
    let r_max = klines[surprise_start..curr_idx]
        .iter()
        .map(|k| k.high - k.low)
        .fold(0.0_f64, f64::max);
    if r_max > 0.0 && f_curr.range > 1.5 * r_max && f_curr.p_b >= 0.5 {
        let direction = if f_curr.body > 0.0 { "long" } else { "short" };
        let f_ratio_flipped = if direction == "long" {
            long_f_ratio_flipped
        } else {
            short_f_ratio_flipped
        };
        if SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT
            && !signal_bar_context_allows(direction, &context, f_ratio_flipped)
        {
            return Vec::new();
        }
        return vec![make_signal(
            direction,
            0.6,
            "pa_pattern",
            "surprise_bar",
            "\u{60ca}\u{559c}K\u{7ebf}",
            json!({"type": "surprise_bar", "direction": direction, "range": (f_curr.range * 10000.0).round() / 10000.0, "r_max": (r_max * 10000.0).round() / 10000.0}),
        )];
    }

    // ---- Priority 4: Regular signal K-line ----
    // Bullish regular signal
    if q_long >= SIGNAL_BAR_Q_MIN && context.is_valid {
        if signal_bar_context_allows("long", &context, long_f_ratio_flipped) {
            return vec![make_signal(
                "long",
                q_long,
                "pa_signal_bar",
                "signal_bar",
                "\u{5e38}\u{89c4}\u{4fe1}\u{53f7}K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": long_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    // Bearish regular signal
    if q_short >= SIGNAL_BAR_Q_MIN && context.is_valid {
        if signal_bar_context_allows("short", &context, short_f_ratio_flipped) {
            return vec![make_signal(
                "short",
                q_short,
                "pa_signal_bar",
                "signal_bar",
                "\u{5e38}\u{89c4}\u{4fe1}\u{53f7}K",
                json!({
                    "type": "signal_bar",
                    "context": {
                        "cr": context.cr,
                        "trend_dir": context.trend_dir,
                        "f_ratio": context.f_ratio,
                        "is_valid": context.is_valid,
                        "f_ratio_flipped": short_f_ratio_flipped,
                    },
                }),
            )];
        }
    }

    Vec::new()
}
