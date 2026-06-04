use crate::backtest::{
    backtest_trades_from_model_long_only, generate_sample_candidates_from_model, AS_COOLDOWN_BARS,
    AS_STOP_ATR_MULT, AS_TARGET_ATR_MULT,
};
use crate::indicators::compute_indicators;
use crate::model::PriceActionModel;
use crate::models::signal_bar::{
    compute_bar_features, compute_prev_f_ratio, evaluate_context, signal_bar_alert_level, signal_bar_alert_reason,
    signal_bar_direction_label, signal_bar_force_allows,
};
use crate::types::{ContextResult, Kline, Signal};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const AS_Q_MIN: f64 = 0.3;
pub const AS_REQUIRE_PATTERN_CONTEXT: bool = true;
pub const AS_REQUIRE_TREND_ALIGN: bool = false;
pub const AS_SURPRISE_LOOKBACK: usize = 20;
pub const AS_TICK_SIZE: f64 = 0.001;

// ---------------------------------------------------------------------------
// SignalBarModelCn struct + trait impl
// ---------------------------------------------------------------------------

pub struct SignalBarModelCn;

impl PriceActionModel for SignalBarModelCn {
    fn code(&self) -> &str {
        "pa_signal_bar_cn_v1"
    }

    fn name(&self) -> &str {
        "pricedog_pa_signal_bar_cn"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn min_klines(&self) -> usize {
        25
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar_cn(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
        // A-share only simulates long trades, short signals are watchlist alerts only
        backtest_trades_from_model_long_only(self, klines, max_holding_bars, fee_bps, slippage_bps)
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
// A-share signal K-line quality scoring
// ---------------------------------------------------------------------------

/// A-share long signal quality score (relaxed criteria)
pub fn score_quality_long_as(f: &crate::types::BarFeatures) -> f64 {
    if f.body <= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.45 && f.p_c >= 0.65 && f.p_u <= 0.20 {
        1.0
    } else if f.p_b >= 0.30 && f.p_c >= 0.50 && f.p_u <= 0.35 {
        0.6
    } else if f.p_b >= 0.20 && f.p_c >= 0.40 {
        0.3
    } else {
        0.0
    }
}

/// A-share short signal quality score (relaxed criteria)
pub fn score_quality_short_as(f: &crate::types::BarFeatures) -> f64 {
    if f.body >= 0.0 {
        return 0.0;
    }
    if f.p_b >= 0.45 && f.p_c <= 0.35 && f.p_d <= 0.20 {
        1.0
    } else if f.p_b >= 0.30 && f.p_c <= 0.50 && f.p_d <= 0.35 {
        0.6
    } else if f.p_b >= 0.20 && f.p_c <= 0.60 {
        0.3
    } else {
        0.0
    }
}

pub fn signal_bar_trend_allows_cn(direction: &str, context: &ContextResult) -> bool {
    if !AS_REQUIRE_TREND_ALIGN {
        return true;
    }
    (direction == "long" && context.trend_dir > 0)
        || (direction == "short" && context.trend_dir < 0)
}

pub fn signal_bar_context_allows_cn(
    direction: &str,
    context: &ContextResult,
    f_ratio_flipped: bool,
) -> bool {
    context.is_valid
        && signal_bar_trend_allows_cn(direction, context)
        && signal_bar_force_allows(direction, context, f_ratio_flipped)
}

/// A-share signal K-line detection: relaxed quality threshold, ATR-based stop/target
pub fn detect_signal_bar_cn(klines: &[Kline]) -> Vec<Signal> {
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
    let delta = AS_TICK_SIZE;

    let f_curr = compute_bar_features(curr);
    let f_prev = compute_bar_features(prev);

    let q_long = score_quality_long_as(&f_curr);
    let q_short = score_quality_short_as(&f_curr);
    let context = evaluate_context(klines, &indicators);
    let prev_f = compute_prev_f_ratio(klines, 5, 5);
    let long_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f < 0.0;
    let short_f_ratio_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f > 0.0;

    let make_signal = |direction: &str,
                       quality: f64,
                       signal_type: &str,
                       pattern_type: &str,
                       pattern_label: &str,
                       pattern_evidence: Value|
     -> Signal {
        // ATR-based stop/target
        let (entry, stop, target) = if direction == "long" {
            let e = curr.close + delta;
            let risk = atr14 * AS_STOP_ATR_MULT;
            let s = e - risk;
            let t = e + atr14 * AS_TARGET_ATR_MULT;
            (e, s, t)
        } else {
            let e = curr.close - delta;
            let risk = atr14 * AS_STOP_ATR_MULT;
            let s = e + risk;
            let t = e - atr14 * AS_TARGET_ATR_MULT;
            (e, s, t)
        };
        // A-share watchlist signal: bullish prompts buy opportunity, bearish prompts risk (reduce/take profit)
        let (alert_hint, expected_use) = if direction == "long" {
            ("bullish_watch", "\u{770b}\u{591a}\u{63d0}\u{9192}\u{ff1a}\u{5173}\u{6ce8}\u{4e70}\u{5165}\u{673a}\u{4f1a}\u{ff0c}\u{4e0b}\u{4e00}\u{6839}K\u{7ebf}\u{786e}\u{8ba4}\u{540e}\u{53ef}\u{8003}\u{8651}\u{5efa}\u{4ed3}/\u{52a0}\u{4ed3}")
        } else {
            ("bearish_watch", "\u{770b}\u{7a7a}\u{63d0}\u{9192}\u{ff1a}\u{6ce8}\u{610f}\u{98ce}\u{9669}\u{ff0c}\u{8003}\u{8651}\u{51cf}\u{4ed3}/\u{6b62}\u{76c8}\u{ff0c}A\u{80a1}\u{4e0d}\u{53ef}\u{505a}\u{7a7a}")
        };
        Signal {
            signal_type: signal_type.to_string(),
            direction: direction.to_string(),
            score: quality,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 10000.0).round() / 10000.0),
            stop_loss: Some((stop * 10000.0).round() / 10000.0),
            target_price: Some((target * 10000.0).round() / 10000.0),
            reason: signal_bar_alert_reason(direction, pattern_label, quality, &context),
            evidence: json!({
                "model_code": "pa_signal_bar_cn_v1",
                "alert_purpose": "watchlist_monitor",
                "alert_semantics": "human_review_required",
                "alert_level": signal_bar_alert_level(quality, pattern_type),
                "watch_alert": {
                    "alert_hint": alert_hint,
                    "direction_hint": direction,
                    "direction_label": signal_bar_direction_label(direction),
                    "pattern_type": pattern_type,
                    "pattern_label": pattern_label,
                    "quality": quality,
                    "expected_use": expected_use,
                    "review_checklist": [
                        "\u{4e0b}\u{4e00}\u{6839}K\u{7ebf}\u{662f}\u{5426}\u{5ef6}\u{7eed}\u{5e76}\u{653e}\u{91cf}",
                        "\u{662f}\u{5426}\u{9760}\u{8fd1}\u{524d}\u{9ad8}/\u{524d}\u{4f4e}/EMA20/\u{6574}\u{6570}\u{4f4d}\u{7b49}\u{5173}\u{952e}\u{4ef7}\u{4f4d}",
                        "\u{662f}\u{5426}\u{5148}\u{51fa}\u{73b0}\u{53cd}\u{5411}1ATR\u{7ea7}\u{522b}\u{6ce2}\u{52a8}",
                        "\u{82e5}\u{6ca1}\u{6709}\u{540e}\u{7eed}\u{786e}\u{8ba4}\u{5219}\u{5ffd}\u{7565}\u{63d0}\u{9192}"
                    ],
                    "reference_levels": {
                        "signal_bar_high": (curr.high * 10000.0).round() / 10000.0,
                        "signal_bar_low": (curr.low * 10000.0).round() / 10000.0,
                        "atr14": (atr14 * 10000.0).round() / 10000.0,
                    },
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
                "tick_size": AS_TICK_SIZE,
                "delta": (delta * 10000.0).round() / 10000.0,
                "optimized_params": {
                    "q_min": AS_Q_MIN,
                    "cooldown_bars": AS_COOLDOWN_BARS,
                    "require_pattern_context": AS_REQUIRE_PATTERN_CONTEXT,
                    "require_trend_align": AS_REQUIRE_TREND_ALIGN,
                },
                "pattern": pattern_evidence,
            }),
        }
    };

    // ---- Priority 1: 2K reversal ----
    let prev_q_short = score_quality_short_as(&f_prev);
    let prev_q_long = score_quality_long_as(&f_prev);

    if prev_q_short >= AS_Q_MIN
        && q_long >= AS_Q_MIN
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped))
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
    if prev_q_long >= AS_Q_MIN
        && q_short >= AS_Q_MIN
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped))
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
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close > curr.open
        && f_curr.p_b >= 0.5
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped))
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
    if curr.high > prev.high
        && curr.low < prev.low
        && curr.close < curr.open
        && f_curr.p_b >= 0.5
        && (!AS_REQUIRE_PATTERN_CONTEXT
            || signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped))
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
    let surprise_start = klines.len().saturating_sub(AS_SURPRISE_LOOKBACK);
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
        if AS_REQUIRE_PATTERN_CONTEXT
            && !signal_bar_context_allows_cn(direction, &context, f_ratio_flipped)
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
    if q_long >= AS_Q_MIN && context.is_valid {
        if signal_bar_context_allows_cn("long", &context, long_f_ratio_flipped) {
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

    if q_short >= AS_Q_MIN && context.is_valid {
        if signal_bar_context_allows_cn("short", &context, short_f_ratio_flipped) {
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
