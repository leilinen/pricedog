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
// Shared parameterised config — drives both crypto and CN detection
// ---------------------------------------------------------------------------

pub struct SignalBarConfig {
    pub q_min: f64,
    pub tick_size: f64,
    pub require_pattern_context: bool,
    pub require_trend_align: bool,
    pub surprise_lookback: usize,
    pub cooldown_bars: usize,
    pub model_code: &'static str,
    /// Price rounding factor: 100.0 → 2 decimals (crypto), 10000.0 → 4 decimals (CN)
    pub price_precision: f64,
    /// Scoring functions
    pub score_long: fn(&BarFeatures) -> f64,
    pub score_short: fn(&BarFeatures) -> f64,
    /// Entry / stop / target calculation
    pub entry_stop_target: fn(direction: &str, curr: &Kline, atr14: f64, delta: f64) -> (f64, f64, f64),
    /// Whether to include `alert_hint` in watch_alert evidence (CN-only)
    pub use_alert_hint: bool,
    /// watch_alert `expected_use` text
    pub expected_use: &'static str,
}

// ---------------------------------------------------------------------------
// SignalBarModel struct + trait impl
// ---------------------------------------------------------------------------

pub struct SignalBarModel;

impl PriceActionModel for SignalBarModel {
    fn code(&self) -> &str { "pa_signal_bar_v1" }
    fn name(&self) -> &str { "pricedog_pa_signal_bar" }
    fn version(&self) -> &str { "v1" }
    fn min_klines(&self) -> usize { 25 }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar(klines)
    }

    fn backtest(
        &self, klines: &[Kline], max_holding_bars: usize, fee_bps: f64, slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
        backtest_trades_from_model(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self, klines: &[Kline], max_candidates: usize,
    ) -> Vec<crate::types::SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

// ---------------------------------------------------------------------------
// Entry / stop / target functions
// ---------------------------------------------------------------------------

/// Crypto: bar-range-based entry, stop at bar extremes ± delta, target = 1:2 R:R
pub fn crypto_entry_stop_target(
    direction: &str, curr: &Kline, _atr14: f64, delta: f64,
) -> (f64, f64, f64) {
    if direction == "long" {
        let e = curr.high + delta;
        let s = curr.low - delta;
        (e, s, e + 2.0 * (e - s))
    } else {
        let e = curr.low - delta;
        let s = curr.high + delta;
        (e, s, e - 2.0 * (s - e))
    }
}

// ---------------------------------------------------------------------------
// SignalBarModel core functions
// ---------------------------------------------------------------------------

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
    BarFeatures { body, range, p_b, p_c, p_u, p_d }
}

pub fn evaluate_context(klines: &[Kline], indicators: &Indicators) -> ContextResult {
    let n = 10;   // channel window
    let p = 5;    // force comparison window
    let theta = 1.2; // narrow channel threshold

    let atr_m = indicators.atr20.unwrap_or(0.0);

    let start = klines.len().saturating_sub(n);
    let window = &klines[start..];
    let h_n = window.iter().map(|k| k.high).fold(f64::NEG_INFINITY, f64::max);
    let l_n = window.iter().map(|k| k.low).fold(f64::INFINITY, f64::min);

    let cr = if atr_m > 0.0 { (h_n - l_n) / atr_m } else { 0.0 };

    let curr_close = klines.last().map(|k| k.close).unwrap_or(0.0);
    let trend_dir = if let Some(ema20) = indicators.ema20 {
        if curr_close > ema20 { 1 }
        else if curr_close < ema20 { -1 }
        else { 0 }
    } else { 0 };

    let force_start = klines.len().saturating_sub(p);
    let force_window = &klines[force_start..];
    let f_bull: f64 = force_window.iter().map(|k| (k.close - k.open).max(0.0)).sum();
    let f_bear: f64 = force_window.iter().map(|k| (k.open - k.close).max(0.0)).sum();
    let f_ratio = if (f_bull + f_bear) > 0.0 {
        (f_bull - f_bear) / (f_bull + f_bear)
    } else { 0.0 };

    ContextResult {
        cr: (cr * 100.0).round() / 100.0,
        trend_dir,
        ma_cross_dir: detect_ma_cross(klines),
        f_ratio: (f_ratio * 100.0).round() / 100.0,
        is_valid: cr >= theta,
    }
}

pub fn detect_ma_cross(klines: &[Kline]) -> Option<i32> {
    if klines.len() < 6 { return None; }
    let search_end = klines.len();
    let search_start = search_end.saturating_sub(SIGNAL_BAR_MA_CROSS_LOOKBACK);
    for i in (search_start + 1)..search_end {
        let closes: Vec<f64> = klines[..=i].iter().map(|k| k.close).collect();
        if closes.len() < 2 { continue; }
        let ema5_vals = ema(&closes, 5);
        let ema20_vals = ema(&closes, 20);
        if ema5_vals.len() < 2 || ema20_vals.len() < 2 { continue; }
        let prev_ema5 = ema5_vals[ema5_vals.len() - 2];
        let prev_ema20 = ema20_vals[ema20_vals.len() - 2];
        let curr_ema5 = ema5_vals[ema5_vals.len() - 1];
        let curr_ema20 = ema20_vals[ema20_vals.len() - 1];
        if prev_ema5 <= prev_ema20 && curr_ema5 > curr_ema20 { return Some(1); }
        if prev_ema5 >= prev_ema20 && curr_ema5 < curr_ema20 { return Some(-1); }
    }
    None
}

pub fn compute_prev_f_ratio(klines: &[Kline], offset: usize, window: usize) -> f64 {
    let end = klines.len().saturating_sub(offset);
    let start = end.saturating_sub(window);
    if start >= end { return 0.0; }
    let slice = &klines[start..end];
    let f_bull: f64 = slice.iter().map(|k| (k.close - k.open).max(0.0)).sum();
    let f_bear: f64 = slice.iter().map(|k| (k.open - k.close).max(0.0)).sum();
    if (f_bull + f_bear) > 0.0 {
        ((f_bull - f_bear) / (f_bull + f_bear) * 100.0).round() / 100.0
    } else { 0.0 }
}

// ---------------------------------------------------------------------------
// Crypto quality scoring
// ---------------------------------------------------------------------------

pub fn score_quality_long(f: &BarFeatures) -> f64 {
    if f.body <= 0.0 { return 0.0; }
    if f.p_b >= 0.55 && f.p_c >= 0.75 && f.p_u <= 0.15 { 1.0 }
    else if f.p_b >= 0.35 && f.p_c >= 0.55 && f.p_u <= 0.30 { 0.6 }
    else if f.p_b >= 0.25 && f.p_c >= 0.45 { 0.3 }
    else { 0.0 }
}

pub fn score_quality_short(f: &BarFeatures) -> f64 {
    if f.body >= 0.0 { return 0.0; }
    if f.p_b >= 0.55 && f.p_c <= 0.25 && f.p_d <= 0.15 { 1.0 }
    else if f.p_b >= 0.35 && f.p_c <= 0.45 && f.p_d <= 0.30 { 0.6 }
    else if f.p_b >= 0.25 && f.p_c <= 0.55 { 0.3 }
    else { 0.0 }
}

// ---------------------------------------------------------------------------
// Context / trend / force helpers
// ---------------------------------------------------------------------------

pub fn signal_bar_trend_allows(direction: &str, context: &ContextResult) -> bool {
    if !SIGNAL_BAR_REQUIRE_TREND_ALIGN { return true; }
    if SIGNAL_BAR_USE_MA_CROSS {
        return (direction == "long" && context.ma_cross_dir.unwrap_or(0) > 0)
            || (direction == "short" && context.ma_cross_dir.unwrap_or(0) < 0);
    }
    (direction == "long" && context.trend_dir > 0)
        || (direction == "short" && context.trend_dir < 0)
}

pub fn signal_bar_force_allows(direction: &str, context: &ContextResult, f_ratio_flipped: bool) -> bool {
    (direction == "long" && (context.f_ratio > 0.0 || f_ratio_flipped))
        || (direction == "short" && (context.f_ratio < 0.0 || f_ratio_flipped))
}

pub fn signal_bar_direction_label(direction: &str) -> &'static str {
    if direction == "long" { "向上" } else { "向下" }
}

pub fn signal_bar_alert_level(quality: f64, pattern_type: &str) -> &'static str {
    if quality >= 1.0 || pattern_type == "2k_reversal" || pattern_type == "surprise_bar" { "high" }
    else { "medium" }
}

pub fn signal_bar_alert_reason(
    direction: &str, pattern_label: &str, quality: f64, context: &ContextResult,
) -> String {
    format!(
        "盯盘提醒: {}出现{}K线，质量{:.1}，通道CR={:.2}，力量比={:.2}。用于提示未来数根K线可能有波动，需结合盘口/成交量/关键位人工确认。",
        signal_bar_direction_label(direction), pattern_label, quality, context.cr, context.f_ratio,
    )
}

// ---------------------------------------------------------------------------
// Shared signal detection — driven by SignalBarConfig
// ---------------------------------------------------------------------------

/// Build the evidence `Value` for a signal, parameterised by config.
fn build_evidence(
    config: &SignalBarConfig,
    direction: &str, quality: f64, _signal_type: &str,
    pattern_type: &str, pattern_label: &str,
    pattern_evidence: &Value,
    curr: &Kline, f_curr: &BarFeatures,
    entry: f64, stop: f64, target: f64,
    q_long: f64, q_short: f64, atr14: f64, atr20: f64, delta: f64,
) -> Value {
    let prec = config.price_precision;
    let alert_label = signal_bar_direction_label(direction);

    // Build watch_alert reference_levels
    let ref_levels = if config.use_alert_hint {
        json!({
            "signal_bar_high": (curr.high * prec).round() / prec,
            "signal_bar_low": (curr.low * prec).round() / prec,
            "atr14": (atr14 * prec).round() / prec,
        })
    } else {
        json!({
            "trigger_price": (entry * 100.0).round() / 100.0,
            "invalidation_price": (stop * 100.0).round() / 100.0,
            "observation_target": (target * 100.0).round() / 100.0,
        })
    };

    // Build watch_alert
    let mut wa = serde_json::Map::new();
    if config.use_alert_hint {
        let hint = if direction == "long" { "bullish_watch" } else { "bearish_watch" };
        wa.insert("alert_hint".to_string(), json!(hint));
    }
    wa.insert("direction_hint".to_string(), json!(direction));
    wa.insert("direction_label".to_string(), json!(alert_label));
    wa.insert("pattern_type".to_string(), json!(pattern_type));
    wa.insert("pattern_label".to_string(), json!(pattern_label));
    wa.insert("quality".to_string(), json!(quality));
    wa.insert("expected_use".to_string(), json!(config.expected_use));
    wa.insert("review_checklist".to_string(), json!([
        "下一根K线是否延续并放量",
        "是否靠近前高/前低/EMA20/整数位等关键价位",
        "是否先出现反向1ATR级别波动",
        "若没有后续确认则忽略提醒",
    ]));
    wa.insert("reference_levels".to_string(), ref_levels);

    // historical_watch_eval only for crypto
    if !config.use_alert_hint {
        wa.insert("historical_watch_eval".to_string(), json!({
            "dataset": "BTCUSDT 1h 2026-01-01..2026-03-30",
            "sample_count_after_cooldown": 144,
            "horizon_6bar": {
                "opportunity_0_5atr_rate": 0.7014,
                "opportunity_1atr_rate": 0.5069,
                "noise_no_0_5atr_rate": 0.2986,
            },
            "horizon_12bar": {
                "opportunity_0_5atr_rate": 0.7917,
                "opportunity_1atr_rate": 0.6319,
                "noise_no_0_5atr_rate": 0.2083,
            },
            "horizon_24bar": {
                "opportunity_0_5atr_rate": 0.8611,
                "opportunity_1atr_rate": 0.7569,
                "noise_no_0_5atr_rate": 0.1389,
            },
        }));
    }

    json!({
        "model_code": config.model_code,
        "alert_purpose": "watchlist_monitor",
        "alert_semantics": "human_review_required",
        "alert_level": signal_bar_alert_level(quality, pattern_type),
        "watch_alert": wa,
        "bar_features": {
            "body": (f_curr.body * prec).round() / prec,
            "range": (f_curr.range * prec).round() / prec,
            "p_b": (f_curr.p_b * 100.0).round() / 100.0,
            "p_c": (f_curr.p_c * 100.0).round() / 100.0,
            "p_u": (f_curr.p_u * 100.0).round() / 100.0,
            "p_d": (f_curr.p_d * 100.0).round() / 100.0,
        },
        "q_long": q_long,
        "q_short": q_short,
        "atr14": (atr14 * prec).round() / prec,
        "atr20": (atr20 * prec).round() / prec,
        "tick_size": config.tick_size,
        "delta": (delta * prec).round() / prec,
        "optimized_params": {
            "q_min": config.q_min,
            "cooldown_bars": config.cooldown_bars,
            "require_pattern_context": config.require_pattern_context,
            "require_trend_align": config.require_trend_align,
        },
        "pattern": pattern_evidence,
    })
}

/// Shared detection logic, parameterised by `config`.
pub fn detect_signal_bar_generic(klines: &[Kline], config: &SignalBarConfig) -> Vec<Signal> {
    const MIN_KLINES: usize = 25;
    if klines.len() < MIN_KLINES { return Vec::new(); }

    let curr_idx = klines.len() - 1;
    let prev_idx = curr_idx.saturating_sub(1);
    let curr = &klines[curr_idx];
    let prev = &klines[prev_idx];

    let indicators = compute_indicators(klines);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let atr20 = indicators.atr20.unwrap_or(0.0);
    let delta = config.tick_size;

    let f_curr = compute_bar_features(curr);
    let f_prev = compute_bar_features(prev);

    let q_long = (config.score_long)(&f_curr);
    let q_short = (config.score_short)(&f_curr);
    let context = evaluate_context(klines, &indicators);
    let prev_f = compute_prev_f_ratio(klines, 5, 5);
    let long_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f < 0.0;
    let short_flipped = context.f_ratio.signum() != prev_f.signum() && prev_f > 0.0;

    let context_ok = |dir: &str, flipped: bool| -> bool {
        if !config.require_pattern_context { return true; }
        context.is_valid
            && signal_bar_force_allows(dir, &context, flipped)
            && {
                if !config.require_trend_align { true }
                else { signal_bar_trend_allows(dir, &context) }
            }
    };

    let make_signal = |direction: &str, quality: f64, signal_type: &str,
                       pattern_type: &str, pattern_label: &str,
                       pattern_evidence: Value| -> Signal {
        let (entry, stop, target) = (config.entry_stop_target)(direction, curr, atr14, delta);
        Signal {
            signal_type: signal_type.to_string(),
            direction: direction.to_string(),
            score: quality,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * config.price_precision).round() / config.price_precision),
            stop_loss: Some((stop * config.price_precision).round() / config.price_precision),
            target_price: Some((target * config.price_precision).round() / config.price_precision),
            reason: signal_bar_alert_reason(direction, pattern_label, quality, &context),
            evidence: build_evidence(
                config, direction, quality, signal_type, pattern_type, pattern_label,
                &pattern_evidence, curr, &f_curr, entry, stop, target,
                q_long, q_short, atr14, atr20, delta,
            ),
        }
    };

    // ---- Priority 1: 2K reversal ----
    let prev_q_short = (config.score_short)(&f_prev);
    let prev_q_long = (config.score_long)(&f_prev);

    if prev_q_short >= config.q_min && q_long >= config.q_min && context_ok("long", long_flipped) {
        return vec![make_signal("long", q_long.min(prev_q_short), "pa_pattern",
            "2k_reversal", "2K反转", json!({"type": "2k_reversal", "direction": "long"}))];
    }
    if prev_q_long >= config.q_min && q_short >= config.q_min && context_ok("short", short_flipped) {
        return vec![make_signal("short", q_short.min(prev_q_long), "pa_pattern",
            "2k_reversal", "2K反转", json!({"type": "2k_reversal", "direction": "short"}))];
    }

    // ---- Priority 2: Engulfing ----
    let engulf_cond = curr.high > prev.high && curr.low < prev.low && f_curr.p_b >= 0.5;
    if engulf_cond && curr.close > curr.open && context_ok("long", long_flipped) {
        return vec![make_signal("long", 0.6, "pa_pattern",
            "engulfing", "吞没形态", json!({"type": "engulfing", "direction": "bullish"}))];
    }
    if engulf_cond && curr.close < curr.open && context_ok("short", short_flipped) {
        return vec![make_signal("short", 0.6, "pa_pattern",
            "engulfing", "吞没形态", json!({"type": "engulfing", "direction": "bearish"}))];
    }

    // ---- Priority 3: Surprise bar ----
    let surprise_start = klines.len().saturating_sub(config.surprise_lookback);
    let r_max = klines[surprise_start..curr_idx]
        .iter().map(|k| k.high - k.low).fold(0.0_f64, f64::max);
    if r_max > 0.0 && f_curr.range > 1.5 * r_max && f_curr.p_b >= 0.5 {
        let direction = if f_curr.body > 0.0 { "long" } else { "short" };
        let flipped = if direction == "long" { long_flipped } else { short_flipped };
        if config.require_pattern_context && !context_ok(direction, flipped) {
            return Vec::new();
        }
        return vec![make_signal(direction, 0.6, "pa_pattern",
            "surprise_bar", "惊喜K线",
            json!({"type": "surprise_bar", "direction": direction,
                   "range": (f_curr.range * 10000.0).round() / 10000.0,
                   "r_max": (r_max * 10000.0).round() / 10000.0}))];
    }

    // ---- Priority 4: Regular signal K-line ----
    let context_ctx = |flipped: bool| json!({
        "type": "signal_bar",
        "context": {
            "cr": context.cr, "trend_dir": context.trend_dir,
            "f_ratio": context.f_ratio, "is_valid": context.is_valid,
            "f_ratio_flipped": flipped,
        },
    });

    if q_long >= config.q_min && context.is_valid {
        if context_ok("long", long_flipped) {
            return vec![make_signal("long", q_long, "pa_signal_bar",
                "signal_bar", "常规信号K", context_ctx(long_flipped))];
        }
    }
    if q_short >= config.q_min && context.is_valid {
        if context_ok("short", short_flipped) {
            return vec![make_signal("short", q_short, "pa_signal_bar",
                "signal_bar", "常规信号K", context_ctx(short_flipped))];
        }
    }

    Vec::new()
}

/// Crypto model: uses `SIGNAL_BAR_*` constants, crypto scoring, bar-range entry/stop/target.
pub fn detect_signal_bar(klines: &[Kline]) -> Vec<Signal> {
    static CONFIG: std::sync::LazyLock<SignalBarConfig> = std::sync::LazyLock::new(|| SignalBarConfig {
        q_min: SIGNAL_BAR_Q_MIN,
        tick_size: SIGNAL_BAR_TICK_SIZE,
        require_pattern_context: SIGNAL_BAR_REQUIRE_PATTERN_CONTEXT,
        require_trend_align: SIGNAL_BAR_REQUIRE_TREND_ALIGN,
        surprise_lookback: SIGNAL_BAR_SURPRISE_LOOKBACK,
        cooldown_bars: SIGNAL_BAR_COOLDOWN_BARS,
        model_code: "pa_signal_bar_v1",
        price_precision: 100.0,
        score_long: score_quality_long,
        score_short: score_quality_short,
        entry_stop_target: crypto_entry_stop_target,
        use_alert_hint: false,
        expected_use: "提醒人工盯盘，不等同于自动买卖信号",
    });
    detect_signal_bar_generic(klines, &CONFIG)
}
