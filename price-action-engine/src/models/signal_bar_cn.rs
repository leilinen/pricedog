use crate::backtest::{
    backtest_trades_from_model_long_only, generate_sample_candidates_from_model, AS_COOLDOWN_BARS,
    AS_STOP_ATR_MULT, AS_TARGET_ATR_MULT,
};
use crate::model::PriceActionModel;
use crate::models::signal_bar::{detect_signal_bar_generic, SignalBarConfig};
use crate::types::{BarFeatures, Kline, Signal};

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
    fn code(&self) -> &str { "pa_signal_bar_cn_v1" }
    fn name(&self) -> &str { "pricedog_pa_signal_bar_cn" }
    fn version(&self) -> &str { "v1" }
    fn min_klines(&self) -> usize { 25 }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_signal_bar_cn(klines)
    }

    fn backtest(
        &self, klines: &[Kline], max_holding_bars: usize, fee_bps: f64, slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
        backtest_trades_from_model_long_only(self, klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self, klines: &[Kline], max_candidates: usize,
    ) -> Vec<crate::types::SampleCandidate> {
        generate_sample_candidates_from_model(self, klines, max_candidates)
    }
}

// ---------------------------------------------------------------------------
// A-share scoring — relaxed thresholds vs crypto
// ---------------------------------------------------------------------------

pub fn score_quality_long_as(f: &BarFeatures) -> f64 {
    if f.body <= 0.0 { return 0.0; }
    if f.p_b >= 0.45 && f.p_c >= 0.65 && f.p_u <= 0.20 { 1.0 }
    else if f.p_b >= 0.30 && f.p_c >= 0.50 && f.p_u <= 0.35 { 0.6 }
    else if f.p_b >= 0.20 && f.p_c >= 0.40 { 0.3 }
    else { 0.0 }
}

pub fn score_quality_short_as(f: &BarFeatures) -> f64 {
    if f.body >= 0.0 { return 0.0; }
    if f.p_b >= 0.45 && f.p_c <= 0.35 && f.p_d <= 0.20 { 1.0 }
    else if f.p_b >= 0.30 && f.p_c <= 0.50 && f.p_d <= 0.35 { 0.6 }
    else if f.p_b >= 0.20 && f.p_c <= 0.60 { 0.3 }
    else { 0.0 }
}

// ---------------------------------------------------------------------------
// A-share entry / stop / target — ATR-based
// ---------------------------------------------------------------------------

fn cn_entry_stop_target(direction: &str, curr: &Kline, atr14: f64, delta: f64) -> (f64, f64, f64) {
    if direction == "long" {
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
    }
}

// ---------------------------------------------------------------------------
// CN detection — delegates to shared generic with CN config
// ---------------------------------------------------------------------------

pub fn detect_signal_bar_cn(klines: &[Kline]) -> Vec<Signal> {
    static CONFIG: std::sync::LazyLock<SignalBarConfig> = std::sync::LazyLock::new(|| SignalBarConfig {
        q_min: AS_Q_MIN,
        tick_size: AS_TICK_SIZE,
        require_pattern_context: AS_REQUIRE_PATTERN_CONTEXT,
        require_trend_align: AS_REQUIRE_TREND_ALIGN,
        surprise_lookback: AS_SURPRISE_LOOKBACK,
        cooldown_bars: AS_COOLDOWN_BARS,
        model_code: "pa_signal_bar_cn_v1",
        price_precision: 10000.0,
        score_long: score_quality_long_as,
        score_short: score_quality_short_as,
        entry_stop_target: cn_entry_stop_target,
        use_alert_hint: true,
        expected_use: "看多提醒：关注买入机会，下一根K线确认后可考虑建仓/加仓",
    });
    detect_signal_bar_generic(klines, &CONFIG)
}
