use crate::backtest::{
    backtest_trades_from_model_long_only, generate_sample_candidates_from_model, AS_STOP_ATR_MULT,
    AS_TARGET_ATR_MULT,
};
use crate::indicators::{compute_indicators, ema};
use crate::model::PriceActionModel;
use crate::types::{Kline, Signal};
use serde_json::json;

// ---------------------------------------------------------------------------
// Ema20CrossModelCn struct + trait impl
// ---------------------------------------------------------------------------

pub struct Ema20CrossModelCn;

impl PriceActionModel for Ema20CrossModelCn {
    fn code(&self) -> &str {
        "pa_ema20_cross_cn_v1"
    }
    fn name(&self) -> &str {
        "pricedog_pa_ema20_cross_cn"
    }
    fn version(&self) -> &str {
        "v1"
    }
    fn min_klines(&self) -> usize {
        22
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_ema20_cross_cn(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
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

/// A-share EMA20 cross detection: bullish cross gives long alert, bearish cross gives short alert
pub fn detect_ema20_cross_cn(klines: &[Kline]) -> Vec<Signal> {
    if klines.len() < 22 {
        return Vec::new();
    }
    let curr = klines.last().unwrap();
    let prev = &klines[klines.len() - 2];
    let indicators = compute_indicators(klines);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let curr_ema20 = indicators.ema20.unwrap_or(0.0);
    let prev_closes: Vec<f64> = klines[..klines.len() - 1].iter().map(|k| k.close).collect();
    let prev_ema20_vals = ema(&prev_closes, 20);
    let prev_ema20 = prev_ema20_vals.last().copied().unwrap_or(0.0);
    if curr_ema20 <= 0.0 || prev_ema20 <= 0.0 {
        return Vec::new();
    }

    let crossed_up = prev.close <= prev_ema20 && curr.close > curr_ema20;
    let crossed_down = prev.close >= prev_ema20 && curr.close < curr_ema20;
    let (direction, alert_hint, expected_use, reason) = if crossed_up {
        ("long",
         "bullish_watch",
         "\u{770b}\u{591a}\u{63d0}\u{9192}\u{ff1a}\u{4ef7}\u{683c}\u{4e0a}\u{7a7f}EMA20\u{ff0c}\u{5173}\u{6ce8}\u{4e70}\u{5165}\u{673a}\u{4f1a}\u{ff0c}\u{4e0b}\u{4e00}\u{6839}K\u{7ebf}\u{786e}\u{8ba4}\u{540e}\u{53ef}\u{8003}\u{8651}\u{5efa}\u{4ed3}/\u{52a0}\u{4ed3}",
         format!("EMA20\u{4e0a}\u{7a7f}: prev_c={:.4} prev_ema={:.4} curr_c={:.4} curr_ema={:.4}", prev.close, prev_ema20, curr.close, curr_ema20))
    } else if crossed_down {
        ("short",
         "bearish_watch",
         "\u{770b}\u{7a7a}\u{63d0}\u{9192}\u{ff1a}\u{4ef7}\u{683c}\u{4e0b}\u{4f20}EMA20\u{ff0c}\u{6ce8}\u{610f}\u{98ce}\u{9669}\u{ff0c}\u{8003}\u{8651}\u{51cf}\u{4ed3}/\u{6b62}\u{76c8}\u{ff0c}A\u{80a1}\u{4e0d}\u{53ef}\u{505a}\u{7a7a}",
         format!("EMA20\u{4e0b}\u{4f20}: prev_c={:.4} prev_ema={:.4} curr_c={:.4} curr_ema={:.4}", prev.close, prev_ema20, curr.close, curr_ema20))
    } else {
        return Vec::new();
    };

    let entry = curr.close;
    let stop = if direction == "long" {
        entry - atr14 * AS_STOP_ATR_MULT
    } else {
        entry + atr14 * AS_STOP_ATR_MULT
    };
    let target = if direction == "long" {
        entry + atr14 * AS_TARGET_ATR_MULT
    } else {
        entry - atr14 * AS_TARGET_ATR_MULT
    };

    vec![Signal {
        signal_type: "pa_ema20_cross".to_string(),
        direction: direction.to_string(),
        score: 0.6,
        ema20_entry_score: 0.0,
        entry_price: Some((entry * 10000.0).round() / 10000.0),
        stop_loss: Some((stop * 10000.0).round() / 10000.0),
        target_price: Some((target * 10000.0).round() / 10000.0),
        reason,
        evidence: json!({
            "model_code": "pa_ema20_cross_cn_v1",
            "alert_purpose": "watchlist_monitor",
            "alert_semantics": "human_review_required",
            "watch_alert": {
                "alert_hint": alert_hint,
                "direction_hint": direction,
                "pattern_type": "ema20_cross",
                "expected_use": expected_use,
                "reference_levels": {
                    "signal_bar_high": (curr.high * 10000.0).round() / 10000.0,
                    "signal_bar_low": (curr.low * 10000.0).round() / 10000.0,
                    "atr14": (atr14 * 10000.0).round() / 10000.0,
                    "ema20": (curr_ema20 * 10000.0).round() / 10000.0,
                },
            },
            "prev_close": prev.close,
            "prev_ema20": prev_ema20,
            "curr_close": curr.close,
            "curr_ema20": curr_ema20,
        }),
    }]
}
