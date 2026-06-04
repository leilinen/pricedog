use crate::backtest::{
    backtest_trades_from_model, generate_sample_candidates_from_model, SIGNAL_BAR_TICK_SIZE,
};
use crate::indicators::{compute_indicators, ema};
use crate::model::PriceActionModel;
use crate::types::{Kline, Signal};
use serde_json::json;

// ---------------------------------------------------------------------------
// Ema20CrossModel struct + trait impl
// ---------------------------------------------------------------------------

pub struct Ema20CrossModel;

impl PriceActionModel for Ema20CrossModel {
    fn code(&self) -> &str {
        "pa_ema20_cross_v1"
    }
    fn name(&self) -> &str {
        "pricedog_pa_ema20_cross"
    }
    fn version(&self) -> &str {
        "v1"
    }
    fn min_klines(&self) -> usize {
        22
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        if klines.len() < 22 {
            return Vec::new();
        }
        let curr = klines.last().unwrap();
        let prev = &klines[klines.len() - 2];
        let indicators = compute_indicators(klines);
        let curr_ema20 = indicators.ema20.unwrap_or(0.0);
        let prev_closes: Vec<f64> = klines[..klines.len() - 1].iter().map(|k| k.close).collect();
        let prev_ema20_vals = ema(&prev_closes, 20);
        let prev_ema20 = prev_ema20_vals.last().copied().unwrap_or(0.0);
        if curr_ema20 <= 0.0 || prev_ema20 <= 0.0 {
            return Vec::new();
        }

        let crossed_up = prev.close <= prev_ema20 && curr.close > curr_ema20;
        let crossed_down = prev.close >= prev_ema20 && curr.close < curr_ema20;
        let (direction, reason) = if crossed_up {
            (
                "long",
                format!(
                    "EMA20 cross up: prev_c={:.2} prev_ema20={:.2} curr_c={:.2} curr_ema20={:.2}",
                    prev.close, prev_ema20, curr.close, curr_ema20
                ),
            )
        } else if crossed_down {
            (
                "short",
                format!(
                    "EMA20 cross down: prev_c={:.2} prev_ema20={:.2} curr_c={:.2} curr_ema20={:.2}",
                    prev.close, prev_ema20, curr.close, curr_ema20
                ),
            )
        } else {
            return Vec::new();
        };

        let delta = SIGNAL_BAR_TICK_SIZE;
        let (entry, stop, target) = if direction == "long" {
            let e = curr.high + delta;
            let s = curr.low - delta;
            (e, s, e + 2.0 * (e - s))
        } else {
            let e = curr.low - delta;
            let s = curr.high + delta;
            (e, s, e - 2.0 * (s - e))
        };

        vec![Signal {
            signal_type: "pa_ema20_cross".to_string(),
            direction: direction.to_string(),
            score: 0.6,
            ema20_entry_score: 0.0,
            entry_price: Some((entry * 100.0).round() / 100.0),
            stop_loss: Some((stop * 100.0).round() / 100.0),
            target_price: Some((target * 100.0).round() / 100.0),
            reason,
            evidence: json!({
                "model_code": "pa_ema20_cross_v1",
                "type": "ema20_cross",
                "direction": direction,
                "prev_close": prev.close,
                "prev_ema20": prev_ema20,
                "curr_close": curr.close,
                "curr_ema20": curr_ema20,
            }),
        }]
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
