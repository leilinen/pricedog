use crate::backtest::{AS_COOLDOWN_BARS, SIGNAL_BAR_COOLDOWN_BARS};
use crate::models::{
    Ema20CrossModel, Ema20CrossModelCn, SignalBarModel, SignalBarModelCn, V2Model,
};
use crate::types::{BacktestTrade, Kline, SampleCandidate, Signal};
use anyhow::{anyhow, Result};

// ---------------------------------------------------------------------------
// PriceActionModel trait -- pluggable model interface
// ---------------------------------------------------------------------------

pub trait PriceActionModel: Send + Sync {
    fn code(&self) -> &str;
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn min_klines(&self) -> usize;
    fn detect(&self, klines: &[Kline]) -> Vec<Signal>;
    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<BacktestTrade>;
    fn generate_candidates(&self, klines: &[Kline], max_candidates: usize) -> Vec<SampleCandidate>;
}

// ---------------------------------------------------------------------------
// Model registry
// ---------------------------------------------------------------------------

pub fn resolve_model(model_code: &str) -> Result<Box<dyn PriceActionModel>> {
    match model_code.trim() {
        "" | "pa_breakout_v2" | "pricedog_pa_breakout" => Ok(Box::new(V2Model)),
        "pa_signal_bar_v1" | "pricedog_pa_signal_bar" => Ok(Box::new(SignalBarModel)),
        "pa_signal_bar_cn_v1" | "pricedog_pa_signal_bar_cn" => Ok(Box::new(SignalBarModelCn)),
        "pa_ema20_cross_v1" | "pricedog_pa_ema20_cross" => Ok(Box::new(Ema20CrossModel)),
        "pa_ema20_cross_cn_v1" | "pricedog_pa_ema20_cross_cn" => Ok(Box::new(Ema20CrossModelCn)),
        other => Err(anyhow!("unknown model_code: {}", other)),
    }
}

// ---------------------------------------------------------------------------
// Cooldown helpers
// ---------------------------------------------------------------------------

pub fn signal_bar_cooldown_blocks(
    model_code: &str,
    current_idx: usize,
    direction: &str,
    last_long_idx: Option<usize>,
    last_short_idx: Option<usize>,
) -> bool {
    let cooldown = if model_code == "pa_signal_bar_cn_v1" || model_code == "pa_ema20_cross_cn_v1" {
        AS_COOLDOWN_BARS
    } else if model_code == "pa_signal_bar_v1" {
        SIGNAL_BAR_COOLDOWN_BARS
    } else {
        return false;
    };
    if cooldown == 0 {
        return false;
    }
    let last_idx = if direction == "long" {
        last_long_idx
    } else {
        last_short_idx
    };
    last_idx.is_some_and(|idx| current_idx.saturating_sub(idx) <= cooldown)
}

pub fn remember_signal_idx(
    direction: &str,
    current_idx: usize,
    last_long_idx: &mut Option<usize>,
    last_short_idx: &mut Option<usize>,
) {
    if direction == "long" {
        *last_long_idx = Some(current_idx);
    } else {
        *last_short_idx = Some(current_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_model_selects_requested_model() {
        let breakout = resolve_model("pa_breakout_v2").unwrap();
        assert_eq!(breakout.code(), "pa_breakout_v2");
        assert_eq!(breakout.name(), "pricedog_pa_breakout");

        let signal_bar = resolve_model("pa_signal_bar_v1").unwrap();
        assert_eq!(signal_bar.code(), "pa_signal_bar_v1");
        assert_eq!(signal_bar.name(), "pricedog_pa_signal_bar");

        assert!(resolve_model("missing_model").is_err());

        let ema20 = resolve_model("pa_ema20_cross_v1").unwrap();
        assert_eq!(ema20.code(), "pa_ema20_cross_v1");
        assert_eq!(ema20.min_klines(), 22);
    }
}
