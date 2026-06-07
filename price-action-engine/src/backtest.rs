use crate::indicators::compute_indicators;
use crate::model::PriceActionModel;
use crate::models::breakout_v2::{
    compute_ema20_entry_v2, ema20_touch_reclaim, evaluate_breakout_bar, score_follow_through,
};
use crate::types::{BacktestSummary, BacktestTrade, Kline, SampleCandidate, Signal};
use serde_json::json;

// ---------------------------------------------------------------------------
// Constants shared across models
// ---------------------------------------------------------------------------

pub const SIGNAL_BAR_Q_MIN: f64 = 0.6;
pub const SIGNAL_BAR_COOLDOWN_BARS: usize = 4;
pub const SIGNAL_BAR_TICK_SIZE: f64 = 0.01;
pub const AS_COOLDOWN_BARS: usize = 3;
pub const AS_STOP_ATR_MULT: f64 = 1.2;
pub const AS_TARGET_ATR_MULT: f64 = 1.5;

// ---------------------------------------------------------------------------
// Shared backtest helpers
// ---------------------------------------------------------------------------

pub fn generate_sample_candidates_from_model(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_candidates: usize,
) -> Vec<SampleCandidate> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        let indicators = compute_indicators(window);
        for signal in model.detect(window) {
            if crate::model::signal_bar_cooldown_blocks(
                model.code(),
                current_idx,
                &signal.direction,
                last_long_idx,
                last_short_idx,
            ) {
                continue;
            }
            let curr = &klines[current_idx];
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: signal.signal_type.clone(),
                direction: signal.direction.clone(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(signal.score),
                evidence: signal.evidence.clone(),
            });
            crate::model::remember_signal_idx(
                &signal.direction,
                current_idx,
                &mut last_long_idx,
                &mut last_short_idx,
            );
        }
    }
    if out.len() > max_candidates {
        out = out[out.len() - max_candidates..].to_vec();
    }
    out
}

pub fn generate_sample_candidates_v2(
    klines: &[Kline],
    max_candidates: usize,
) -> Vec<SampleCandidate> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for current_idx in 27..klines.len() {
        let breakout_idx = current_idx - 3;
        let curr = &klines[current_idx];

        // Breakout candidates via v2 model
        if let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) {
            if let Some((follow_pts, struct_pts, follow_evidence)) =
                score_follow_through(klines, breakout_idx, &cand)
            {
                let total = cand.breakout_bar_score + follow_pts + struct_pts;
                let window = &klines[..=breakout_idx];
                let indicators = compute_indicators(window);
                if total >= 35.0 {
                    out.push(SampleCandidate {
                        ts: curr.ts.clone(),
                        candidate_type: "pa_breakout".to_string(),
                        direction: cand.direction.clone(),
                        label: None,
                        current_price: curr.close,
                        ema20: indicators.ema20,
                        atr14: indicators.atr14,
                        ema20_position: indicators.ema20_position,
                        volume_ratio: indicators.volume_ratio,
                        score: Some(total),
                        evidence: json!({
                            "breakout_bar_score": cand.breakout_bar_score,
                            "follow_pts": follow_pts,
                            "struct_pts": struct_pts,
                            "total": total,
                            "follow_through": follow_evidence,
                        }),
                    });
                }
            }
        }

        // EMA20 reclaim candidates (keep existing logic)
        let window = &klines[..=current_idx];
        let indicators = compute_indicators(window);
        if ema20_touch_reclaim(curr, &indicators, "long")
            && indicators.ema20_position.unwrap_or(0.0) >= -0.5
        {
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: "ema20_reclaim".to_string(),
                direction: "long".to_string(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(compute_ema20_entry_v2(curr, &indicators, "long")),
                evidence: json!({
                    "ema20_touched": true,
                    "close_above_ema20": indicators.ema20.is_some_and(|ema20| curr.close >= ema20),
                    "low_vs_ema20": indicators.ema20.map(|ema20| curr.low - ema20),
                    "ema20_position": indicators.ema20_position,
                }),
            });
        }

        if ema20_touch_reclaim(curr, &indicators, "short")
            && indicators.ema20_position.unwrap_or(0.0) <= 0.5
        {
            out.push(SampleCandidate {
                ts: curr.ts.clone(),
                candidate_type: "ema20_reclaim".to_string(),
                direction: "short".to_string(),
                label: None,
                current_price: curr.close,
                ema20: indicators.ema20,
                atr14: indicators.atr14,
                ema20_position: indicators.ema20_position,
                volume_ratio: indicators.volume_ratio,
                score: Some(compute_ema20_entry_v2(curr, &indicators, "short")),
                evidence: json!({
                    "ema20_touched": true,
                    "close_below_ema20": indicators.ema20.is_some_and(|ema20| curr.close <= ema20),
                    "high_vs_ema20": indicators.ema20.map(|ema20| curr.high - ema20),
                    "ema20_position": indicators.ema20_position,
                }),
            });
        }
    }
    if out.len() > max_candidates {
        out = out[out.len() - max_candidates..].to_vec();
    }
    out
}

pub fn backtest_trades_from_model(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut trades = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        for signal in model.detect(window) {
            if crate::model::signal_bar_cooldown_blocks(
                model.code(),
                current_idx,
                &signal.direction,
                last_long_idx,
                last_short_idx,
            ) {
                continue;
            }
            if let Some(trade) = simulate_backtest_trade(
                klines,
                current_idx,
                &signal,
                max_holding_bars,
                fee_bps,
                slippage_bps,
            ) {
                let curr = &klines[current_idx];
                trades.push(BacktestTrade {
                    ts: curr.ts.clone(),
                    direction: signal.direction.clone(),
                    signal_type: signal.signal_type.clone(),
                    score: signal.score,
                    entry_price: trade.0,
                    stop_loss: signal.stop_loss,
                    target_price: signal.target_price,
                    exit_ts: trade.1.ts.clone(),
                    exit_price: trade.2,
                    exit_reason: trade.3.clone(),
                    holding_bars: trade.4,
                    return_pct: trade.5,
                    hit_target: trade.3 == "hit_target",
                    hit_stop: trade.3 == "hit_stop",
                    signal: signal.signal_type.clone(),
                    reason: signal.reason.clone(),
                    evidence: signal.evidence.clone(),
                });
                crate::model::remember_signal_idx(
                    &signal.direction,
                    current_idx,
                    &mut last_long_idx,
                    &mut last_short_idx,
                );
            }
        }
    }
    trades
}

/// A-share backtest: only simulate long trades, short signals are watchlist alerts only (occupy cooldown but no trade)
pub fn backtest_trades_from_model_long_only(
    model: &dyn PriceActionModel,
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < model.min_klines() {
        return Vec::new();
    }
    let mut trades = Vec::new();
    let mut last_long_idx: Option<usize> = None;
    let mut last_short_idx: Option<usize> = None;
    for current_idx in model.min_klines() - 1..klines.len() {
        let window = &klines[..=current_idx];
        for signal in model.detect(window) {
            if crate::model::signal_bar_cooldown_blocks(
                model.code(),
                current_idx,
                &signal.direction,
                last_long_idx,
                last_short_idx,
            ) {
                continue;
            }
            crate::model::remember_signal_idx(
                &signal.direction,
                current_idx,
                &mut last_long_idx,
                &mut last_short_idx,
            );
            // A-share only simulates long, short signals are watchlist alerts only
            if signal.direction != "long" {
                continue;
            }
            if let Some(trade) = simulate_backtest_trade(
                klines,
                current_idx,
                &signal,
                max_holding_bars,
                fee_bps,
                slippage_bps,
            ) {
                let curr = &klines[current_idx];
                trades.push(BacktestTrade {
                    ts: curr.ts.clone(),
                    direction: signal.direction.clone(),
                    signal_type: signal.signal_type.clone(),
                    score: signal.score,
                    entry_price: trade.0,
                    stop_loss: signal.stop_loss,
                    target_price: signal.target_price,
                    exit_ts: trade.1.ts.clone(),
                    exit_price: trade.2,
                    exit_reason: trade.3.clone(),
                    holding_bars: trade.4,
                    return_pct: trade.5,
                    hit_target: trade.3 == "hit_target",
                    hit_stop: trade.3 == "hit_stop",
                    signal: signal.signal_type.clone(),
                    reason: signal.reason.clone(),
                    evidence: signal.evidence.clone(),
                });
            }
        }
    }
    trades
}

pub fn backtest_trades_v2(
    klines: &[Kline],
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Vec<BacktestTrade> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let mut trades = Vec::new();
    // current_idx is where follow-through ends; breakout bar is at current_idx - 3
    for current_idx in 27..klines.len() {
        let breakout_idx = current_idx - 3;

        let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) else {
            continue;
        };
        let Some((follow_pts, struct_pts, follow_evidence)) =
            score_follow_through(klines, breakout_idx, &cand)
        else {
            continue;
        };

        let total = cand.breakout_bar_score + follow_pts + struct_pts;
        if total < 35.0 {
            continue;
        }

        let window = &klines[..=breakout_idx];
        let indicators = compute_indicators(window);
        let ema_score = compute_ema20_entry_v2(&klines[breakout_idx], &indicators, &cand.direction);

        let entry_price = klines[breakout_idx].close;
        let signal = Signal {
            signal_type: "pa_breakout".to_string(),
            direction: cand.direction.clone(),
            score: total,
            ema20_entry_score: ema_score,
            entry_price: Some(entry_price),
            stop_loss: Some(cand.stop_loss),
            target_price: Some(cand.target_price),
            reason: format!(
                "{} breakout: bar={:.0}+follow={:.0}+struct={:.0}={:.0}",
                cand.direction, cand.breakout_bar_score, follow_pts, struct_pts, total
            ),
            evidence: json!({
                "range_high": cand.range_high,
                "range_low": cand.range_low,
                "body_ratio": (cand.body_ratio * 100.0).round() / 100.0,
                "close_location": (cand.close_location * 100.0).round() / 100.0,
                "distance_atr": (cand.distance_atr * 100.0).round() / 100.0,
                "breakout_bar_score": cand.breakout_bar_score,
                "follow_through": follow_evidence,
            }),
        };

        if let Some(trade) = simulate_backtest_trade(
            klines,
            current_idx,
            &signal,
            max_holding_bars,
            fee_bps,
            slippage_bps,
        ) {
            let curr = &klines[current_idx];
            trades.push(BacktestTrade {
                ts: curr.ts.clone(),
                direction: signal.direction.clone(),
                signal_type: signal.signal_type.clone(),
                score: signal.score,
                entry_price: trade.0,
                stop_loss: signal.stop_loss,
                target_price: signal.target_price,
                exit_ts: trade.1.ts.clone(),
                exit_price: trade.2,
                exit_reason: trade.3.clone(),
                holding_bars: trade.4,
                return_pct: trade.5,
                hit_target: trade.3 == "hit_target",
                hit_stop: trade.3 == "hit_stop",
                signal: signal.signal_type.clone(),
                reason: signal.reason.clone(),
                evidence: signal.evidence.clone(),
            });
        }
    }
    trades
}

pub fn simulate_backtest_trade<'a>(
    klines: &'a [Kline],
    signal_index: usize,
    signal: &Signal,
    max_holding_bars: usize,
    fee_bps: f64,
    slippage_bps: f64,
) -> Option<(f64, &'a Kline, f64, String, usize, f64)> {
    let raw_entry = signal.entry_price?;
    let stop = signal.stop_loss?;
    let target = signal.target_price?;
    let cost_bps = (fee_bps + slippage_bps).max(0.0) / 10_000.0;
    let direction = signal.direction.as_str();
    let entry_price = if direction == "long" {
        raw_entry * (1.0 + cost_bps)
    } else {
        raw_entry * (1.0 - cost_bps)
    };

    let start = signal_index + 1;
    if start >= klines.len() {
        return None;
    }
    let end_exclusive = (start + max_holding_bars).min(klines.len());
    for (offset, bar) in klines[start..end_exclusive].iter().enumerate() {
        if direction == "long" {
            if bar.low <= stop {
                let exit_price = stop * (1.0 - cost_bps);
                let ret = (exit_price - entry_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_stop".to_string(),
                    offset + 1,
                    ret,
                ));
            }
            if bar.high >= target {
                let exit_price = target * (1.0 - cost_bps);
                let ret = (exit_price - entry_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_target".to_string(),
                    offset + 1,
                    ret,
                ));
            }
        } else {
            if bar.high >= stop {
                let exit_price = stop * (1.0 + cost_bps);
                let ret = (entry_price - exit_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_stop".to_string(),
                    offset + 1,
                    ret,
                ));
            }
            if bar.low <= target {
                let exit_price = target * (1.0 + cost_bps);
                let ret = (entry_price - exit_price) / entry_price * 100.0;
                return Some((
                    entry_price,
                    bar,
                    exit_price,
                    "hit_target".to_string(),
                    offset + 1,
                    ret,
                ));
            }
        }
    }

    let last_bar = &klines[end_exclusive - 1];
    let exit_price = if direction == "long" {
        last_bar.close * (1.0 - cost_bps)
    } else {
        last_bar.close * (1.0 + cost_bps)
    };
    let ret = if direction == "long" {
        (exit_price - entry_price) / entry_price * 100.0
    } else {
        (entry_price - exit_price) / entry_price * 100.0
    };
    Some((
        entry_price,
        last_bar,
        exit_price,
        "timeout".to_string(),
        end_exclusive - start,
        ret,
    ))
}

pub fn summarize_backtest(trades: &[BacktestTrade]) -> BacktestSummary {
    if trades.is_empty() {
        return BacktestSummary {
            signal_count: 0,
            evaluated_count: 0,
            win_rate: 0.0,
            avg_return_pct: 0.0,
            median_return_pct: 0.0,
            hit_target_rate: 0.0,
            hit_stop_rate: 0.0,
            avg_holding_bars: 0.0,
        };
    }
    let returns = trades.iter().map(|t| t.return_pct).collect::<Vec<_>>();
    let wins = trades.iter().filter(|t| t.return_pct > 0.0).count();
    let hits_target = trades.iter().filter(|t| t.hit_target).count();
    let hits_stop = trades.iter().filter(|t| t.hit_stop).count();
    let avg_hold = trades.iter().map(|t| t.holding_bars as f64).sum::<f64>() / trades.len() as f64;
    BacktestSummary {
        signal_count: trades.len(),
        evaluated_count: trades.len(),
        win_rate: wins as f64 / trades.len() as f64,
        avg_return_pct: returns.iter().sum::<f64>() / trades.len() as f64,
        median_return_pct: median(returns),
        hit_target_rate: hits_target as f64 / trades.len() as f64,
        hit_stop_rate: hits_stop as f64 / trades.len() as f64,
        avg_holding_bars: avg_hold,
    }
}

pub fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}
