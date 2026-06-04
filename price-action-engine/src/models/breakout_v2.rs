use crate::backtest::{
    backtest_trades_v2, generate_sample_candidates_v2, median,
};
use crate::indicators::compute_indicators;
use crate::model::PriceActionModel;
use crate::types::{BreakoutCandidate, Indicators, Kline, Signal};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// V2 model -- 3-layer funnel (veto -> bar score -> follow-through), 65-pt scale
// ---------------------------------------------------------------------------

pub struct V2Model;

impl PriceActionModel for V2Model {
    fn code(&self) -> &str {
        "pa_breakout_v2"
    }

    fn name(&self) -> &str {
        "pricedog_pa_breakout"
    }

    fn version(&self) -> &str {
        "v2"
    }

    fn min_klines(&self) -> usize {
        28
    }

    fn detect(&self, klines: &[Kline]) -> Vec<Signal> {
        detect_breakout_v2(klines)
    }

    fn backtest(
        &self,
        klines: &[Kline],
        max_holding_bars: usize,
        fee_bps: f64,
        slippage_bps: f64,
    ) -> Vec<crate::types::BacktestTrade> {
        backtest_trades_v2(klines, max_holding_bars, fee_bps, slippage_bps)
    }

    fn generate_candidates(
        &self,
        klines: &[Kline],
        max_candidates: usize,
    ) -> Vec<crate::types::SampleCandidate> {
        generate_sample_candidates_v2(klines, max_candidates)
    }
}

// ---------------------------------------------------------------------------
// V2 model functions
// ---------------------------------------------------------------------------

/// Layer 1+2: Veto + breakout bar scoring. Returns None if vetoed.
pub fn evaluate_breakout_bar(klines: &[Kline], bar_idx: usize) -> Option<BreakoutCandidate> {
    if bar_idx < 20 {
        return None;
    }
    let prev_window = &klines[bar_idx - 20..bar_idx];
    let range_high = prev_window.iter().map(|k| k.high).fold(f64::MIN, f64::max);
    let range_low = prev_window.iter().map(|k| k.low).fold(f64::MAX, f64::min);
    let curr = &klines[bar_idx];

    let (direction, breakout_distance) = if curr.close > range_high {
        ("long".to_string(), curr.close - range_high)
    } else if curr.close < range_low {
        ("short".to_string(), range_low - curr.close)
    } else {
        return None;
    };

    let window = &klines[..=bar_idx];
    let indicators = compute_indicators(window);
    let atr14 = indicators.atr14.unwrap_or(0.0);
    let distance_atr = if atr14 > 0.0 {
        breakout_distance / atr14
    } else {
        0.0
    };

    // Entry / stop / target
    let entry = curr.close;
    let stop = if direction == "long" {
        curr.low.min(range_high)
    } else {
        curr.high.max(range_low)
    };
    let target = if direction == "long" {
        entry + (entry - stop).abs() * 2.0
    } else {
        entry - (entry - stop).abs() * 2.0
    };

    // Layer 1 -- Veto
    // V1: actual_rr < 1.0
    let risk = (entry - stop).abs();
    let reward = (target - entry).abs();
    if risk > 0.0 && reward / risk < 1.0 {
        return None;
    }
    // Note: wick-back veto removed -- crypto 1h bars naturally re-enter range.
    // close_location in Layer 2 already penalizes long-wick bars.

    // Layer 2 -- Breakout bar score (0-25)
    let body = (curr.close - curr.open).abs();
    let median_body = median(
        prev_window
            .iter()
            .map(|k| (k.close - k.open).abs())
            .collect(),
    );
    let body_ratio = if median_body > 0.0 {
        body / median_body
    } else {
        0.0
    };
    let range = curr.high - curr.low;
    let close_location = if range > 0.0 {
        (curr.close - curr.low) / range
    } else {
        0.5
    };

    let mut score = 0.0;
    if body_ratio >= 2.0 {
        score += 5.0;
    }
    if body_ratio >= 5.0 {
        score += 5.0;
    }
    if close_location >= 0.75 {
        score += 5.0;
    }
    if close_location >= 0.9 {
        score += 3.0;
    }
    if distance_atr >= 0.3 {
        score += 4.0;
    }
    if distance_atr >= 0.8 {
        score += 3.0;
    }

    Some(BreakoutCandidate {
        direction,
        range_high,
        range_low,
        body_ratio,
        close_location,
        distance_atr,
        breakout_bar_score: score,
        atr14,
        stop_loss: stop,
        target_price: target,
    })
}

/// Layer 3: Follow-through (0-25) + Structure (0-15). Returns None if delayed-vetoed.
pub fn score_follow_through(
    klines: &[Kline],
    breakout_idx: usize,
    cand: &BreakoutCandidate,
) -> Option<(f64, f64, Value)> {
    if breakout_idx + 3 >= klines.len() {
        return None;
    }
    let bar1 = &klines[breakout_idx + 1];
    let bar2 = &klines[breakout_idx + 2];
    let bar3 = &klines[breakout_idx + 3];
    let breakout_bar = &klines[breakout_idx];
    let is_long = cand.direction == "long";

    // --- Delayed veto ---
    // 2 of 3 bars close back inside range
    let inside_count = [bar1, bar2, bar3]
        .iter()
        .filter(|b| {
            if is_long {
                b.close < cand.range_high
            } else {
                b.close > cand.range_low
            }
        })
        .count();
    if inside_count >= 2 {
        return None;
    }

    // 2 consecutive adverse bars with body_ratio >= 1.5
    let median_body = if cand.body_ratio > 0.0 {
        (breakout_bar.close - breakout_bar.open).abs() / cand.body_ratio
    } else {
        0.0
    };
    let adverse_cl = if is_long { 0.3 } else { 0.7 };
    for pair in [[bar1, bar2], [bar2, bar3]] {
        let r0 = pair[0].high - pair[0].low;
        let cl0 = if r0 > 0.0 {
            (pair[0].close - pair[0].low) / r0
        } else {
            0.5
        };
        let r1 = pair[1].high - pair[1].low;
        let cl1 = if r1 > 0.0 {
            (pair[1].close - pair[1].low) / r1
        } else {
            0.5
        };
        let is_adverse = if is_long {
            cl0 <= adverse_cl && cl1 <= adverse_cl
        } else {
            cl0 >= adverse_cl && cl1 >= adverse_cl
        };
        let br0 = if median_body > 0.0 {
            (pair[0].close - pair[0].open).abs() / median_body
        } else {
            0.0
        };
        let br1 = if median_body > 0.0 {
            (pair[1].close - pair[1].open).abs() / median_body
        } else {
            0.0
        };
        if is_adverse && br0 >= 1.5 && br1 >= 1.5 {
            return None;
        }
    }

    // --- Follow-through score (0-25) ---
    let outside_closes = [bar1, bar2, bar3]
        .iter()
        .filter(|b| {
            if is_long {
                b.close > cand.range_high
            } else {
                b.close < cand.range_low
            }
        })
        .count();

    // follow_score: 9-point checklist
    let mut follow_score: usize = 0;
    let mut running_high = breakout_bar.high;
    let mut running_low = breakout_bar.low;
    for bar in [bar1, bar2, bar3] {
        let same_dir = if is_long {
            bar.close > bar.open
        } else {
            bar.close < bar.open
        };
        if same_dir {
            follow_score += 1;
        }
        let bar_range = bar.high - bar.low;
        let cl = if bar_range > 0.0 {
            (bar.close - bar.low) / bar_range
        } else {
            0.5
        };
        if (is_long && cl >= 0.6) || (!is_long && cl <= 0.4) {
            follow_score += 1;
        }
        if is_long && bar.high > running_high {
            follow_score += 1;
            running_high = bar.high;
        }
        if !is_long && bar.low < running_low {
            follow_score += 1;
            running_low = bar.low;
        }
    }

    // Strong counter reclaim
    let has_strong_counter = [bar1, bar2, bar3].iter().any(|b| {
        let inside = if is_long {
            b.close < cand.range_high
        } else {
            b.close > cand.range_low
        };
        let br = if median_body > 0.0 {
            (b.close - b.open).abs() / median_body
        } else {
            0.0
        };
        inside && br >= 1.5
    });

    let mut follow_pts: f64 = 0.0;
    if outside_closes >= 2 {
        follow_pts += 8.0;
    }
    if follow_score >= 5 {
        follow_pts += 8.0;
    }
    if follow_score >= 7 {
        follow_pts += 4.0;
    }
    if !has_strong_counter {
        follow_pts += 5.0;
    }
    if outside_closes == 0 {
        follow_pts = follow_pts.min(5.0);
    }

    // --- Structure score (0-15) ---
    let breakout_magnitude = if is_long {
        breakout_bar.close - cand.range_high
    } else {
        cand.range_low - breakout_bar.close
    };
    let low_3 = bar1.low.min(bar2.low).min(bar3.low);
    let high_3 = bar1.high.max(bar2.high).max(bar3.high);
    let pullback_depth = if breakout_magnitude > 0.0 {
        if is_long {
            (breakout_bar.close - low_3) / breakout_magnitude
        } else {
            (high_3 - breakout_bar.close) / breakout_magnitude
        }
    } else {
        1.0
    };

    // Crypto gap_hold: follow bars don't retrace breakout bar's body open
    let gap_hold = if is_long {
        low_3 > breakout_bar.open
    } else {
        high_3 < breakout_bar.open
    };

    let mut struct_pts: f64 = 0.0;
    if pullback_depth <= 0.25 {
        struct_pts += 5.0;
    } else if pullback_depth <= 0.5 {
        struct_pts += 3.0;
    }
    if gap_hold {
        struct_pts += 5.0;
    }
    if outside_closes == 3 {
        struct_pts += 5.0;
    }
    if pullback_depth > 0.5 {
        struct_pts = struct_pts.min(6.0);
    }

    let evidence = json!({
        "outside_closes": outside_closes,
        "follow_score": follow_score,
        "has_strong_counter": has_strong_counter,
        "pullback_depth": (pullback_depth * 100.0).round() / 100.0,
        "gap_hold": gap_hold,
        "breakout_bar_ts": breakout_bar.ts,
    });

    Some((follow_pts, struct_pts, evidence))
}

/// Simplified EMA20 entry score -- independent module, not part of the 65-pt total.
pub fn compute_ema20_entry_v2(curr: &Kline, indicators: &Indicators, direction: &str) -> f64 {
    let Some(ema20) = indicators.ema20 else {
        return 0.0;
    };
    let atr14 = indicators.atr14.unwrap_or(0.0);
    if atr14 <= 0.0 {
        return 0.0;
    }
    let ema_gap = (curr.close - ema20).abs() / atr14;
    let mut score: f64 = 0.0;
    if (direction == "long" && curr.close > ema20) || (direction == "short" && curr.close < ema20) {
        score += 3.0;
    }
    if ema_gap <= 1.5 {
        score += 3.0;
    }
    if ema_gap <= 0.5 {
        score += 3.0;
    }
    if ema_gap >= 2.5 {
        score -= 5.0;
    }
    score.max(0.0)
}

/// Main v2 detection: 3-layer funnel (veto -> bar score -> follow-through).
/// Looks at bar (len-4) as breakout candidate, bars (len-3..len-1) as follow-through.
pub fn detect_breakout_v2(klines: &[Kline]) -> Vec<Signal> {
    if klines.len() < 28 {
        return Vec::new();
    }
    let breakout_idx = klines.len() - 4;

    let Some(cand) = evaluate_breakout_bar(klines, breakout_idx) else {
        return Vec::new();
    };
    let Some((follow_pts, struct_pts, follow_evidence)) =
        score_follow_through(klines, breakout_idx, &cand)
    else {
        return Vec::new();
    };

    let total = cand.breakout_bar_score + follow_pts + struct_pts;
    if total < 35.0 {
        return Vec::new();
    }

    let window = &klines[..=breakout_idx];
    let indicators = compute_indicators(window);
    let ema_score = compute_ema20_entry_v2(&klines[breakout_idx], &indicators, &cand.direction);
    let entry_price = klines[breakout_idx].close;

    vec![Signal {
        signal_type: "pa_breakout".to_string(),
        direction: cand.direction.clone(),
        score: total,
        ema20_entry_score: ema_score,
        entry_price: Some(entry_price),
        stop_loss: Some(cand.stop_loss),
        target_price: Some(cand.target_price),
        reason: format!(
            "{} breakout: bar={:.0}+follow={:.0}+struct={:.0}={:.0} ema20_entry={:.0}",
            cand.direction, cand.breakout_bar_score, follow_pts, struct_pts, total, ema_score
        ),
        evidence: json!({
            "range_high": cand.range_high,
            "range_low": cand.range_low,
            "body_ratio": (cand.body_ratio * 100.0).round() / 100.0,
            "close_location": (cand.close_location * 100.0).round() / 100.0,
            "distance_atr": (cand.distance_atr * 100.0).round() / 100.0,
            "breakout_bar_score": cand.breakout_bar_score,
            "follow_through": follow_evidence,
            "ema20_entry_score": ema_score,
        }),
    }]
}

pub fn ema20_touch_reclaim(curr: &Kline, indicators: &Indicators, direction: &str) -> bool {
    let Some(ema20) = indicators.ema20 else {
        return false;
    };
    if direction == "long" {
        curr.low <= ema20 && curr.close >= ema20
    } else {
        curr.high >= ema20 && curr.close <= ema20
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Kline;

    #[test]
    fn detects_strong_long_breakout() {
        // 20 range bars + 1 breakout bar + 3 follow-through + 4 warmup = 28
        let mut bars: Vec<Kline> = (0..24)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        // Breakout bar (index 24): strong bar closing above range_high=102
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.0,
            high: 110.0,
            low: 100.5,
            close: 109.0,
            volume: 3500.0,
            turnover: 0.0,
        });
        // 3 follow-through bars: continue up, stay above range_high
        for i in 0..3 {
            bars.push(Kline {
                ts: format!("2024-01-{:02}", 26 + i),
                open: 109.0 + i as f64,
                high: 112.0 + i as f64,
                low: 108.5 + i as f64,
                close: 111.0 + i as f64,
                volume: 1500.0,
                turnover: 0.0,
            });
        }

        let signals = detect_breakout_v2(&bars);

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "pa_breakout");
        assert_eq!(signals[0].direction, "long");
        assert!(signals[0].score >= 35.0);
        // Entry price is the breakout bar's close
        assert_eq!(signals[0].entry_price, Some(109.0));
        assert!(signals[0].stop_loss.unwrap() <= 102.5);
        assert!(signals[0].target_price.unwrap() > signals[0].entry_price.unwrap());
    }

    #[test]
    fn ignores_range_bound_price_action() {
        let bars: Vec<Kline> = (0..28)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0 + i as f64,
                turnover: 0.0,
            })
            .collect();

        let signals = detect_breakout_v2(&bars);

        assert!(signals.is_empty());
    }

    #[test]
    fn ignores_weak_breakout_without_confirmation() {
        let mut bars: Vec<Kline> = (0..24)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        bars.push(Kline {
            ts: "2024-01-25".to_string(),
            open: 101.9,
            high: 102.4,
            low: 100.8,
            close: 102.1,
            volume: 900.0,
            turnover: 0.0,
        });
        // Follow-through bars that go back inside range (delayed veto)
        for i in 0..3 {
            bars.push(Kline {
                ts: format!("2024-01-{:02}", 26 + i),
                open: 101.5,
                high: 101.8,
                low: 100.0,
                close: 101.0,
                volume: 800.0,
                turnover: 0.0,
            });
        }

        let signals = detect_breakout_v2(&bars);

        assert!(signals.is_empty());
    }

    #[test]
    fn ema20_entry_v2_prefers_near_ema20() {
        let bars: Vec<Kline> = (0..30)
            .map(|i| Kline {
                ts: format!("2024-01-{:02}", i + 1),
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1000.0,
                turnover: 0.0,
            })
            .collect();
        let indicators = compute_indicators(&bars);

        let near_bar = Kline {
            ts: "2024-02-01".to_string(),
            open: indicators.ema20.unwrap() - 0.5,
            high: indicators.ema20.unwrap() + 2.0,
            low: indicators.ema20.unwrap() - 1.0,
            close: indicators.ema20.unwrap() + 1.0,
            volume: 1000.0,
            turnover: 0.0,
        };
        let far_bar = Kline {
            ts: "2024-02-02".to_string(),
            open: indicators.ema20.unwrap() + 20.0,
            high: indicators.ema20.unwrap() + 25.0,
            low: indicators.ema20.unwrap() + 18.0,
            close: indicators.ema20.unwrap() + 22.0,
            volume: 1000.0,
            turnover: 0.0,
        };

        let near_score = compute_ema20_entry_v2(&near_bar, &indicators, "long");
        let far_score = compute_ema20_entry_v2(&far_bar, &indicators, "long");

        assert!(near_score > far_score);
    }

    #[test]
    fn overextended_position_reduces_ema20_entry_v2() {
        let moderate = crate::types::Indicators {
            ema20: Some(100.0),
            ema5: None,
            atr14: Some(10.0),
            atr20: Some(10.0),
            ema20_position: Some(1.0),
            volume_ratio: None,
            amplitude: None,
        };
        let stretched = crate::types::Indicators {
            ema20: Some(100.0),
            ema5: None,
            atr14: Some(10.0),
            atr20: Some(10.0),
            ema20_position: Some(3.0),
            volume_ratio: None,
            amplitude: None,
        };
        // close=130, ema20=100, atr14=10 -> ema_gap=3.0 -> triggers -5 penalty
        let stretched_bar = Kline {
            ts: "2024-02-01".to_string(),
            open: 128.0,
            high: 132.0,
            low: 127.0,
            close: 130.0,
            volume: 1000.0,
            turnover: 0.0,
        };
        // close=105, ema20=100, atr14=10 -> ema_gap=0.5 -> near EMA20
        let near_bar = Kline {
            ts: "2024-02-02".to_string(),
            open: 103.0,
            high: 106.0,
            low: 102.5,
            close: 105.0,
            volume: 1000.0,
            turnover: 0.0,
        };

        assert!(compute_ema20_entry_v2(&near_bar, &moderate, "long") > 0.0);
        assert!(
            compute_ema20_entry_v2(&near_bar, &moderate, "long")
                > compute_ema20_entry_v2(&stretched_bar, &stretched, "long")
        );
    }
}
