#!/usr/bin/env python3
"""Regression test for price action backtest models.

Runs a fixed set of scenarios against the Rust engine's backtest API,
compares results against a stored baseline, and reports drift.

Usage:
    python scripts/regression_test.py                     # Run all scenarios, compare
    python scripts/regression_test.py --scenario breakout # Filter by model name
    python scripts/regression_test.py --update            # Save current results as new baseline
    python scripts/regression_test.py --engine-url http://127.0.0.1:8001

Requires the Rust engine to be running (cargo run in price-action-engine).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

DEFAULT_ENGINE_URL = "http://127.0.0.1:8001"
SCENARIOS_FILE = Path(__file__).resolve().parent / "regression_scenarios.json"
BASELINE_FILE = Path(__file__).resolve().parent / "regression_baseline.json"

BACKTEST_PARAMS = {
    "max_holding_bars": 48,
    "fee_bps": 10.0,
    "slippage_bps": 5.0,
    "persist": False,
}


@dataclass(frozen=True)
class Scenario:
    data: str
    model: str
    market: str
    symbol: str
    interval: str

    @property
    def key(self) -> str:
        return f"{self.symbol} {self.interval} + {self.model}"


def load_scenarios(path: str | Path) -> list[Scenario]:
    with open(path) as f:
        raw = json.load(f)
    return [
        Scenario(
            data=item["data"],
            model=item["model"],
            market=item["market"],
            symbol=item["symbol"],
            interval=item["interval"],
        )
        for item in raw
    ]


def load_klines(filepath: str) -> list[dict[str, Any]]:
    with open(filepath) as f:
        data = json.load(f)
    return data.get("klines", data)


def run_backtest(
    engine_url: str,
    klines: list[dict[str, Any]],
    scenario: Scenario,
) -> dict[str, Any]:
    req = {
        "market": scenario.market,
        "symbol": scenario.symbol,
        "interval": scenario.interval,
        "model_code": scenario.model,
        "klines": klines,
        **BACKTEST_PARAMS,
    }
    resp = requests.post(f"{engine_url}/api/v1/backtest", json=req, timeout=120)
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    return resp.json()


def load_baseline(path: str | Path) -> dict[str, dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def strip_trades(result: dict[str, Any]) -> dict[str, Any]:
    """Keep only summary + metadata, drop trades array to keep baseline lean."""
    summary = result.get("summary", {})
    return {
        "model_code": result.get("model_code"),
        "symbol": result.get("symbol"),
        "interval": result.get("interval"),
        "market": result.get("market"),
        "summary": summary,
    }


def save_baseline(path: str | Path, results: dict[str, dict[str, Any]]) -> None:
    stripped = {k: strip_trades(v) for k, v in results.items()}
    with open(path, "w") as f:
        json.dump(stripped, f, ensure_ascii=False, indent=2)


def compare(
    results: dict[str, dict[str, Any]],
    baseline: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, result in results.items():
        summary = result.get("summary", {})
        base = baseline.get(key, {}).get("summary", baseline.get(key, {}))
        rows.append({
            "key": key,
            "signal_count": summary.get("signal_count", 0),
            "win_rate": summary.get("win_rate", 0),
            "avg_return_pct": summary.get("avg_return_pct", 0),
            "median_return_pct": summary.get("median_return_pct", 0),
            "hit_stop_rate": summary.get("hit_stop_rate", 0),
            "hit_target_rate": summary.get("hit_target_rate", 0),
            "avg_holding_bars": summary.get("avg_holding_bars", 0),
            "base_signal_count": base.get("signal_count", 0),
            "base_win_rate": base.get("win_rate", 0),
            "base_avg_return_pct": base.get("avg_return_pct", 0),
        })
    return rows


def print_report(
    results: dict[str, dict[str, Any]],
    baseline: dict[str, dict[str, Any]],
) -> None:
    rows = compare(results, baseline)
    has_baseline = any(r["base_signal_count"] > 0 for r in rows)

    print()
    print("=" * 115)
    print("REGRESSION TEST REPORT")
    print("=" * 115)

    # Summary table
    header = f"{'Scenario':<47} {'Signals':>7} {'WR':>7} {'AvgRet':>9}"
    if has_baseline:
        header += f" {'vsBase WR':>10} {'vsBase Avg':>11}"
    print(header)
    print("-" * 115)

    for r in rows:
        line = (
            f"{r['key']:<47} {r['signal_count']:>7} "
            f"{r['win_rate']:>6.1%} {r['avg_return_pct']:>+8.3f}%"
        )
        if has_baseline:
            wr_d = r["win_rate"] - r["base_win_rate"]
            avg_d = r["avg_return_pct"] - r["base_avg_return_pct"]
            line += f" {wr_d:>+9.1%} {avg_d:>+10.3f}%"
        print(line)

    # Per-scenario detail
    print()
    print("-" * 115)
    for key, result in results.items():
        summary = result.get("summary", {})
        if "error" in result:
            print(f"\n{key}: ERROR - {result['error']}")
            continue
        print(f"\n{key}:")
        print(f"  signals={summary.get('signal_count', 0)}")
        print(f"  win_rate={summary.get('win_rate', 0):.1%}")
        print(f"  avg_return={summary.get('avg_return_pct', 0):+.3f}%")
        print(f"  median_return={summary.get('median_return_pct', 0):+.3f}%")
        print(f"  hit_stop_rate={summary.get('hit_stop_rate', 0):.1%}")
        print(f"  hit_target_rate={summary.get('hit_target_rate', 0):.1%}")
        print(f"  avg_holding_bars={summary.get('avg_holding_bars', 0):.1f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Regression test for backtest models")
    parser.add_argument(
        "--engine-url", default=DEFAULT_ENGINE_URL,
        help=f"Rust engine base URL (default: {DEFAULT_ENGINE_URL})",
    )
    parser.add_argument(
        "--scenarios", default=str(SCENARIOS_FILE),
        help=f"Path to scenarios JSON (default: {SCENARIOS_FILE})",
    )
    parser.add_argument(
        "--baseline", default=str(BASELINE_FILE),
        help=f"Path to baseline JSON (default: {BASELINE_FILE})",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Save current results as new baseline",
    )
    parser.add_argument(
        "--scenario", dest="filter_model",
        help="Only run scenarios matching this model code substring",
    )
    args = parser.parse_args()

    # Check engine
    try:
        r = requests.get(f"{args.engine_url}/api/v1/health", timeout=3)
        if not r.ok:
            print(f"Engine not healthy: HTTP {r.status_code}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"Cannot reach engine at {args.engine_url}: {e}", file=sys.stderr)
        print("Start the Rust engine first: cd price-action-engine && cargo run", file=sys.stderr)
        return 1

    # Load scenarios
    scenarios = load_scenarios(args.scenarios)
    if args.filter_model:
        scenarios = [s for s in scenarios if args.filter_model.lower() in s.model.lower()]
        if not scenarios:
            print(f"No scenarios match filter '{args.filter_model}'", file=sys.stderr)
            return 1

    print(f"Running {len(scenarios)} scenario(s)...")

    # Run backtests
    results: dict[str, dict[str, Any]] = {}
    for i, scenario in enumerate(scenarios, 1):
        print(f"[{i}/{len(scenarios)}] {scenario.key} ... ", end="", flush=True)
        klines = load_klines(scenario.data)
        result = run_backtest(args.engine_url, klines, scenario)
        results[scenario.key] = result
        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            s = result.get("summary", {})
            print(f"{s.get('signal_count', 0)} signals, WR={s.get('win_rate', 0):.1%}")

    # Compare with baseline
    baseline = load_baseline(args.baseline)
    print_report(results, baseline)

    # Update baseline
    if args.update:
        save_baseline(args.baseline, results)
        print(f"\nBaseline updated: {args.baseline}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
