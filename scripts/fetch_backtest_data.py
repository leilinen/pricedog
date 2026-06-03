#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
DEFAULT_DATA_PROVIDER_URL = os.environ.get("DATA_PROVIDER_URL", "http://127.0.0.1:8003")
DEFAULT_TIMEOUT = 30.0
BINANCE_LIMIT = 1000
OKX_LIMIT = 100
BACKTEST_DATA_SCHEMA_VERSION = "v1"
TS_MODE = "bar_open"
TIMEZONE = "UTC"
QUALITY_MODE_CONTINUOUS_24_7 = "continuous_24_7"
QUALITY_MODE_BEST_EFFORT = "best_effort"


@dataclass(frozen=True)
class FetchConfig:
    market: str
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    data_provider_url: str
    timeout: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch historical trade data for backtesting. "
        "Crypto uses Binance REST directly; stocks use the akshare-adapter HTTP API."
    )
    parser.add_argument("--symbol", required=True, help="Symbol, for example BTCUSDT, 600519, AAPL")
    parser.add_argument(
        "--market",
        default="AUTO",
        help="AUTO, CRYPTO, CN, US, HK. AUTO infers from symbol when possible.",
    )
    parser.add_argument(
        "--interval",
        required=True,
        help="Supported: 5m, 15m, 30m, 1h, 2h, 4h, 1d",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start time, date or datetime. Examples: 2024-01-01, 2024-01-01T00:00:00Z",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End time, date or datetime. Examples: 2024-03-01, 2024-03-01T00:00:00Z",
    )
    parser.add_argument(
        "--output",
        help="Output path. Defaults to stdout for JSON, or stdout CSV when --format csv is used.",
    )
    parser.add_argument(
        "--format",
        default="json",
        choices=("json", "csv"),
        help="Output format. Default: json",
    )
    parser.add_argument(
        "--data-provider-url",
        default=DEFAULT_DATA_PROVIDER_URL,
        help=f"Data provider service URL for stock data. Default: {DEFAULT_DATA_PROVIDER_URL}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT}",
    )
    return parser.parse_args()


def normalize_interval(interval: str) -> str:
    value = interval.strip().lower()
    mapping = {
        "5": "5m",
        "5m": "5m",
        "5min": "5m",
        "15": "15m",
        "15m": "15m",
        "15min": "15m",
        "30": "30m",
        "30m": "30m",
        "30min": "30m",
        "60": "1h",
        "60m": "1h",
        "1h": "1h",
        "120": "2h",
        "120m": "2h",
        "2h": "2h",
        "240": "4h",
        "240m": "4h",
        "4h": "4h",
        "1d": "1d",
        "d": "1d",
        "day": "1d",
        "daily": "1d",
    }
    if value not in mapping:
        raise ValueError(f"unsupported interval: {interval}")
    return mapping[value]


def interval_millis(interval: str) -> int:
    value = normalize_interval(interval)
    mapping = {
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "2h": 2 * 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "1d": 24 * 60 * 60 * 1000,
    }
    return mapping[value]


def parse_datetime_to_utc_ms(raw: str, is_end: bool) -> int:
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        if "T" in value or " " in value:
            raise
        base = datetime.fromisoformat(value)
        if is_end:
            dt = datetime(base.year, base.month, base.day, 23, 59, 59, 999000)
        else:
            dt = datetime(base.year, base.month, base.day, 0, 0, 0, 0)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp() * 1000)


def infer_market(symbol: str) -> str:
    clean = symbol.strip().upper()
    crypto_quotes = ("USDT", "USDC", "USD", "BTC", "ETH")
    if "-" in clean:
        return "CRYPTO"
    if any(clean.endswith(quote) and len(clean) > len(quote) for quote in crypto_quotes):
        return "CRYPTO"
    if clean.isdigit() and len(clean) in (5, 6):
        return "CN"
    if clean.isalpha():
        return "US"
    raise ValueError(f"unable to infer market from symbol: {symbol}; pass --market explicitly")


def iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def build_config(args: argparse.Namespace) -> FetchConfig:
    market = args.market.strip().upper()
    if market == "AUTO":
        market = infer_market(args.symbol)
    interval = normalize_interval(args.interval)
    start_ms = parse_datetime_to_utc_ms(args.start, is_end=False)
    end_ms = parse_datetime_to_utc_ms(args.end, is_end=True)
    if start_ms >= end_ms:
        raise ValueError("--start must be earlier than --end")
    return FetchConfig(
        market=market,
        symbol=args.symbol.strip().upper(),
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        data_provider_url=args.data_provider_url.rstrip("/"),
        timeout=float(args.timeout),
    )


def fetch_crypto_klines(client: httpx.Client, cfg: FetchConfig) -> tuple[list[dict[str, Any]], str]:
    errors: list[str] = []
    try:
        return fetch_binance_klines(client, cfg), "binance"
    except Exception as exc:
        errors.append(f"binance:{exc}")
    try:
        return fetch_okx_klines(client, cfg), "okx"
    except Exception as exc:
        errors.append(f"okx:{exc}")
    raise RuntimeError("crypto providers failed; " + "; ".join(errors))


def fetch_binance_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    step_ms = interval_millis(cfg.interval)
    start_ms = cfg.start_ms
    rows: list[dict[str, Any]] = []

    while start_ms < cfg.end_ms:
        params = {
            "symbol": cfg.symbol,
            "interval": cfg.interval,
            "startTime": start_ms,
            "endTime": cfg.end_ms,
            "limit": BINANCE_LIMIT,
        }
        resp = client.get(BINANCE_KLINES_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"invalid Binance response: {payload}")
        if not payload:
            break

        batch_count = 0
        last_open_ms = start_ms
        for item in payload:
            if not isinstance(item, list) or len(item) < 8:
                continue
            open_ms = int(item[0])
            if open_ms >= cfg.end_ms:
                continue
            rows.append(
                {
                    "ts": iso_utc(open_ms),
                    "open": safe_float(item[1]),
                    "high": safe_float(item[2]),
                    "low": safe_float(item[3]),
                    "close": safe_float(item[4]),
                    "volume": safe_float(item[5]),
                    "turnover": safe_float(item[7]),
                }
            )
            batch_count += 1
            last_open_ms = open_ms

        if batch_count == 0:
            break
        if batch_count < BINANCE_LIMIT:
            break
        start_ms = last_open_ms + step_ms

    return rows


def fetch_okx_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    okx_interval = okx_interval_for(cfg.interval)
    step_ms = interval_millis(cfg.interval)
    # OKX history-candles: returns data in reverse chronological order.
    # Pagination: set "after" to the smallest ts of current batch to fetch older data.
    after_ms = cfg.end_ms
    rows: list[dict[str, Any]] = []

    while after_ms > cfg.start_ms:
        params = {
            "instId": okx_inst_id(cfg.symbol),
            "bar": okx_interval,
            "after": after_ms,
            "limit": OKX_LIMIT,
        }
        resp = client.get(OKX_HISTORY_CANDLES_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("code", "")) != "0":
            raise RuntimeError(payload.get("msg") or "okx kline error")
        data = payload.get("data") or []
        if not isinstance(data, list) or not data:
            break

        batch_ts: list[int] = []
        for item in data:
            if not isinstance(item, list) or len(item) < 8:
                continue
            open_ms = int(item[0])
            batch_ts.append(open_ms)
            if cfg.start_ms <= open_ms < cfg.end_ms:
                rows.append(
                    {
                        "ts": iso_utc(open_ms),
                        "open": safe_float(item[1]),
                        "high": safe_float(item[2]),
                        "low": safe_float(item[3]),
                        "close": safe_float(item[4]),
                        "volume": safe_float(item[5]),
                        "turnover": safe_float(item[7]),
                    }
                )

        if not batch_ts:
            break
        min_ts = min(batch_ts)
        if min_ts <= cfg.start_ms:
            break
        after_ms = min_ts

    return rows


def okx_interval_for(interval: str) -> str:
    return {
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "1d": "1D",
    }[normalize_interval(interval)]


def okx_inst_id(symbol: str) -> str:
    clean = symbol.strip().upper()
    if "-" in clean:
        return clean
    for quote in ("USDT", "USDC", "USD", "BTC", "ETH"):
        if clean.endswith(quote) and len(clean) > len(quote):
            return f"{clean[:-len(quote)]}-{quote}"
    return clean


def fetch_stock_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    step_ms = interval_millis(cfg.interval)
    approx_limit = max(32, ((cfg.end_ms - cfg.start_ms) // step_ms) + 4)
    url = f"{cfg.data_provider_url}/api/v1/klines/{cfg.market}/{cfg.symbol}"
    resp = client.get(url, params={"interval": cfg.interval, "limit": approx_limit})
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        raise RuntimeError(payload.get("message") or "data-provider request failed")
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    rows = []
    for item in klines:
        if not isinstance(item, dict):
            continue
        ts_ms = parse_datetime_to_utc_ms(str(item.get("ts", "")), is_end=False)
        rows.append(
            {
                "ts": iso_utc(ts_ms),
                "open": safe_float(item.get("open")),
                "high": safe_float(item.get("high")),
                "low": safe_float(item.get("low")),
                "close": safe_float(item.get("close")),
                "volume": safe_float(item.get("volume")),
                "turnover": safe_float(item.get("turnover")),
            }
        )
    return rows


def normalize_klines(rows: list[dict[str, Any]], cfg: FetchConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    step_ms = interval_millis(cfg.interval)
    by_ts: dict[str, dict[str, Any]] = {}
    filtered_count = 0
    for row in rows:
        ts = str(row["ts"])
        ts_ms = parse_datetime_to_utc_ms(ts, is_end=False)
        if cfg.start_ms <= ts_ms < cfg.end_ms:
            filtered_count += 1
            by_ts[ts] = row
    normalized = [by_ts[ts] for ts in sorted(by_ts)]
    expected_ts_ms = list(range(cfg.start_ms, cfg.end_ms, step_ms))
    actual_ts_ms = {
        parse_datetime_to_utc_ms(str(row["ts"]), is_end=False)
        for row in normalized
    }
    missing_ts = [ts for ts in expected_ts_ms if ts not in actual_ts_ms]
    duplicate_bars = max(0, filtered_count - len(by_ts))
    quality = {
        "expected_bars": len(expected_ts_ms),
        "actual_bars": len(normalized),
        "missing_bars": len(missing_ts),
        "duplicate_bars": duplicate_bars,
        "is_continuous": len(missing_ts) == 0
        and duplicate_bars == 0
        and len(normalized) == len(expected_ts_ms),
        "first_ts": normalized[0]["ts"] if normalized else None,
        "last_ts": normalized[-1]["ts"] if normalized else None,
        "missing_timestamps_sample": [iso_utc(ts) for ts in missing_ts[:50]],
    }
    return normalized, quality


def build_output(
    cfg: FetchConfig,
    rows: list[dict[str, Any]],
    source: str,
    quality: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": BACKTEST_DATA_SCHEMA_VERSION,
        "market": cfg.market,
        "symbol": cfg.symbol,
        "interval": cfg.interval,
        "source": source,
        "timezone": TIMEZONE,
        "ts_mode": TS_MODE,
        "quality_mode": quality_mode_for_market(cfg.market),
        "start": iso_utc(cfg.start_ms),
        "end": iso_utc(cfg.end_ms),
        "count": len(rows),
        "quality": quality,
        "klines": rows,
    }


def quality_mode_for_market(market: str) -> str:
    if market.strip().upper() == "CRYPTO":
        return QUALITY_MODE_CONTINUOUS_24_7
    return QUALITY_MODE_BEST_EFFORT


def write_json(payload: dict[str, Any], output: str | None) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if output:
        Path(output).write_text(text + "\n", encoding="utf-8")
        return
    sys.stdout.write(text)
    sys.stdout.write("\n")


def write_csv(rows: list[dict[str, Any]], output: str | None) -> None:
    fieldnames = ["ts", "open", "high", "low", "close", "volume", "turnover"]
    if output:
        with open(output, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)


def main() -> int:
    try:
        args = parse_args()
        cfg = build_config(args)
        with httpx.Client(timeout=cfg.timeout, headers={"User-Agent": "PriceDogBacktestFetcher/0.1"}) as client:
            if cfg.market == "CRYPTO":
                rows, source = fetch_crypto_klines(client, cfg)
            elif cfg.market in {"CN", "US", "HK"}:
                rows = fetch_stock_klines(client, cfg)
                source = "data-provider"
            else:
                raise ValueError(f"unsupported market: {cfg.market}")

        rows, quality = normalize_klines(rows, cfg)
        payload = build_output(cfg, rows, source, quality)
        if args.format == "csv":
            write_csv(rows, args.output)
        else:
            write_json(payload, args.output)
        return 0
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
