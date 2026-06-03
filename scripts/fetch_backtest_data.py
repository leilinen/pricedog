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

import urllib.parse

import httpx

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
TENCENT_KLINES_URL = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
SINA_KLINES_URL = "https://quotes.sina.cn/cn/api/jsonp_v2.php/callback/CN_MarketDataService.getKLineData"
EASTMONEY_KLINES_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
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
    timeout: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch historical trade data for backtesting. "
        "Crypto uses Binance/OKX REST directly; "
        "CN/HK stocks use Tencent/EastMoney REST directly; "
        "US stocks use Stooq."
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


def fetch_stock_klines(client: httpx.Client, cfg: FetchConfig) -> tuple[list[dict[str, Any]], str]:
    """Fetch stock K-lines with provider fallback (same pattern as crypto).

    Provider priority:
    - CN intraday: Sina → Tencent → EastMoney
    - CN daily / HK: Tencent → Sina → EastMoney
    - US: Stooq
    """
    errors: list[str] = []

    # CN intraday: Sina first (most reliable for intraday)
    if cfg.market == "CN" and cfg.interval != "1d":
        try:
            rows = fetch_sina_stock_klines(client, cfg)
            if rows:
                return rows, "sina"
        except Exception as exc:
            errors.append(f"sina:{exc}")

    try:
        rows = fetch_tencent_stock_klines(client, cfg)
        if rows:
            return rows, "tencent"
    except Exception as exc:
        errors.append(f"tencent:{exc}")

    # Fallback: Sina for daily if Tencent fails
    if cfg.market == "CN" and cfg.interval == "1d" and "sina" not in ";".join(errors):
        try:
            rows = fetch_sina_stock_klines(client, cfg)
            if rows:
                return rows, "sina"
        except Exception as exc:
            errors.append(f"sina:{exc}")

    try:
        return fetch_eastmoney_stock_klines(client, cfg), "eastmoney"
    except Exception as exc:
        errors.append(f"eastmoney:{exc}")
    raise RuntimeError("stock providers failed; " + "; ".join(errors))


# ── Stock helper functions ──────────────────────────────────

def cn_prefix(symbol: str) -> str:
    """CN stock symbol → Tencent exchange prefix: sh/sz."""
    s = symbol.strip()
    if s.startswith("5") or s.startswith("6") or s.startswith("9"):
        return "sh"
    return "sz"


def tencent_stock_period(interval: str) -> str:
    """Normalized interval → Tencent period parameter."""
    return {
        "5m": "m5",
        "15m": "m15",
        "30m": "m30",
        "1h": "m60",
        "1d": "day",
    }[normalize_interval(interval)]


def eastmoney_secid(symbol: str, market: str) -> str:
    """Symbol + market → EastMoney secid format."""
    if market == "HK":
        return f"116.{symbol}"
    if market == "US":
        return f"105.{symbol}"
    # CN: SH → 1.xxx, SZ → 0.xxx
    prefix = "1" if cn_prefix(symbol) == "sh" else "0"
    return f"{prefix}.{symbol}"


def eastmoney_klt(interval: str) -> str:
    """Normalized interval → EastMoney klt parameter."""
    return {
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "1d": "101",
    }[normalize_interval(interval)]


def ms_to_date_str(ms: int) -> str:
    """Milliseconds timestamp → YYYY-MM-DD string."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def ms_to_compact_date(ms: int) -> str:
    """Milliseconds timestamp → YYYYMMDD string."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y%m%d")


# ── Sina stock K-lines ──────────────────────────────────────

SINA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://finance.sina.com.cn/",
}


def sina_scale(interval: str) -> str:
    """Normalized interval → Sina scale parameter."""
    return {
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "1d": "240",
    }[normalize_interval(interval)]


def fetch_sina_stock_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    """Fetch CN stock K-lines from Sina Finance API.

    Sina supports intraday (5m, 15m, 30m, 60) and daily (scale=240).
    Max datalen ~1200 bars. Returns recent data in reverse chronological order.
    """
    if cfg.market != "CN":
        raise ValueError(f"Sina klines only support CN market, got: {cfg.market}")

    sym = f"{cn_prefix(cfg.symbol)}{cfg.symbol}"
    scale = sina_scale(cfg.interval)

    # Estimate needed bars from time range
    if cfg.interval == "1d":
        trading_days = (cfg.end_ms - cfg.start_ms) // (24 * 60 * 60 * 1000)
        datalen = min(max(32, trading_days + 20), 1200)
    else:
        bars_per_day = max(1, 8 * 60 * 60 * 1000 // interval_millis(cfg.interval))
        trading_days = (cfg.end_ms - cfg.start_ms) // (24 * 60 * 60 * 1000)
        datalen = min(max(32, trading_days * bars_per_day // 3 + 20), 1200)

    params = {
        "symbol": sym,
        "scale": scale,
        "ma": "no",
        "datalen": str(datalen),
    }

    resp = client.get(SINA_KLINES_URL, params=params, headers=SINA_HEADERS)
    resp.raise_for_status()
    text = resp.text

    # Parse JSONP: callback([...])
    start = text.index("(") + 1
    end = text.rindex(")")
    json_str = text[start:end]
    data = json.loads(json_str)
    if not data:
        return []

    rows: list[dict[str, Any]] = []
    for item in data:
        ts_str = item.get("day", "")
        if not ts_str:
            continue
        try:
            ts_ms = parse_datetime_to_utc_ms(ts_str, is_end=False)
        except Exception:
            continue
        if not (cfg.start_ms <= ts_ms < cfg.end_ms):
            continue
        rows.append({
            "ts": iso_utc(ts_ms),
            "open": safe_float(item.get("open")),
            "close": safe_float(item.get("close")),
            "high": safe_float(item.get("high")),
            "low": safe_float(item.get("low")),
            "volume": safe_float(item.get("volume")),
            "turnover": safe_float(item.get("amount", 0)),
        })

    return rows


# ── Tencent stock K-lines ───────────────────────────────────

TENCENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def fetch_tencent_stock_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    """Fetch stock K-lines from Tencent API.

    Tencent only supports count-based fetching (no start/end date range).
    We calculate the approximate bar count from the time range, fetch that many bars,
    then filter client-side.
    """
    if cfg.market == "CN":
        sym = f"{cn_prefix(cfg.symbol)}{cfg.symbol}"
    elif cfg.market == "HK":
        sym = f"hk{cfg.symbol}"
    elif cfg.market == "US":
        sym = f"us{cfg.symbol}"
    else:
        raise ValueError(f"unsupported market for Tencent: {cfg.market}")

    period = tencent_stock_period(cfg.interval)
    # Estimate bar count: trading hours for intraday, ~250 trading days/year
    if cfg.interval == "1d":
        approx_bars = max(32, ((cfg.end_ms - cfg.start_ms) // interval_millis("1d")) + 10)
    else:
        # Intraday: ~4 bars/day for 1h, ~240 bars/day for 5m etc.
        bars_per_day = max(1, 8 * 60 * 60 * 1000 // interval_millis(cfg.interval))
        trading_days = (cfg.end_ms - cfg.start_ms) // (24 * 60 * 60 * 1000)
        approx_bars = max(32, trading_days * bars_per_day // 3 + 10)
    count = min(approx_bars, 5000)

    # Tencent API: empty start/end = use count only
    param = f"{sym},{period},,,{count},qfq"
    encoded_param = urllib.parse.quote(param, safe="")
    url = f"{TENCENT_KLINES_URL}?param={encoded_param}&_var=kline_{period}qfq"

    resp = client.get(url, headers=TENCENT_HEADERS, follow_redirects=True)
    resp.raise_for_status()
    text = resp.text

    # Parse JS variable format: kline_dayqfq={...};
    eq_idx = text.find("=")
    if eq_idx < 0:
        return []
    json_str = text[eq_idx + 1:].strip()
    if json_str.endswith(";"):
        json_str = json_str[:-1]

    data = json.loads(json_str)
    raw_data = data.get("data")

    # Extract kline array from response (handles both old and new formats)
    day_data: list = []
    if isinstance(raw_data, list):
        day_data = raw_data
    elif isinstance(raw_data, dict) and isinstance(raw_data.get(sym), dict):
        sd = raw_data[sym]
        qfq_key = f"qfq{period}"
        day_data = (
            sd.get(qfq_key) or sd.get(period)
            or sd.get("qfqday") or sd.get("day") or []
        )

    rows: list[dict[str, Any]] = []
    for item in day_data:
        if not isinstance(item, list) or len(item) < 5:
            continue
        ts_str = str(item[0])
        try:
            ts_ms = parse_datetime_to_utc_ms(ts_str, is_end=False)
        except Exception:
            continue
        if not (cfg.start_ms <= ts_ms < cfg.end_ms):
            continue
        rows.append({
            "ts": iso_utc(ts_ms),
            "open": safe_float(item[1]),
            "close": safe_float(item[2]),
            "high": safe_float(item[3]),
            "low": safe_float(item[4]),
            "volume": safe_float(item[5]) if len(item) > 5 else 0.0,
            "turnover": safe_float(item[6]) if len(item) > 6 else 0.0,
        })

    return rows


# ── EastMoney stock K-lines ─────────────────────────────────

EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}


def fetch_eastmoney_stock_klines(client: httpx.Client, cfg: FetchConfig) -> list[dict[str, Any]]:
    """Fetch stock K-lines from EastMoney API with time range support (fallback)."""
    if cfg.market not in ("CN", "HK"):
        raise ValueError(f"unsupported market for EastMoney klines: {cfg.market}")

    secid = eastmoney_secid(cfg.symbol, cfg.market)
    klt = eastmoney_klt(cfg.interval)
    beg = ms_to_compact_date(cfg.start_ms)
    end = ms_to_compact_date(cfg.end_ms)

    params = {
        "secid": secid,
        "klt": klt,
        "fqt": "1",
        "lmt": "10000",
        "beg": beg,
        "end": end,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }

    resp = client.get(EASTMONEY_KLINES_URL, params=params, headers=EASTMONEY_HEADERS)
    resp.raise_for_status()
    payload = resp.json()

    raw = payload.get("data", {}).get("klines") or []
    rows: list[dict[str, Any]] = []
    for s in raw:
        parts = s.split(",")
        if len(parts) < 6:
            continue
        ts_str = parts[0]
        try:
            ts_ms = parse_datetime_to_utc_ms(ts_str, is_end=False)
        except Exception:
            continue
        if not (cfg.start_ms <= ts_ms < cfg.end_ms):
            continue
        rows.append({
            "ts": iso_utc(ts_ms),
            "open": safe_float(parts[1]),
            "close": safe_float(parts[2]),
            "high": safe_float(parts[3]),
            "low": safe_float(parts[4]),
            "volume": safe_float(parts[5]),
            "turnover": 0.0,
        })

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
                rows, source = fetch_stock_klines(client, cfg)
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
