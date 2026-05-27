from __future__ import annotations

import math
import json
from datetime import datetime
from time import sleep
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="PriceDog AkShare Adapter")

TENCENT_QUOTE_URL = "http://qt.gtimg.cn/q="
TENCENT_KLINE_URL = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


class AdapterResponse(BaseModel):
    success: bool
    data: Any = None
    error: str = ""


class KlinePoint(BaseModel):
    ts: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    turnover: float = 0.0


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return 0.0
        return out
    except Exception:
        return 0.0


def _normalize_columns(df) -> dict[str, str]:
    cols = {str(c).strip().lower(): str(c) for c in getattr(df, "columns", [])}
    aliases = {
        "ts": ["时间", "日期", "date", "datetime", "time"],
        "open": ["开盘", "open"],
        "high": ["最高", "high"],
        "low": ["最低", "low"],
        "close": ["收盘", "close"],
        "volume": ["成交量", "volume", "vol"],
        "turnover": ["成交额", "amount", "turnover"],
    }
    out: dict[str, str] = {}
    for key, names in aliases.items():
        for name in names:
            found = cols.get(name.lower())
            if found:
                out[key] = found
                break
    return out


def _df_to_klines(df, limit: int) -> list[dict]:
    if df is None or getattr(df, "empty", True):
        return []
    col = _normalize_columns(df)
    if any(k not in col for k in ("ts", "open", "high", "low", "close")):
        return []

    rows = []
    for _, row in df.tail(max(1, int(limit or 120))).iterrows():
        rows.append(
            KlinePoint(
                ts=str(row[col["ts"]]),
                open=_safe_float(row[col["open"]]),
                high=_safe_float(row[col["high"]]),
                low=_safe_float(row[col["low"]]),
                close=_safe_float(row[col["close"]]),
                volume=_safe_float(row[col["volume"]]) if "volume" in col else 0.0,
                turnover=_safe_float(row[col["turnover"]]) if "turnover" in col else 0.0,
            ).model_dump()
        )
    return rows


def _ak_period(interval: str) -> str:
    return {
        "5": "5",
        "5m": "5",
        "5min": "5",
        "5mins": "5",
        "5minute": "5",
        "5minutes": "5",
        "15": "15",
        "15m": "15",
        "15min": "15",
        "15mins": "15",
        "15minute": "15",
        "15minutes": "15",
        "30": "30",
        "30m": "30",
        "30min": "30",
        "30mins": "30",
        "30minute": "30",
        "30minutes": "30",
        "60": "60",
        "1h": "60",
        "60m": "60",
        "1hour": "60",
        "1hours": "60",
        "1d": "daily",
        "d": "daily",
        "day": "daily",
        "daily": "daily",
    }.get((interval or "1d").strip().lower(), "")


def _em_klt(interval: str) -> str:
    return {
        "5": "5",
        "5m": "5",
        "5min": "5",
        "5mins": "5",
        "5minute": "5",
        "5minutes": "5",
        "15": "15",
        "15m": "15",
        "15min": "15",
        "15mins": "15",
        "15minute": "15",
        "15minutes": "15",
        "30": "30",
        "30m": "30",
        "30min": "30",
        "30mins": "30",
        "30minute": "30",
        "30minutes": "30",
        "60": "60",
        "1h": "60",
        "60m": "60",
        "1hour": "60",
        "1hours": "60",
        "1d": "101",
        "d": "101",
        "day": "101",
        "daily": "101",
    }.get((interval or "1d").strip().lower(), "")


def _retry(call, attempts: int = 3, delay_seconds: float = 0.8):
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            return call()
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleep(delay_seconds * (attempt + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError("retry_call_failed")


def _provider_error(exc: Exception) -> str:
    return f"{exc.__class__.__name__}:{str(exc)[:220]}"


def _cn_secid(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol.startswith(("5", "6", "9")):
        return f"1.{symbol}"
    if symbol.startswith(("83", "87", "88", "92")):
        return f"0.{symbol}"
    return f"0.{symbol}"


def _fetch_eastmoney_cn_klines(symbol: str, interval: str, limit: int) -> list[dict]:
    klt = _em_klt(interval)
    if not klt:
        return []
    params = {
        "secid": _cn_secid(symbol),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": klt,
        "fqt": "1",
        "beg": "0",
        "end": "20500101",
        "lmt": str(max(1, int(limit or 120))),
    }
    with httpx.Client(timeout=10, headers={"User-Agent": "PriceDog/0.1"}) as client:
        resp = _retry(lambda: client.get(EASTMONEY_KLINE_URL, params=params), attempts=3)
        resp.raise_for_status()
        payload = resp.json()
    rows = (((payload or {}).get("data") or {}).get("klines") or [])[-max(1, int(limit or 120)) :]
    bars = []
    for item in rows:
        parts = str(item).split(",")
        if len(parts) < 7:
            continue
        bars.append(
            KlinePoint(
                ts=parts[0],
                open=_safe_float(parts[1]),
                close=_safe_float(parts[2]),
                high=_safe_float(parts[3]),
                low=_safe_float(parts[4]),
                volume=_safe_float(parts[5]),
                turnover=_safe_float(parts[6]),
            ).model_dump()
        )
    return bars


def _fetch_tencent_cn_daily_klines(symbol: str, market: str, limit: int) -> list[dict]:
    tencent_symbol = _tencent_symbol(symbol, market)
    params = {
        "param": f"{tencent_symbol},day,,,{max(1, int(limit or 120))},qfq",
        "_var": "kline_dayqfq",
    }
    with httpx.Client(follow_redirects=True, timeout=10, headers={"User-Agent": "PriceDog/0.1"}) as client:
        resp = _retry(lambda: client.get(TENCENT_KLINE_URL, params=params), attempts=3)
        resp.raise_for_status()
        text = resp.text
    if "=" not in text:
        return []
    raw = text.split("=", 1)[1].strip()
    if raw.endswith(";"):
        raw = raw[:-1]
    payload = json.loads(raw)
    data = payload.get("data", {})
    day_data = []
    if isinstance(data, dict):
        stock_data = data.get(tencent_symbol, {})
        if isinstance(stock_data, dict):
            day_data = stock_data.get("day") or stock_data.get("qfqday") or []
    elif isinstance(data, list):
        day_data = data

    bars = []
    for item in day_data[-max(1, int(limit or 120)) :]:
        if len(item) < 5:
            continue
        bars.append(
            KlinePoint(
                ts=str(item[0]),
                open=_safe_float(item[1]),
                close=_safe_float(item[2]),
                high=_safe_float(item[3]),
                low=_safe_float(item[4]),
                volume=_safe_float(item[5]) if len(item) > 5 else 0.0,
                turnover=0.0,
            ).model_dump()
        )
    return bars


def _tencent_symbol(symbol: str, market: str) -> str:
    market = market.upper()
    symbol = symbol.strip().upper()
    if market == "HK":
        return f"hk{symbol}"
    if market == "US":
        return f"us{symbol}"
    if symbol.startswith(("5", "6", "9")):
        return f"sh{symbol}"
    if symbol.startswith(("83", "87", "88", "92")):
        return f"bj{symbol}"
    return f"sz{symbol}"


def _parse_tencent_line(line: str) -> dict | None:
    if "=\"\"" in line or not line.strip():
        return None
    try:
        _, value = line.split('="', 1)
        parts = value.rstrip('";').split("~")
        if len(parts) < 35:
            return None
        symbol = parts[2]
        if "." in symbol and not symbol.startswith("."):
            symbol = symbol.split(".")[0]
        turnover = 0.0
        if len(parts) > 35 and "/" in str(parts[35]):
            chunks = parts[35].split("/")
            if len(chunks) >= 3:
                turnover = _safe_float(chunks[2])
        return {
            "symbol": symbol,
            "name": parts[1],
            "current_price": _safe_float(parts[3]),
            "prev_close": _safe_float(parts[4]),
            "open_price": _safe_float(parts[5]),
            "volume": _safe_float(parts[6]),
            "change_amount": _safe_float(parts[31]),
            "change_pct": _safe_float(parts[32]),
            "high_price": _safe_float(parts[33]),
            "low_price": _safe_float(parts[34]),
            "turnover": turnover,
        }
    except Exception:
        return None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/klines/{market}/{symbol}")
def get_klines(market: str, symbol: str, interval: str = "1d", limit: int = 120):
    try:
        import akshare as ak

        market = market.upper()
        symbol = symbol.strip().upper()
        period = _ak_period(interval)
        if not period:
            return AdapterResponse(success=False, error=f"unsupported_interval:{interval}").model_dump()

        source = "akshare"
        provider_errors: list[str] = []
        if market == "CN":
            try:
                if period == "daily":
                    df = _retry(lambda: ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq"))
                else:
                    df = _retry(lambda: ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, adjust="qfq"))
                bars = _df_to_klines(df, limit)
            except Exception as exc:
                provider_errors.append(f"akshare:{_provider_error(exc)}")
                bars = []
            if not bars:
                if period == "daily":
                    try:
                        bars = _fetch_tencent_cn_daily_klines(symbol, market, limit)
                        if bars:
                            source = "tencent"
                    except Exception as exc:
                        provider_errors.append(f"tencent:{_provider_error(exc)}")
            if not bars:
                try:
                    bars = _fetch_eastmoney_cn_klines(symbol, interval, limit)
                    if bars:
                        source = "eastmoney"
                except Exception as exc:
                    provider_errors.append(f"eastmoney:{_provider_error(exc)}")
        elif market == "US":
            bars = []
            if period == "daily":
                for fn_name in ("stock_us_daily", "stock_us_hist"):
                    fn = getattr(ak, fn_name, None)
                    if not fn:
                        continue
                    try:
                        bars = _df_to_klines(_retry(lambda: fn(symbol=symbol)), limit)
                        if bars:
                            break
                    except Exception:
                        continue
            else:
                fn = getattr(ak, "stock_us_hist_min_em", None)
                bars = _df_to_klines(_retry(lambda: fn(symbol=symbol, period=period)), limit) if fn else []
        else:
            bars = []

        if not bars:
            error = "no_kline_data"
            if provider_errors:
                error = ";".join(provider_errors)
            return AdapterResponse(success=False, error=error).model_dump()
        return AdapterResponse(
            success=True,
            data={
                "market": market,
                "symbol": symbol,
                "interval": interval,
                "klines": bars,
                "source": source,
                "provider_errors": provider_errors,
            },
        ).model_dump()
    except ImportError:
        return AdapterResponse(success=False, error="akshare_not_installed").model_dump()
    except Exception as exc:
        return AdapterResponse(success=False, error=str(exc)).model_dump()


@app.get("/quote/{market}/{symbol}")
def get_quote(market: str, symbol: str):
    market = market.upper()
    symbol = symbol.strip().upper()
    try:
        tencent_symbol = _tencent_symbol(symbol, market)
        with httpx.Client(timeout=10) as client:
            resp = client.get(TENCENT_QUOTE_URL + tencent_symbol)
            content = resp.content.decode("gbk", errors="ignore")
        parsed = None
        for line in content.strip().split(";"):
            parsed = _parse_tencent_line(line)
            if parsed and parsed["current_price"] > 0:
                break
        if not parsed:
            return AdapterResponse(success=False, error="no_quote_data").model_dump()
        parsed.update({"market": market, "timestamp": datetime.now().isoformat(), "source": "tencent"})
        return AdapterResponse(success=True, data=parsed).model_dump()
    except Exception as exc:
        return AdapterResponse(success=False, error=str(exc)).model_dump()
