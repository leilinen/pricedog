"""价格提醒引擎：规则评估、命中落库与通知发送。"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy.orm import Session

from src.core.notifier import NotifierManager
from src.core.providers import ProviderRequest, get_quote_orchestrator
from src.models.market import MarketCode, MARKETS
from src.web.database import SessionLocal
from src.web.models import NotifyChannel, PriceAlertHit, PriceAlertRule, Stock

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _to_market(market: str) -> MarketCode:
    try:
        return MarketCode(market)
    except Exception:
        return MarketCode.CN


def _is_trading_time(market: MarketCode) -> bool:
    market_def = MARKETS.get(market)
    if not market_def:
        return False
    return market_def.is_trading_time()


def _day_key(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _minute_bucket(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


def _json_get(obj: dict, key: str, default=None):
    try:
        return obj.get(key, default)
    except Exception:
        return default


def _op_eval(left: float | None, op: str, right: Any) -> bool:
    if left is None:
        return False
    o = (op or "").strip().lower()
    if o in ("between", "in"):
        if not isinstance(right, (list, tuple)) or len(right) != 2:
            return False
        lo = _safe_float(right[0])
        hi = _safe_float(right[1])
        if lo is None or hi is None:
            return False
        return lo <= left <= hi

    rv = _safe_float(right)
    if rv is None:
        return False
    if o == ">":
        return left > rv
    if o == ">=":
        return left >= rv
    if o == "<":
        return left < rv
    if o == "<=":
        return left <= rv
    if o in ("=", "=="):
        return left == rv
    if o in ("!=", "<>"):
        return left != rv
    return False


def _engine_url() -> str:
    return (os.environ.get("PRICE_ACTION_ENGINE_URL") or "http://127.0.0.1:8001").rstrip("/")


async def _fetch_price_action_quote(market: MarketCode, symbol: str) -> dict | None:
    url = f"{_engine_url()}/api/v1/quote/{market.value}/{symbol}"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
        if isinstance(payload, dict) and payload.get("ok") is False:
            return None
        quote = payload.get("quote") if isinstance(payload, dict) else None
        if isinstance(quote, dict):
            return quote
    except Exception as e:
        logger.debug(f"Price Action Engine quote unavailable {market.value}:{symbol}: {e}")
    return None


async def _evaluate_price_action(
    market: MarketCode, symbol: str, interval: str = "1d", *, model_code: str = ""
) -> dict:
    url = f"{_engine_url()}/api/v1/evaluate"
    payload = {
        "market": market.value,
        "symbol": symbol,
        "interval": interval,
        "klines": [],
        "persist_signal": False,
    }
    if model_code:
        payload["model_code"] = model_code
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.debug(f"Price Action Engine evaluate unavailable {market.value}:{symbol}: {e}")
    return {"ok": False, "error": "engine_unavailable"}


@dataclass
class RuleEvalResult:
    matched: bool
    hits: list[dict]
    snapshot: dict


class PriceAlertEngine:
    """价格提醒扫描执行引擎（支持小规模缓存和去重）。"""

    def __init__(self):
        self._quote_cache: dict[str, tuple[float, dict]] = {}
        self._engine_eval_cache: dict[str, tuple[float, dict]] = {}
        self.quote_ttl_sec = 5.0
        self.engine_eval_ttl_sec = 60.0

    async def _fetch_quotes_map(self, stocks: list[Stock]) -> dict[tuple[str, str], dict]:
        """走 QuoteOrchestrator,支持多 provider 主备故障转移。"""
        grouped: dict[MarketCode, list[Stock]] = {}
        for s in stocks:
            grouped.setdefault(_to_market(s.market), []).append(s)

        orch = get_quote_orchestrator()
        out: dict[tuple[str, str], dict] = {}
        for market, items in grouped.items():
            symbols = [s.symbol for s in items]
            if not symbols:
                continue
            if market == MarketCode.CRYPTO:
                for sym in symbols:
                    q = await _fetch_price_action_quote(market, sym)
                    if q:
                        out[(market.value, sym)] = q
                continue
            resp = await orch.fetch(
                ProviderRequest(symbols=tuple(symbols), market=market.value)
            )
            if not resp.success:
                logger.error(f"价格提醒批量拉行情失败 {market.value}: {resp.error}")
                continue
            by_symbol = {str(r.get("symbol")): r for r in (resp.data or [])}
            for sym in symbols:
                q = by_symbol.get(sym)
                if q:
                    out[(market.value, sym)] = q
        return out

    async def _fetch_signals(
        self, market: MarketCode, symbol: str, interval: str
    ) -> list[dict]:
        """实时调用 evaluate API (SignalBarModel) 检测信号 K 线，DB 查询作为补充。"""
        # 1. 实时 evaluate — 根据市场选择对应的信号K线模型
        model_code = "pa_signal_bar_cn_v1" if market.value == "CN" else "pa_signal_bar_v1"
        ev = await self._evaluate_price_action_cached(
            market, symbol, interval, model_code=model_code
        )
        if ev.get("ok") and ev.get("signals"):
            return ev.get("signals") or []

        # 2. 兜底：从 pa_signal 表查最近 2 小时的历史信号
        from datetime import timedelta
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=2)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M")

        signals: list[dict] = []
        try:
            from src.web.database import SessionLocal
            from sqlalchemy import text
            with SessionLocal() as session:
                rows = session.execute(
                    text(
                        "SELECT signal_type, direction, score, reason, signal_date "
                        "FROM pa_signal "
                        "WHERE market = :m AND symbol = :s AND interval = :iv "
                        "AND signal_date > :cutoff "
                        "ORDER BY id DESC LIMIT 5"
                    ),
                    {"m": market.value, "s": symbol, "iv": interval, "cutoff": cutoff_str},
                ).fetchall()
                for r in rows:
                    sig_date = str(r[4] or "")
                    if sig_date:
                        signals.append({
                            "signal_type": r[0],
                            "direction": r[1],
                            "score": float(r[2]) if r[2] else 0,
                            "reason": r[3],
                            "signal_date": sig_date,
                        })
        except Exception as e:
            logger.debug("pa_signal query failed for %s/%s: %s", market.value, symbol, e)

        return signals

    async def _evaluate_price_action_cached(
        self, market: MarketCode, symbol: str, interval: str = "1d",
        *, model_code: str = ""
    ) -> dict:
        key = f"{market.value}:{symbol}:{interval}:{model_code}"
        now = time.monotonic()
        cached = self._engine_eval_cache.get(key)
        if cached and now - cached[0] < self.engine_eval_ttl_sec:
            return cached[1]
        result = await _evaluate_price_action(market, symbol, interval, model_code=model_code)
        self._engine_eval_cache[key] = (now, result or {})
        return result or {}

    async def _eval_condition(
        self,
        cond: dict,
        quote: dict,
        market: MarketCode,
        symbol: str,
    ) -> tuple[bool, dict]:
        ctype = str(_json_get(cond, "type", "")).strip()
        op = str(_json_get(cond, "op", "")).strip()
        value = _json_get(cond, "value")
        left: float | None = None

        if ctype == "price":
            left = _safe_float(quote.get("current_price"))
        elif ctype == "change_pct":
            left = _safe_float(quote.get("change_pct"))
        elif ctype == "turnover":
            left = _safe_float(quote.get("turnover"))
        elif ctype == "volume":
            left = _safe_float(quote.get("volume"))
        elif ctype == "volume_ratio":
            interval = str(_json_get(cond, "interval", "1d") or "1d")
            ev = await self._evaluate_price_action_cached(market, symbol, interval)
            if not ev.get("ok", False):
                return False, {
                    "type": ctype,
                    "error": ev.get("error") or "engine_unavailable",
                    "matched": False,
                }
            indicators = ev.get("indicators") or {}
            left = _safe_float(indicators.get("volume_ratio"))
        elif ctype == "ema20_position":
            interval = str(_json_get(cond, "interval", "1d") or "1d")
            ev = await self._evaluate_price_action_cached(market, symbol, interval)
            if not ev.get("ok", False):
                return False, {
                    "type": ctype,
                    "error": ev.get("error") or "engine_unavailable",
                    "matched": False,
                }
            indicators = ev.get("indicators") or {}
            left = _safe_float(indicators.get("ema20_position"))
        elif ctype == "pattern":
            interval = str(_json_get(cond, "interval", "1d") or "1d")
            # 信号 K 线直接查 pa_signal 表（由 Rust engine 实时写入）
            # 同时也跑 evaluate API 作为补充
            signals = await self._fetch_signals(market, symbol, interval)
            target = str(value or "").strip()
            matched_signal = None
            for sig in signals:
                if not isinstance(sig, dict):
                    continue
                sig_type = str(sig.get("signal_type") or "")
                if not target or sig_type == target:
                    matched_signal = sig
                    break
            ok = matched_signal is not None
            return ok, {
                "type": ctype,
                "op": op or "==",
                "target": target,
                "actual": matched_signal.get("signal_type") if matched_signal else None,
                "matched": ok,
                "signal": matched_signal,
            }
        elif ctype == "ema20_cross":
            interval = str(_json_get(cond, "interval", "1d") or "1d")
            direction = str(value or "any").strip()
            # 根据 market 选择 cn 或通用模型
            model_code = "pa_ema20_cross_cn_v1" if market.value == "CN" else "pa_ema20_cross_v1"
            ev = await self._evaluate_price_action_cached(
                market, symbol, interval, model_code=model_code
            )
            signals = ev.get("signals") or []
            matched_signal = None
            for sig in signals:
                if not isinstance(sig, dict):
                    continue
                sig_dir = str(sig.get("direction") or "")
                if direction == "any" or direction == "up":
                    if sig_dir == "long":
                        matched_signal = sig
                        break
                if direction == "any" or direction == "down":
                    if sig_dir == "short":
                        matched_signal = sig
                        break
            ok = matched_signal is not None
            return ok, {
                "type": ctype,
                "op": op or "==",
                "target": direction,
                "actual": matched_signal.get("direction") if matched_signal else None,
                "matched": ok,
                "signal": matched_signal,
            }
        else:
            return False, {"type": ctype, "error": "unsupported_type"}

        ok = _op_eval(left, op, value)
        return ok, {
            "type": ctype,
            "op": op,
            "target": value,
            "actual": left,
            "matched": ok,
        }

    async def eval_rule(self, rule: PriceAlertRule, quote: dict) -> RuleEvalResult:
        cond_group = rule.condition_group or {}
        op = str(cond_group.get("op", "and")).lower()
        items = cond_group.get("items") or []
        if not isinstance(items, list) or not items:
            return RuleEvalResult(matched=False, hits=[], snapshot={"error": "empty_items"})

        market = _to_market(rule.stock.market)
        symbol = rule.stock.symbol
        results: list[dict] = []
        bools: list[bool] = []
        for cond in items:
            if not isinstance(cond, dict):
                continue
            ok, detail = await self._eval_condition(cond, quote, market, symbol)
            results.append(detail)
            bools.append(ok)

        if not bools:
            matched = False
        elif op == "or":
            matched = any(bools)
        else:
            matched = all(bools)

        snapshot = {
            "symbol": symbol,
            "market": market.value,
            "quote": {
                "current_price": quote.get("current_price"),
                "change_pct": quote.get("change_pct"),
                "turnover": quote.get("turnover"),
                "volume": quote.get("volume"),
            },
            "conditions": results,
            "group_op": op,
        }
        return RuleEvalResult(matched=matched, hits=results, snapshot=snapshot)

    def _can_trigger(
        self, rule: PriceAlertRule, now: datetime, *, bypass_market_hours: bool = False
    ) -> tuple[bool, str]:
        if not rule.enabled:
            return False, "disabled"

        if rule.expire_at:
            exp = rule.expire_at
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if now > exp:
                return False, "expired"

        if rule.market_hours_mode == "trading_only" and not bypass_market_hours:
            if not _is_trading_time(_to_market(rule.stock.market)):
                return False, "non_trading"

        today = _day_key(now)
        if (rule.trigger_date or "") != today:
            rule.trigger_date = today
            rule.trigger_count_today = 0

        max_per_day = int(rule.max_triggers_per_day or 0)
        if max_per_day > 0 and int(rule.trigger_count_today or 0) >= max_per_day:
            return False, "daily_limit"

        if rule.repeat_mode == "once" and rule.last_trigger_at:
            return False, "once_triggered"

        if rule.last_trigger_at:
            last = rule.last_trigger_at
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            delta_sec = (now - last).total_seconds()
            cooldown = max(0, int(rule.cooldown_minutes or 0)) * 60
            if delta_sec < cooldown:
                return False, "cooldown"

        return True, "ok"

    def _resolve_channels(self, db: Session, rule: PriceAlertRule) -> list[NotifyChannel]:
        ids = rule.notify_channel_ids or []
        if ids:
            return (
                db.query(NotifyChannel)
                .filter(NotifyChannel.enabled == True, NotifyChannel.id.in_(ids))
                .all()
            )
        return (
            db.query(NotifyChannel)
            .filter(NotifyChannel.enabled == True, NotifyChannel.is_default == True)
            .all()
        )

    async def _send_notify(self, db: Session, rule: PriceAlertRule, snapshot: dict) -> tuple[bool, str]:
        channels = self._resolve_channels(db, rule)
        notifier = NotifierManager()
        for ch in channels:
            notifier.add_channel(ch.type, ch.config or {})

        symbol = rule.stock.symbol
        name = rule.stock.name or symbol
        quote = snapshot.get("quote") or {}
        price = _safe_float(quote.get("current_price"))
        chg = _safe_float(quote.get("change_pct"))
        title = f"【价格提醒】{name} ({symbol})"
        lines = [
            f"规则: {rule.name or f'提醒#{rule.id}'}",
            f"现价: {price:.2f}" if price is not None else "现价: --",
            f"涨跌幅: {chg:+.2f}%" if chg is not None else "涨跌幅: --",
        ]
        hit_lines = []
        for h in snapshot.get("conditions") or []:
            if h.get("matched"):
                ctype = h.get("type", "")
                if ctype == "ema20_cross":
                    sig = h.get("signal") or {}
                    dir_label = {"long": "上穿看多", "short": "下传看空"}.get(str(h.get("actual")), str(h.get("actual")))
                    evidence = sig.get("evidence") or {}
                    watch = evidence.get("watch_alert") or {}
                    hint = watch.get("alert_hint", "")
                    expected = watch.get("expected_use", "")
                    line = f"- EMA20{dir_label}"
                    if expected:
                        line += f"\n  {expected}"
                    hit_lines.append(line)
                elif ctype == "pattern":
                    sig = h.get("signal") or {}
                    dir_label = {"long": "看多", "short": "看空"}.get(str(sig.get("direction")), "")
                    reason = sig.get("reason", "")
                    line = f"- 信号K线 {dir_label}".strip()
                    if reason:
                        line += f"\n  {reason}"
                    hit_lines.append(line)
                else:
                    hit_lines.append(
                        f"- {ctype} {h.get('op')} {h.get('target')} (当前: {h.get('actual')})"
                    )
        if hit_lines:
            lines.append("命中条件:")
            lines.extend(hit_lines[:4])
        content = "\n".join(lines)

        try:
            result = await notifier.notify_with_result(title, content)
            if result.get("success"):
                return True, ""
            err = str(result.get("error") or result.get("skipped") or "notify_failed")
            return False, err
        except Exception as e:
            return False, str(e)

    async def scan_once(
        self,
        *,
        only_rule_id: int | None = None,
        dry_run: bool = False,
        bypass_market_hours: bool = False,
    ) -> dict:
        now = _utc_now()
        db = SessionLocal()
        try:
            query = db.query(PriceAlertRule).join(Stock).filter(PriceAlertRule.enabled == True)
            if only_rule_id:
                query = query.filter(PriceAlertRule.id == only_rule_id)
            rules = query.all()
            if not rules:
                return {"total_rules": 0, "triggered": 0, "skipped": 0, "items": []}

            stocks = [r.stock for r in rules if r.stock is not None]
            quote_map = await self._fetch_quotes_map(stocks)

            items: list[dict] = []
            triggered = 0
            skipped = 0

            for rule in rules:
                stock = rule.stock
                if not stock:
                    skipped += 1
                    items.append({"rule_id": rule.id, "status": "no_stock"})
                    continue
                market = _to_market(stock.market)
                quote = quote_map.get((market.value, stock.symbol))
                if not quote:
                    skipped += 1
                    items.append({"rule_id": rule.id, "status": "no_quote"})
                    continue

                can, reason = self._can_trigger(
                    rule, now, bypass_market_hours=bypass_market_hours
                )
                if not can:
                    skipped += 1
                    items.append({"rule_id": rule.id, "status": "gated", "reason": reason})
                    continue

                ev = await self.eval_rule(rule, quote)
                if not ev.matched:
                    skipped += 1
                    items.append({"rule_id": rule.id, "status": "not_matched"})
                    continue

                if dry_run:
                    triggered += 1
                    items.append(
                        {
                            "rule_id": rule.id,
                            "status": "would_trigger",
                            "snapshot": ev.snapshot,
                        }
                    )
                    continue

                bucket = _minute_bucket(now)
                hit = PriceAlertHit(
                    rule_id=rule.id,
                    stock_id=stock.id,
                    trigger_time=now,
                    trigger_bucket=bucket,
                    trigger_snapshot=ev.snapshot,
                )
                db.add(hit)
                try:
                    db.flush()
                except Exception:
                    db.rollback()
                    skipped += 1
                    items.append({"rule_id": rule.id, "status": "duplicated"})
                    continue

                notify_ok, notify_err = await self._send_notify(db, rule, ev.snapshot)
                hit.notify_success = bool(notify_ok)
                hit.notify_error = notify_err or ""

                rule.last_trigger_at = now
                rule.last_trigger_price = _safe_float(quote.get("current_price"))
                rule.trigger_count_today = int(rule.trigger_count_today or 0) + 1
                rule.trigger_date = _day_key(now)
                if rule.repeat_mode == "once":
                    rule.enabled = False

                db.commit()
                triggered += 1
                items.append(
                    {
                        "rule_id": rule.id,
                        "status": "triggered",
                        "notify_success": bool(notify_ok),
                        "notify_error": notify_err,
                    }
                )

            return {
                "total_rules": len(rules),
                "triggered": triggered,
                "skipped": skipped,
                "items": items,
                "scanned_at": now.isoformat(),
            }
        finally:
            db.close()


ENGINE = PriceAlertEngine()
