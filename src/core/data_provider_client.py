"""data-provider HTTP client — calls the unified Rust data-provider service."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _base_url() -> str:
    return (
        os.environ.get("DATA_PROVIDER_URL") or "http://127.0.0.1:8003"
    ).rstrip("/")


class DataProviderClient:
    """Thin client for the data-provider Rust service. Supports both async and sync."""

    def __init__(self, base_url: str | None = None, timeout: float = 10.0):
        self._base = base_url or _base_url()
        self._timeout = timeout

    # ── Internal: async ────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(body.get("message", "unknown error"))
        return body.get("data", {})

    async def _post(self, path: str, json_data: dict) -> dict:
        url = f"{self._base}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(url, json=json_data)
            resp.raise_for_status()
            body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(body.get("message", "unknown error"))
        return body.get("data")

    # ── Internal: sync ─────────────────────────────────────

    def _sync_get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(body.get("message", "unknown error"))
        return body.get("data", {})

    def _sync_post(self, path: str, json_data: dict) -> dict:
        url = f"{self._base}{path}"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(url, json=json_data)
            resp.raise_for_status()
            body = resp.json()
        if body.get("code") != 0:
            raise RuntimeError(body.get("message", "unknown error"))
        return body.get("data")

    # ── Quotes ─────────────────────────────────────────────

    async def get_quote(self, market: str, symbol: str) -> dict | None:
        """Fetch a single quote."""
        try:
            return await self._get(f"/api/v1/quote/{market}/{symbol}")
        except Exception as e:
            logger.warning("quote fetch failed %s/%s: %s", market, symbol, e)
            return None

    async def batch_quotes(self, items: list[dict[str, str]]) -> list[dict]:
        """Batch fetch quotes. items = [{"symbol": "...", "market": "..."}, ...]"""
        try:
            data = await self._post("/api/v1/quotes/batch", {"items": items})
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("batch quotes failed: %s", e)
            return []

    # ── K-lines ─────────────────────────────────────────────

    async def get_klines(
        self, market: str, symbol: str, interval: str = "1d", limit: int = 60
    ) -> list[dict]:
        """Fetch K-line data."""
        try:
            data = await self._get(
                f"/api/v1/klines/{market}/{symbol}",
                params={"interval": interval, "limit": limit},
            )
            return data.get("klines", [])
        except Exception as e:
            logger.warning("kline fetch failed %s/%s: %s", market, symbol, e)
            return []

    # ── News ─────────────────────────────────────────────────

    async def get_news(
        self, symbols: list[str], hours: int = 24, limit: int = 50
    ) -> list[dict]:
        """Fetch news/announcements."""
        try:
            data = await self._get(
                "/api/v1/news",
                params={"symbols": ",".join(symbols), "hours": hours, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("news fetch failed: %s", e)
            return []

    # ── Events ───────────────────────────────────────────────

    async def get_events(
        self, symbols: list[str], days: int = 7, limit: int = 50
    ) -> list[dict]:
        """Fetch corporate events."""
        try:
            data = await self._get(
                "/api/v1/events",
                params={"symbols": ",".join(symbols), "days": days, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("events fetch failed: %s", e)
            return []

    # ── Capital Flow ─────────────────────────────────────────

    async def get_capital_flow(self, market: str, symbol: str) -> dict | None:
        """Fetch capital flow data."""
        try:
            return await self._get(f"/api/v1/capital-flow/{market}/{symbol}")
        except Exception as e:
            logger.warning("capital flow fetch failed %s/%s: %s", market, symbol, e)
            return None

    # ── Discovery ────────────────────────────────────────────

    async def get_hot_stocks(
        self, market: str = "CN", mode: str = "turnover", limit: int = 20
    ) -> list[dict]:
        """Fetch hot stocks ranking."""
        try:
            data = await self._get(
                "/api/v1/discovery/stocks",
                params={"market": market, "mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("hot stocks fetch failed: %s", e)
            return []

    async def get_hot_boards(
        self, mode: str = "gainers", limit: int = 12
    ) -> list[dict]:
        """Fetch hot industry boards."""
        try:
            data = await self._get(
                "/api/v1/discovery/boards",
                params={"mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("hot boards fetch failed: %s", e)
            return []

    async def get_board_stocks(
        self, board_code: str, mode: str = "gainers", limit: int = 20
    ) -> list[dict]:
        """Fetch stocks within a board."""
        try:
            data = await self._get(
                f"/api/v1/discovery/boards/{board_code}/stocks",
                params={"mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("board stocks fetch failed: %s", e)
            return []

    # ── Sync wrappers ──────────────────────────────────────

    def sync_get_quote(self, market: str, symbol: str) -> dict | None:
        try:
            return self._sync_get(f"/api/v1/quote/{market}/{symbol}")
        except Exception as e:
            logger.warning("quote fetch failed %s/%s: %s", market, symbol, e)
            return None

    def sync_batch_quotes(self, items: list[dict[str, str]]) -> list[dict]:
        try:
            data = self._sync_post("/api/v1/quotes/batch", {"items": items})
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("batch quotes failed: %s", e)
            return []

    def sync_get_klines(
        self, market: str, symbol: str, interval: str = "1d", limit: int = 60
    ) -> list[dict]:
        try:
            data = self._sync_get(
                f"/api/v1/klines/{market}/{symbol}",
                params={"interval": interval, "limit": limit},
            )
            return data.get("klines", [])
        except Exception as e:
            logger.warning("kline fetch failed %s/%s: %s", market, symbol, e)
            return []

    def sync_get_capital_flow(self, market: str, symbol: str) -> dict | None:
        try:
            return self._sync_get(f"/api/v1/capital-flow/{market}/{symbol}")
        except Exception as e:
            logger.warning("capital flow fetch failed %s/%s: %s", market, symbol, e)
            return None

    def sync_get_events(
        self, symbols: list[str], days: int = 7, limit: int = 50
    ) -> list[dict]:
        try:
            data = self._sync_get(
                "/api/v1/events",
                params={"symbols": ",".join(symbols), "days": days, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("events fetch failed: %s", e)
            return []

    def sync_get_hot_stocks(
        self, market: str = "CN", mode: str = "turnover", limit: int = 20
    ) -> list[dict]:
        try:
            data = self._sync_get(
                "/api/v1/discovery/stocks",
                params={"market": market, "mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("hot stocks fetch failed: %s", e)
            return []

    def sync_get_hot_boards(self, mode: str = "gainers", limit: int = 12) -> list[dict]:
        try:
            data = self._sync_get(
                "/api/v1/discovery/boards",
                params={"mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("hot boards fetch failed: %s", e)
            return []

    def sync_get_board_stocks(
        self, board_code: str, mode: str = "gainers", limit: int = 20
    ) -> list[dict]:
        try:
            data = self._sync_get(
                f"/api/v1/discovery/boards/{board_code}/stocks",
                params={"mode": mode, "limit": limit},
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("board stocks fetch failed: %s", e)
            return []


# Singleton
_client: DataProviderClient | None = None


def get_data_provider() -> DataProviderClient:
    global _client
    if _client is None:
        _client = DataProviderClient()
    return _client


def _dp_klines_to_kline_data(klines: list[dict]) -> list:
    """Convert data-provider kline dicts to KlineData objects for compatibility."""
    from src.collectors.kline_collector import KlineData

    result = []
    for k in klines:
        ts = k.get("ts", "")
        # Extract date part from ISO timestamp (e.g. "2024-01-01T00:00:00Z" -> "2024-01-01")
        date = ts[:10] if ts else ""
        result.append(KlineData(
            date=date,
            open=float(k.get("open", 0)),
            close=float(k.get("close", 0)),
            high=float(k.get("high", 0)),
            low=float(k.get("low", 0)),
            volume=float(k.get("volume", 0)),
        ))
    return result
