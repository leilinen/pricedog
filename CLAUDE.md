# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is PriceDog

PriceDog (fork from PanWatch/盯盘侠) is a self-hosted AI stock/crypto assistant with real-time market monitoring, technical analysis, price alerts, and paper trading. A Rust engine handles real-time data ingestion, indicator calculation, breakout signal detection, and backtesting. The Python layer handles Web UI, auth, notifications, AI agent orchestration, and price alerts. Supports A-shares (CN), Hong Kong (HK), US, and Crypto markets.

**Current development focus**: Crypto real-time data (OKX WebSocket), EMA20 breakout quantitative model validation, and backtesting.

## Build & Run Commands

```bash
# Docker Compose (full stack)
make compose-up                     # Start all services (postgres, rust engine, python, frontend)
make compose-down                   # Stop all services
docker compose up --build           # Build and start

# Python backend
make setup-backend                  # Create venv + install deps
make dev-api                        # venv + deps + uvicorn --reload (port 8000)

# Rust engine
make dev-engine                     # cargo run (port 8001)

# Frontend
make dev-web                        # pnpm install + dev (port 5183)
cd frontend && pnpm build           # Production build → frontend/dist

# Tests
make test                           # Python tests (default: no notifications)
python -m pytest tests/ -v          # Python tests directly
python -m pytest tests/ -v --notify # Python tests with real notifications
python -m pytest tests/test_foo.py -v  # Single test file
cd price-action-engine && cargo test     # Rust engine tests
cd price-action-engine && cargo fmt -- --check  # Rust format check

# Build
./build.sh <version>                # Build frontend + Docker image

# Git Hooks
bash scripts/install-hooks.sh       # Install pre-push hook (runs tests before push)
```

## Architecture

### Service Topology (Docker Compose)

| Service | Port | Role |
|---------|------|------|
| `panwatch` | 18000→8000 | Python web app, serves frontend static + API |
| `price-action-engine` | 8001 | Rust engine: market data, indicators, signals, backtest |
| `akshare-adapter` | 8002 (internal) | Python adapter for stock data (akshare) |
| `postgres` | 15432→5432 | Shared PostgreSQL, database name: `pricedog` |

Design principles: Rust Engine is the core calculation service. Python layer does Web/rules/notifications/AI only. PostgreSQL is a shared service, not app-specific. No SQLite.

### Backend (Python / FastAPI)

- **`server.py`** — Entrypoint. Initializes DB, registers agents/data sources, starts schedulers (APScheduler), mounts FastAPI app.
- **`src/config.py`** — `Settings` (pydantic-settings, from `.env`), `AppConfig`, `StockConfig` (from YAML).
- **`src/web/`** — FastAPI app with JWT auth (`HTTPBearer`). API routes in `src/web/api/`. Models in `src/web/models.py`. PostgreSQL via SQLAlchemy (`src/web/database.py`).
- **`src/agents/`** — Business logic agents (premarket_outlook, daily_report, intraday_monitor, news_digest, chart_analyst). Each registered in `server.py` `AGENT_REGISTRY`.
- **`src/collectors/`** — Stateless data collectors (quotes, kline, news). Use `efinance`/`akshare` for CN market data.
- **`src/core/`** — Core utilities:
  - `ai_client.py` — OpenAI-compatible API client
  - `notifier.py` / `notify_dedupe.py` / `notify_policy.py` — Notification system (Telegram, WeChat Work, DingTalk, Bark, Webhook via Apprise)
  - `scheduler.py` — Agent scheduling
  - `paper_trading_engine.py` / `paper_trading_notifier.py` / `paper_trading_scheduler.py` — Paper trading system
  - `price_alert_engine.py` — Price alert engine, calls Rust engine HTTP API for quote/evaluate
  - `signals/` — Strategy signal generation (trend_follow, macd_golden, momentum, etc.)
  - `stock_link.py` — Shared utility for generating stock URLs (global `stock_link_platform` setting)
- **`prompts/`** — Prompt templates used by agents (one file per agent).

### Rust Engine (`price-action-engine/`)

Single-file architecture in `src/main.rs` (~2600 lines). Key dependencies: Axum, tokio-postgres, tokio-tungstenite, moka (caching).

**Data flow**: OKX WebSocket → parse → moka cache → PostgreSQL upsert → signal evaluation

**API endpoints** (all under `/api/v1/`):
- `GET /health` — Health check
- `GET /provider-status` — WebSocket connection status
- `GET /data-health/:market/:symbol` — K-line data completeness check
- `GET /klines/:market/:symbol` — Historical K-lines (`?interval=5m&limit=120`)
- `GET /quote/:market/:symbol` — Current quote
- `POST /evaluate` — Evaluate single symbol (`{market, symbol, interval}`)
- `POST /scan` — Batch scan multiple symbols
- `POST /backtest` — Run backtest (accepts JSON from `fetch_backtest_data.py`)

**Crypto WebSocket**: Subscribes to OKX ticker + candle channels. Supports configurable symbols (`PA_CRYPTO_WS_SYMBOLS`) and intervals (`PA_CRYPTO_WS_INTERVALS`). Includes automatic REST backfill for data gaps (`PA_CRYPTO_BACKFILL_*`).

**Indicators calculated**: EMA20 (on close series), ATR14, `ema20_position = (close - ema20) / atr14`, volume ratio (current / 20-bar avg), amplitude.

### Python ↔ Rust Engine Communication

Python calls Rust engine via HTTP using `PRICE_ACTION_ENGINE_URL` (default `http://127.0.0.1:8001`):
- `price_alert_engine.py` → `GET /api/v1/quote` and `POST /api/v1/evaluate` for price alert conditions
- Both share the same PostgreSQL database — Rust writes `pa_signal`, Python reads it via SQLAlchemy

### Frontend (React + TypeScript + Vite)

- **Monorepo** with pnpm workspaces under `frontend/packages/`:
  - `packages/api/` — API client functions and TypeScript interfaces
  - `packages/base-ui/` — shadcn/ui component library (Radix UI + Tailwind)
  - `packages/biz-ui/` — Business components
- **Pages** in `frontend/src/pages/` — Dashboard, PaperTrading, Settings, etc.
- **Styling** — Tailwind CSS with shadcn/ui components

### API Response Format

All API responses wrapped: `{ code: number, data: T, message: string }`. Frontend `fetchAPI` unwraps automatically.

### Key Patterns

- **ORM serialization before session close**: In `paper_trading_engine.py`, ORM objects are serialized to plain dicts via `_serialize_position()` / `_serialize_trade()` / `_serialize_signal()` before `db.close()`, to avoid detached instance errors.
- **Global settings**: Stored in `AppSettings` table (key-value). Settings API at `/api/settings`.
- **Notification channels**: Configured in UI, stored in `NotifyChannel` table. Multiple channel types via Apprise URIs.
- **Strategy signals**: Generated by `src/core/signals/`, stored as `StrategySignalRun`. Paper trading engine consumes these.
- **PriceDog tables**: Rust engine auto-creates `pa_kline`, `pa_quote`, `pa_signal`, `pa_backtest_runs` on startup. No migration framework — "new project, direct PostgreSQL init" approach.
- **Backtest data format**: `fetch_backtest_data.py` outputs standardized JSON (`schema_version: "v1"`, `ts_mode: "bar_open"`, `timezone: "UTC"`). Crypto uses Binance REST (OKX fallback). Stocks use akshare-adapter.

## Coding Conventions

- Python: PEP 8, type hints for new code, `snake_case` files/functions, `PascalCase` classes
- Rust: standard Rust conventions, `anyhow` for errors, `tracing` for logging, async/await throughout
- TypeScript: `PascalCase.tsx` components, `use-` prefix hooks, `camelCase.ts` utilities
- Commits: `<type>: <subject>` where type ∈ {feat, fix, docs, refactor, style, test, chore}
- All user-facing text is in Chinese (zh-CN)
- Strategy names have a Chinese mapping in `STRATEGY_NAME_MAP` (paper_trading_notifier.py)

## Testing

- **Python**: `pyproject.toml` defines pytest config, `tests/conftest.py` provides shared fixtures
- **通知屏蔽**: Default: no notifications sent (monkeypatch NotifierManager). Pass `--notify` to enable real sends
- **Rust**: `cd price-action-engine && cargo test`
- **pre-push hook**: `scripts/pre-push` runs tests before push. Install with `bash scripts/install-hooks.sh`
- **CI**: `.github/workflows/release.yml` runs tests before Docker build
- **中文描述**: Every test function must have a Chinese docstring (first line). `conftest.py` `pytest_itemcollected` hook makes `pytest -v` display Chinese descriptions

## Environment Variables

Configured via `.env` (see `.env.example`). Key variables:

**Core**: `AUTH_USERNAME`, `AUTH_PASSWORD`, `JWT_SECRET`, `DATA_DIR`
**AI**: `AI_API_KEY`, `AI_BASE_URL`, `AI_MODEL`
**Network**: `HTTP_PROXY`
**Rust Engine**: `PRICE_ACTION_ENGINE_URL` (Python→Rust), `PA_ENGINE_PORT`, `PA_DATABASE_URL`
**Crypto WebSocket**: `PA_CRYPTO_WS_ENABLED`, `PA_CRYPTO_WS_SYMBOLS`, `PA_CRYPTO_WS_INTERVALS`
**Crypto Backfill**: `PA_CRYPTO_BACKFILL_ENABLED`, `PA_CRYPTO_BACKFILL_INTERVAL_SEC`, `PA_CRYPTO_BACKFILL_LOOKBACK`
**Database**: `PANWATCH_DATABASE_URL` (Python), `PA_DATABASE_URL` (Rust), `POSTGRES_DB`, `POSTGRES_PORT`
**Python**: `INSTALL_SCREENSHOT`, `INSTALL_TRADINGAGENTS`, `PLAYWRIGHT_SKIP_BROWSER_INSTALL`
