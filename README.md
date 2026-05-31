# PriceDog

PriceDog 是一个基于 Price Action 裸 K 突破策略的行情监控和量化计算项目。项目 fork 自 PanWatch，保留其 Web UI、用户认证、通知、价格提醒和 AI Agent 基础能力，并新增 Rust 计算引擎负责行情数据、技术指标、突破识别和后续回测能力。

当前开发重点是 crypto 实时数据获取和 EMA20 突破量化模型验证。A 股和美股数据源会接入同一 Rust Engine，但 A 股分钟线稳定性暂不作为当前优先级。

## 架构

PriceDog 使用 Docker Compose 部署多个轻量服务：

| 服务 | 端口 | 职责 |
| --- | --- | --- |
| `panwatch` | `18000 -> 8000` | Web UI、用户认证、价格提醒、通知、AI Agent 编排 |
| `price-action-engine` | `8001` | Rust 计算引擎，负责行情获取、K 线缓存、EMA20/ATR、突破信号、信号入库 |
| `akshare-adapter` | 内部 `8002` | Python AkShare 适配器，提供股票数据 HTTP 接口 |
| `postgres` | `15432 -> 5432` | 公共 PostgreSQL 服务，PriceDog 使用数据库 `pricedog` |

设计原则：

- Rust Engine 是核心行情和计算服务。
- PanWatch Python 层尽量只做 Web、规则、通知和 AI 编排。
- PostgreSQL 是公共数据库服务，不绑定到单一应用；PriceDog 使用独立数据库名 `pricedog`。
- 不再使用 SQLite。
- 默认不安装 Playwright 浏览器，保持服务轻量。

## 当前能力

- Crypto 实时行情：OKX WebSocket ticker。
- Crypto 实时 K 线：OKX WebSocket candle，闭合 K 线写入 PostgreSQL。
- Crypto 历史 K 线：Binance REST 优先，OKX REST fallback。
- 股票数据：通过 `akshare-adapter` 获取，后续逐步完善。
- K 线周期：`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、`1d`。
- 指标：
  - EMA20，使用 close 序列计算。
  - ATR14。
  - `ema20_position = (close - ema20) / atr14`。
  - 量比：当前成交量 / 前 20 根平均成交量。
  - 振幅。
- 基础突破信号识别。
- 价格提醒条件支持 `ema20_position` 和 `pattern`。
- 数据源状态接口：查看 OKX WebSocket 连接状态、最近消息时间和最近闭合 K 线时间。

## 快速启动

复制环境变量模板：

```bash
cp .env.example .env
```

启动全部服务：

```bash
docker compose up --build
```

访问：

- Web UI: `http://127.0.0.1:18000`
- Rust Engine: `http://127.0.0.1:8001`
- PostgreSQL: `127.0.0.1:15432`

停止服务：

```bash
docker compose down
```

## 常用接口

健康检查：

```bash
curl http://127.0.0.1:8001/api/v1/health
```

数据源状态：

```bash
curl http://127.0.0.1:8001/api/v1/provider-status
```

K 线数据健康检查：

```bash
curl 'http://127.0.0.1:8001/api/v1/data-health/CRYPTO/BTCUSDT?interval=5m&limit=120'
```

实时行情：

```bash
curl http://127.0.0.1:8001/api/v1/quote/CRYPTO/BTCUSDT
```

K 线：

```bash
curl 'http://127.0.0.1:8001/api/v1/klines/CRYPTO/BTCUSDT?interval=5m&limit=120'
```

单标的评估：

```bash
curl -X POST http://127.0.0.1:8001/api/v1/evaluate \
  -H 'Content-Type: application/json' \
  -d '{"market":"CRYPTO","symbol":"BTCUSDT","interval":"5m"}'
```

批量扫描：

```bash
curl -X POST http://127.0.0.1:8001/api/v1/scan \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"market":"CRYPTO","symbol":"BTCUSDT"},{"market":"CRYPTO","symbol":"ETHUSDT"}],"interval":"5m","limit":120}'
```

独立抓取回测数据：

```bash
python scripts/fetch_backtest_data.py \
  --symbol BTCUSDT \
  --start 2024-01-01T00:00:00Z \
  --end 2024-03-01T00:00:00Z \
  --interval 1h \
  --output /tmp/btcusdt-1h.json
```

脚本特性：

- 与服务解耦，不依赖 `price-action-engine` 运行。
- `CRYPTO` 直接走交易所 REST 分页抓全量历史 K 线，Binance 优先，OKX fallback。
- `CN`、`US`、`HK` 走 `akshare-adapter` HTTP 接口。
- 输出 JSON 可直接作为后续回测输入；也支持 `--format csv`。

统一回测数据规范：

- `schema_version = "v1"`
- `timezone = "UTC"`
- `ts_mode = "bar_open"`，表示 `klines[].ts` 是这根 K 线的开始时间
- `quality_mode = "continuous_24_7"` 表示按全天候连续交易检查，当前用于 crypto
- `quality_mode = "best_effort"` 表示当前数据质量检查不含交易日历，当前用于股票
- `interval` 表示这根 K 线的长度
- 回测在处理某根 K 线时，应把该 K 线视为在 `ts + interval` 时刻才完整可用
- `quality` 描述数据完整性，crypto 按 24/7 连续交易检查；股票在接入交易日历前只作为粗略参考

标准 JSON 结构：

```json
{
  "schema_version": "v1",
  "market": "CRYPTO",
  "symbol": "BTCUSDT",
  "interval": "1h",
  "source": "binance",
  "timezone": "UTC",
  "ts_mode": "bar_open",
  "quality_mode": "continuous_24_7",
  "start": "2024-01-01T00:00:00Z",
  "end": "2024-03-01T00:00:00Z",
  "count": 2,
  "quality": {
    "expected_bars": 2,
    "actual_bars": 2,
    "missing_bars": 0,
    "duplicate_bars": 0,
    "is_continuous": true,
    "first_ts": "2024-01-01T00:00:00Z",
    "last_ts": "2024-01-01T01:00:00Z",
    "missing_timestamps_sample": []
  },
  "klines": [
    {
      "ts": "2024-01-01T00:00:00Z",
      "open": 42000.0,
      "high": 42500.0,
      "low": 41800.0,
      "close": 42300.0,
      "volume": 100.0,
      "turnover": 4215000.0
    }
  ]
}
```

把抓取结果直接喂给回测接口：

```bash
curl -X POST http://127.0.0.1:8001/api/v1/backtest \
  -H 'Content-Type: application/json' \
  --data @/tmp/btcusdt-1h.json
```

说明：

- 回测接口会直接读取 JSON 里的 `market`、`symbol`、`interval`、`klines`。
- 其他字段如 `schema_version`、`source`、`timezone`、`ts_mode`、`quality_mode`、`start`、`end`、`count`、`quality` 会被忽略，不影响回测。
- 如果要覆盖默认回测参数，可以在 JSON 里额外加入 `max_holding_bars`、`fee_bps`、`slippage_bps`、`persist`。

## 环境变量

核心配置见 `.env.example`。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PANWATCH_PORT` | `18000` | Web UI 映射端口 |
| `PRICE_ACTION_ENGINE_URL` | `http://127.0.0.1:8001` | Python 层访问 Rust Engine 的地址 |
| `PA_ENGINE_PORT` | `8001` | Rust Engine 监听端口 |
| `AKSHARE_ADAPTER_URL` | `http://127.0.0.1:8002` | 本地开发时 AkShare adapter 地址 |
| `PA_CRYPTO_WS_ENABLED` | `true` | 是否启用 crypto WebSocket 采集 |
| `PA_CRYPTO_WS_SYMBOLS` | `BTCUSDT,ETHUSDT` | WebSocket 订阅标的 |
| `PA_CRYPTO_WS_INTERVALS` | `5m,1h,4h` | WebSocket 订阅 K 线周期 |
| `PA_CRYPTO_BACKFILL_ENABLED` | `true` | 是否启用 crypto K 线 REST 自动回补 |
| `PA_CRYPTO_BACKFILL_INTERVAL_SEC` | `60` | 回补检查间隔，最低建议 60 秒 |
| `PA_CRYPTO_BACKFILL_LOOKBACK` | `120` | 每次回补检查的近端 K 线窗口 |
| `PANWATCH_DATABASE_URL` | `postgresql+psycopg://postgres:postgres@postgres:5432/pricedog` | PanWatch Python 使用的数据库连接 |
| `PA_DATABASE_URL` | `postgres://postgres:postgres@postgres:5432/pricedog` | Rust Engine 使用的数据库连接 |
| `POSTGRES_DB` | `pricedog` | PriceDog 数据库名 |
| `POSTGRES_PORT` | `15432` | PostgreSQL 宿主机映射端口 |
| `INSTALL_SCREENSHOT` | `false` | 是否安装截图相关依赖 |
| `INSTALL_TRADINGAGENTS` | `false` | 是否安装 TradingAgents 深度分析依赖 |
| `PLAYWRIGHT_SKIP_BROWSER_INSTALL` | `1` | 默认跳过浏览器安装 |

## 本地开发

Python 后端：

```bash
make setup-backend
make dev-api
```

Rust Engine：

```bash
make dev-engine
```

前端：

```bash
make dev-web
```

Docker Compose：

```bash
make compose-up
make compose-down
```

## 测试

Rust Engine：

```bash
cd price-action-engine
cargo fmt -- --check
cargo test
```

Python：

```bash
make test
```

## 数据库

PostgreSQL 由 Compose 独立启动：

```text
postgres://postgres:postgres@127.0.0.1:15432/pricedog
```

Rust Engine 当前会初始化 PriceDog 自己需要的表：

- `pa_kline`
- `pa_signal`

项目按“新项目直接 PostgreSQL 初始化”的方式开发，不保留 SQLite 迁移路径。

## 开发路线

Phase 1：

- 完善 crypto WebSocket 采集。
- 验证 EMA20 + ATR + 量价突破模型。
- 完善数据源状态、缓存、异常降级。
- 让价格提醒稳定接入 Rust Engine 指标。

Phase 2：

- 形态识别。
- 更完整的 Price Action 场景分类。
- Telegram 信号推送。

Phase 3：

- 回测引擎。
- 绩效统计。
- 前端回测页面。

## 代码仓库

```text
git@github.com:leilinen/pricedog.git
```

## License

MIT
