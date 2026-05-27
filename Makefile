.PHONY: help setup-backend dev-api dev-web dev-engine compose-up compose-down build test test-notify install-hooks clean-venv

# 端口约定：
#   - 后端：:8000（Docker / 本地 dev 统一，避免存量用户升级困惑）
#   - 前端：:5183（与 BeeCount-Cloud 的 :5173 错开避免冲突）

help:
	@echo "PanWatch 开发命令:"
	@echo "  make setup-backend   创建 venv 并安装后端依赖"
	@echo "  make dev-api         启动后端（:8000，自动 setup-backend）"
	@echo "  make dev-engine      启动 Rust Price Action Engine（:8001）"
	@echo "  make dev-web         启动前端（:5183，自动 pnpm install）"
	@echo "  make compose-up      使用 docker compose 启动 PanWatch + Rust Engine"
	@echo "  make compose-down    停止 docker compose 服务"
	@echo "  make test            跑全部单测（默认不发通知）"
	@echo "  make test-notify     跑全部单测（实际发送通知）"
	@echo "  make build VERSION=x 构建前端 + Docker 镜像"
	@echo "  make install-hooks   安装 git pre-push hook"
	@echo "  make clean-venv      删除本地 venv"

setup-backend:
	@if [ ! -d .venv ]; then \
		echo ">>> 创建 venv"; \
		python3 -m venv .venv; \
	fi
	@. .venv/bin/activate && pip install -q -r requirements.txt
	@if [ ! -f .env ] && [ -f .env.example ]; then cp .env.example .env; fi

# server.py 内部已经用 uvicorn.run(host=0.0.0.0, port=8000, reload=True) 启动。
dev-api: setup-backend
	. .venv/bin/activate && python server.py

dev-engine:
	cd price-action-engine && DATABASE_URL=$${PA_DATABASE_URL:-postgres://postgres:postgres@localhost:15432/pricedog} PA_ENGINE_PORT=8001 AKSHARE_ADAPTER_URL=$${AKSHARE_ADAPTER_URL:-http://127.0.0.1:8002} cargo run

dev-web:
	@if ! command -v pnpm >/dev/null 2>&1; then \
		echo "pnpm 未安装，请先 npm install -g pnpm"; \
		exit 1; \
	fi
	cd frontend && pnpm install --no-frozen-lockfile && pnpm dev

test:
	. .venv/bin/activate && python -m pytest tests/ -v

test-notify:
	. .venv/bin/activate && python -m pytest tests/ -v --notify

compose-up:
	docker compose up --build

compose-down:
	docker compose down

# 用法: make build VERSION=0.3.0
build:
	@if [ -z "$(VERSION)" ]; then \
		echo "Usage: make build VERSION=<version>"; \
		exit 1; \
	fi
	./build.sh $(VERSION)

install-hooks:
	bash scripts/install-hooks.sh

clean-venv:
	rm -rf .venv
