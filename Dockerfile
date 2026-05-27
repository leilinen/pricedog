# PanWatch Dockerfile
# 多阶段构建，减小最终镜像大小

# ===== Stage 1: 前端构建 =====
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

# 安装 pnpm
RUN npm install -g pnpm

# 复制依赖文件
COPY frontend/package.json frontend/pnpm-lock.yaml ./

# 安装依赖
RUN pnpm install --frozen-lockfile

# 复制源码并构建
COPY frontend/ ./
RUN pnpm build


# ===== Stage 2: Python 运行环境 =====
FROM python:3.11-slim

# 版本号（构建时传入）
ARG VERSION=dev
ARG INSTALL_SCREENSHOT=false
ARG INSTALL_TRADINGAGENTS=false

WORKDIR /app

# 安装系统依赖
# - tzdata: 时区数据（zoneinfo 模块需要）
# - 默认镜像保持轻量；截图/TradingAgents 相关依赖通过 build arg 按需安装。
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    ca-certificates \
    $(if [ "$INSTALL_TRADINGAGENTS" = "true" ]; then echo git; fi) \
    $(if [ "$INSTALL_SCREENSHOT" = "true" ]; then echo \
        fonts-noto-cjk \
        libxcursor1 \
        libgtk-3-0 \
        libpangocairo-1.0-0 \
        libcairo-gobject2 \
        libgdk-pixbuf-2.0-0 \
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libasound2 \
        libpango-1.0-0 \
        libcairo2 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxext6 \
        libxi6 \
        libxrender1 \
        libxss1 \
        libxtst6 \
        libxshmfence1 \
        libegl1 \
        libfontconfig1 \
        libglib2.0-0; fi) \
    && rm -rf /var/lib/apt/lists/* \
    && if command -v fc-cache >/dev/null 2>&1; then fc-cache -fv; fi

# 复制依赖文件
COPY requirements*.txt ./

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt \
    && if [ "$INSTALL_SCREENSHOT" = "true" ]; then pip install --no-cache-dir -r requirements-optional-screenshot.txt; fi \
    && if [ "$INSTALL_TRADINGAGENTS" = "true" ]; then pip install --no-cache-dir -r requirements-optional-tradingagents.txt; fi

# 注意: 默认跳过 Playwright 浏览器安装。需要截图功能时设置
# INSTALL_SCREENSHOT=true 构建，并在运行时覆盖 PLAYWRIGHT_SKIP_BROWSER_INSTALL=0。

# 复制后端代码
COPY src/ ./src/
COPY server.py ./
COPY prompts/ ./prompts/

# 写入版本号
RUN echo "${VERSION}" > VERSION

# 从前端构建阶段复制静态文件
COPY --from=frontend-builder /app/frontend/dist ./static/

# 创建数据目录
RUN mkdir -p /app/data

# 环境变量
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/app/data
ENV DOCKER=1
ENV PLAYWRIGHT_SKIP_BROWSER_INSTALL=1

# 默认时区（可在 docker run 时用 -e TZ=... 覆盖）
ENV TZ=Asia/Shanghai

# 暴露端口（保持 8000 不变，避免影响存量用户升级）
EXPOSE 8000

# 健康检查（使用 Python）
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

# 启动命令
CMD ["python", "server.py"]
