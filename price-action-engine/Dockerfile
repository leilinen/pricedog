# PriceDog Price Action Engine Dockerfile

FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# Use mirrors for faster crate downloads
RUN mkdir -p /usr/local/cargo && \
    echo '[source.crates-io]\nreplace-with = "ustc"\n[source.ustc]\nregistry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"' > /usr/local/cargo/config.toml

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY price-action-engine/Cargo.toml price-action-engine/Cargo.lock ./
COPY price-action-engine/src ./src

RUN cargo build --release


FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/price-action-engine /usr/local/bin/price-action-engine

ENV PA_ENGINE_PORT=8001
ENV DATABASE_URL=postgres://postgres:postgres@postgres:5432/pricedog
ENV DATA_PROVIDER_URL=http://data-provider:8003

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/price-action-engine --healthcheck || exit 1

CMD ["price-action-engine"]
