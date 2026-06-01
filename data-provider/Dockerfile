# PriceDog Data Provider Dockerfile

FROM rust:1-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY data-provider/Cargo.toml data-provider/Cargo.lock ./
COPY data-provider/src ./src

RUN cargo build --release


FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/data-provider /usr/local/bin/data-provider

ENV DP_PORT=8003
ENV DP_DATABASE_URL=postgres://postgres:postgres@postgres:5432/pricedog

EXPOSE 8003

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8003/health || exit 1

CMD ["data-provider"]
