# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:1.78-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies first (layer-friendly)
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml   crates/common/
COPY crates/store/Cargo.toml    crates/store/
COPY crates/mailer/Cargo.toml   crates/mailer/
COPY crates/consumer/Cargo.toml crates/consumer/
COPY crates/api/Cargo.toml      crates/api/

# Dummy source so Cargo can resolve the workspace
RUN mkdir -p src \
    crates/common/src crates/store/src crates/mailer/src \
    crates/consumer/src crates/api/src && \
    echo "fn main(){}" > src/main.rs && \
    for c in common store mailer consumer api; do \
      echo "" > crates/$c/src/lib.rs; \
    done && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src crates/*/src

# Now copy real source and build
COPY src/           src/
COPY crates/        crates/
COPY migrations/    migrations/
COPY config/        config/

RUN cargo build --release

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/notification-service ./notification-service
COPY --from=builder /app/migrations  ./migrations
COPY --from=builder /app/config      ./config

# Drop privileges
RUN useradd -m -u 1001 appuser
USER appuser

EXPOSE 8080

ENTRYPOINT ["./notification-service"]
