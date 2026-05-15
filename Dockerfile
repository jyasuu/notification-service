# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:1.78-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies first (layer-friendly).
# ALL workspace member Cargo.tomls must be present here so that
# `cargo build` can resolve the full workspace and actually compile
# (and cache) the dependency graph.  Missing any member causes the
# workspace resolver to fail, the build to exit via `|| true`, and
# zero dependencies to be cached — defeating the whole layer.
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml         crates/common/
COPY crates/store/Cargo.toml          crates/store/
COPY crates/mailer/Cargo.toml         crates/mailer/
COPY crates/consumer/Cargo.toml       crates/consumer/
COPY crates/api/Cargo.toml            crates/api/
COPY crates/outbox/Cargo.toml         crates/outbox/
COPY crates/rate_limiter/Cargo.toml   crates/rate_limiter/
COPY crates/recipient_filter/Cargo.toml crates/recipient_filter/
COPY crates/ns-cli/Cargo.toml         crates/ns-cli/

# Dummy source files so Cargo can compile (and cache) all dependencies
# without the real application source.  Removed before the real build.
RUN mkdir -p src \
    crates/common/src crates/store/src crates/mailer/src \
    crates/consumer/src crates/api/src crates/outbox/src \
    crates/rate_limiter/src crates/recipient_filter/src \
    crates/ns-cli/src && \
    echo "fn main(){}" > src/main.rs && \
    echo "fn main(){}" > crates/ns-cli/src/main.rs && \
    for c in common store mailer consumer api outbox rate_limiter recipient_filter; do \
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

# curl is required by the docker-compose healthcheck
# (test: curl -sf http://localhost:8080/ready || exit 1).
# Without it the container is never marked healthy and dependent
# services cannot start.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
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
