//! Standalone outbox-worker binary.
//!
//! Connects to the **business** database (read/write on the `outbox` table)
//! and the shared RabbitMQ broker.  It has NO connection to the
//! anvil-notify's own PostgreSQL database — that isolation is the
//! entire point of running this as a separate container.
//!
//! Configuration is via environment variables (AN__OUTBOX__ prefix):
//!
//!   AN__OUTBOX__DATABASE_URL          — business DB  (required)
//!   AN__OUTBOX__AMQP_URL              — RabbitMQ URL (required)
//!   AN__OUTBOX__EXCHANGE              — default: anvil-notify
//!   AN__OUTBOX__ROUTING_KEY           — default: email.requested
//!   AN__OUTBOX__POLL_INTERVAL_MS      — default: 1000
//!   AN__OUTBOX__BATCH_SIZE            — default: 50
//!   AN__OUTBOX__MAX_PUBLISH_FAILURES  — default: 5

use anyhow::Context;
use outbox::{run_outbox_worker, OutboxConfig};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Deserialize)]
struct OutboxEnv {
    database_url: String,
    amqp_url: String,
    #[serde(default = "default_exchange")]
    exchange: String,
    #[serde(default = "default_routing_key")]
    routing_key: String,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    batch_size: i64,
    /// Max connections in the outbox DB pool (AN__OUTBOX__POOL_SIZE, default: 2).
    #[serde(default = "default_pool_size")]
    pool_size: u32,
    /// Seconds before an IN_PROGRESS row is considered stuck and reset to
    /// PENDING by the reaper (AN__OUTBOX__STALE_LOCK_TIMEOUT_SECS, default: 300).
    /// Requires migration 0016_outbox_locked_at.sql to be applied first.
    #[serde(default = "default_stale_lock_timeout_secs")]
    stale_lock_timeout_secs: u64,
    /// Consecutive publish failures before a row is permanently marked FAILED
    /// (AN__OUTBOX__MAX_PUBLISH_FAILURES, default: 5).
    #[serde(default = "default_max_publish_failures")]
    max_publish_failures: i32,
}

fn default_exchange() -> String {
    "anvil-notify".into()
}
fn default_routing_key() -> String {
    "email.requested".into()
}
fn default_poll_interval_ms() -> u64 {
    1_000
}
fn default_batch_size() -> i64 {
    50
}
fn default_pool_size() -> u32 {
    2
}
fn default_stale_lock_timeout_secs() -> u64 {
    300
}
fn default_max_publish_failures() -> i32 {
    5
}

impl OutboxEnv {
    fn load() -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("AN__OUTBOX").separator("__"))
            .build()?;
        let env: Self = cfg.try_deserialize()?;
        if env.database_url.is_empty() {
            anyhow::bail!("AN__OUTBOX__DATABASE_URL must not be empty");
        }
        if env.amqp_url.is_empty() {
            anyhow::bail!("AN__OUTBOX__AMQP_URL must not be empty");
        }
        Ok(env)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Tracing ───────────────────────────────────────────────────────────────
    // LOG_FORMAT=json   → structured JSON (default in Docker / production)
    // LOG_FORMAT=pretty  → human-readable coloured output (local dev)
    // LOG_FORMAT=compact → human-readable, no colours (CI / plain terminals)
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "json".into());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let registry = tracing_subscriber::registry().with(filter);
    match log_format.to_lowercase().as_str() {
        "pretty" => registry
            .with(tracing_subscriber::fmt::layer().pretty())
            .init(),
        "compact" => registry
            .with(tracing_subscriber::fmt::layer().compact())
            .init(),
        _ => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }

    // ── Config ────────────────────────────────────────────────────────────────
    let env = OutboxEnv::load().context("Failed to load outbox worker config")?;
    info!(
        exchange    = %env.exchange,
        routing_key = %env.routing_key,
        poll_ms     = env.poll_interval_ms,
        batch_size  = env.batch_size,
        "Outbox worker config loaded"
    );

    let cfg = OutboxConfig {
        database_url: env.database_url,
        amqp_url: env.amqp_url,
        exchange: env.exchange,
        routing_key: env.routing_key,
        poll_interval_ms: env.poll_interval_ms,
        batch_size: env.batch_size,
        pool_size: env.pool_size,
        stale_lock_timeout_secs: env.stale_lock_timeout_secs,
        max_publish_failures: env.max_publish_failures,
    };

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    let shutdown = CancellationToken::new();

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).context("Failed to register SIGTERM handler")?;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("SIGINT received — shutting down outbox worker");
                }
                _ = sigterm.recv() => {
                    info!("SIGTERM received — shutting down outbox worker");
                }
            }
            shutdown_clone.cancel();
        });
    }
    #[cfg(not(unix))]
    {
        // On non-unix platforms SIGTERM is unavailable; only SIGINT (Ctrl-C) is
        // handled.  We spawn the signal waiter as a task so that
        // `run_outbox_worker` can drive the main loop, but we do NOT detach it
        // — if `run_outbox_worker` returns before the signal fires (e.g. in a
        // fast shutdown test), the task is dropped with the runtime and
        // `shutdown_clone.cancel()` is never called.  Since `run_outbox_worker`
        // observes the same `shutdown` token, this is harmless: the token is
        // already cancelled (or the process is about to exit).  The pattern
        // mirrors the unix branch where the signal task is also fire-and-forget.
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("SIGINT received — shutting down outbox worker");
            shutdown_clone.cancel();
        });
    }

    // ── Run ───────────────────────────────────────────────────────────────────
    info!("Outbox worker starting");
    run_outbox_worker(cfg, shutdown).await?;
    info!("Outbox worker stopped");

    Ok(())
}
