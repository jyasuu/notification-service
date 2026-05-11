use std::time::Duration;

use anyhow::Context;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use metrics;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::OutboxConfig;

// ── Schema ────────────────────────────────────────────────────────────────────
//
// The business service must have a table with AT LEAST these columns:
//
//   CREATE TABLE outbox (
//       id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
//       event_id     UUID        NOT NULL UNIQUE,
//       event_type   TEXT        NOT NULL,
//       payload      JSONB       NOT NULL,
//       status       TEXT        NOT NULL DEFAULT 'PENDING'
//                                CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
//       fail_count   INT         NOT NULL DEFAULT 0,
//       created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
//       published_at TIMESTAMPTZ
//   );
//
// See `migrations/0002_create_outbox.sql` for the full definition.

/// Maximum consecutive publish failures before a row is permanently marked FAILED.
const MAX_PUBLISH_FAILURES: i32 = 5;

// ── Public entry point ────────────────────────────────────────────────────────

/// Poll the business outbox table and publish pending events to RabbitMQ.
///
/// Reconnects to both Postgres and RabbitMQ on failure.
/// Exits cleanly when `shutdown` is cancelled.
pub async fn run_outbox_worker(
    cfg: OutboxConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.database_url)
        .await
        .context("Outbox worker: failed to connect to business DB")?;

    info!("Outbox worker connected to business DB");

    let mut reconnect_delay = Duration::from_secs(2);

    loop {
        if shutdown.is_cancelled() {
            info!("Outbox worker: shutdown requested");
            return Ok(());
        }

        match connect_amqp_and_poll(&cfg, &pool, shutdown.clone()).await {
            Ok(()) => {
                info!("Outbox worker: exiting cleanly");
                return Ok(());
            }
            Err(e) if shutdown.is_cancelled() => {
                info!(error = %e, "Outbox worker: exited after shutdown");
                return Ok(());
            }
            Err(e) => {
                error!(
                    error = %e,
                    delay_secs = reconnect_delay.as_secs(),
                    "Outbox worker error — reconnecting"
                );
                tokio::select! {
                    _ = sleep(reconnect_delay) => {}
                    _ = shutdown.cancelled()   => return Ok(()),
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

// ── One AMQP connection lifetime ──────────────────────────────────────────────

async fn connect_amqp_and_poll(
    cfg: &OutboxConfig,
    pool: &PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let conn = Connection::connect(&cfg.amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            &cfg.exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    info!(exchange = %cfg.exchange, "Outbox worker AMQP ready");
    // Reset backoff on successful connect.

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Outbox worker: shutdown — stopping poll loop");
                return Ok(());
            }
            _ = poll_once(cfg, pool, &channel) => {}
        }

        // Wait before next poll cycle.
        tokio::select! {
            _ = sleep(Duration::from_millis(cfg.poll_interval_ms)) => {}
            _ = shutdown.cancelled() => return Ok(()),
        }
    }
}

// ── Single poll cycle ─────────────────────────────────────────────────────────

async fn poll_once(cfg: &OutboxConfig, pool: &PgPool, channel: &Channel) {
    match fetch_pending_batch(pool, cfg.batch_size).await {
        Ok(rows) if rows.is_empty() => {
            // Nothing to do this cycle.
        }
        Ok(rows) => {
            let count = rows.len();
            info!(count, "Outbox: processing batch");
            for row in rows {
                match publish_and_mark(pool, channel, cfg, &row).await {
                    Ok(()) => {
                        metrics::counter!("outbox_published_total").increment(1);
                    }
                    Err(e) => {
                        error!(event_id = %row.event_id, error = %e, "Failed to publish outbox row");
                        metrics::counter!("outbox_publish_failed_total").increment(1);
                        if let Err(e2) =
                            record_publish_failure(pool, row.id, MAX_PUBLISH_FAILURES).await
                        {
                            error!(event_id = %row.event_id, error = %e2, "Could not record publish failure");
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!(error = %e, "Outbox: failed to fetch batch");
        }
    }
}

// ── DB helpers ────────────────────────────────────────────────────────────────

struct OutboxRow {
    id: Uuid,
    event_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
}

async fn fetch_pending_batch(pool: &PgPool, limit: i64) -> anyhow::Result<Vec<OutboxRow>> {
    // SELECT … FOR UPDATE SKIP LOCKED — safe for multiple concurrent workers.
    let rows = sqlx::query!(
        r#"
        SELECT id, event_id, event_type, payload
        FROM   outbox
        WHERE  status = 'PENDING'
        ORDER  BY created_at ASC
        LIMIT  $1
        FOR    UPDATE SKIP LOCKED
        "#,
        limit,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| OutboxRow {
            id: r.id,
            event_id: r.event_id,
            event_type: r.event_type,
            payload: r.payload,
        })
        .collect())
}

async fn publish_and_mark(
    pool: &PgPool,
    channel: &Channel,
    cfg: &OutboxConfig,
    row: &OutboxRow,
) -> anyhow::Result<()> {
    // Build the canonical EmailEvent JSON.
    //
    // Backwards-compatible promotion:
    //   Legacy payload: { "recipient": {...}, "payload": {...} }
    //   New payload:    { "recipients": [...], "payload": {...} }
    //
    // If the outbox row was written with a singular "recipient" key (old
    // business service), we wrap it in a one-element array so the consumer's
    // deserializer always receives the array form.  If "recipients" is already
    // present (new business service), we forward it verbatim.
    let recipients = if row.payload.get("recipients").is_some() {
        row.payload["recipients"].clone()
    } else if let Some(r) = row.payload.get("recipient") {
        serde_json::Value::Array(vec![r.clone()])
    } else {
        serde_json::Value::Array(vec![])
    };

    let event = serde_json::json!({
        "event_id":      row.event_id,
        "timestamp":     chrono::Utc::now().to_rfc3339(),
        "type":          row.event_type,
        "recipients":    recipients,
        "payload":       row.payload.get("payload").cloned().unwrap_or(serde_json::Value::Object(Default::default())),
        // Forward optional per-event sender override from the outbox payload.
        // Business service writes: { "from_override": { "email": "...", "name": "..." } }
        "from_override": row.payload.get("from_override").cloned().unwrap_or(serde_json::Value::Null),
        "metadata":      row.payload.get("metadata").cloned().unwrap_or(serde_json::Value::Null),
    });

    let body = serde_json::to_vec(&event)?;

    channel
        .basic_publish(
            &cfg.exchange,
            &cfg.routing_key,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await?
        .await?; // wait for broker confirm

    // Mark as PUBLISHED — atomic so duplicates can't slip through.
    sqlx::query!(
        r#"
        UPDATE outbox
        SET    status = 'PUBLISHED', published_at = now()
        WHERE  id = $1 AND status = 'PENDING'
        "#,
        row.id,
    )
    .execute(pool)
    .await?;

    info!(event_id = %row.event_id, event_type = %row.event_type, "Published outbox event");
    Ok(())
}

// ── Publish failure tracking ──────────────────────────────────────────────────

/// Increment the fail_count for an outbox row.
/// When fail_count reaches max_failures, mark the row as FAILED so it stops
/// being polled. This prevents a permanently-broken event from blocking the
/// outbox indefinitely.
async fn record_publish_failure(pool: &PgPool, id: Uuid, max_failures: i32) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE outbox
        SET    fail_count = fail_count + 1,
               status     = CASE WHEN fail_count + 1 >= $2 THEN 'FAILED' ELSE status END
        WHERE  id = $1
        "#,
        id,
        max_failures,
    )
    .execute(pool)
    .await?;
    Ok(())
}
