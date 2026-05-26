//! AMQP connection loop and queue topology setup.
//!
//! This module owns the outermost lifecycle of the consumer:
//!
//! * [`run_consumer`] — reconnect loop with exponential back-off.
//! * `connect_and_consume` — one connection lifetime: declare topology, open
//!   a channel, pull deliveries, and dispatch each to [`delivery::handle_delivery`].
//! * `declare_topology` — passive-then-active queue/exchange declarations.
//! * `append_heartbeat_param` — URL helper that injects the AMQP heartbeat param.
//!
//! Per-delivery processing (deserialise, fetch attachments, retry loops) lives
//! in [`delivery`].  Per-recipient logic (idempotency, template render, send)
//! lives in [`processor`].

use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use metrics::counter;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Returns `true` when a lapin error is an AMQP 404 NOT_FOUND reply.
///
/// Used during topology setup to distinguish "queue does not exist yet"
/// (normal first-run) from a genuine broker error (wrong args, auth, etc.).
///
/// Previously this was a string-match on `e.to_string()`, which was fragile
/// against changes in how lapin formats error messages.  We now match on the
/// structured `AMQPSoftError::NOTFOUND` variant from `amq_protocol`, which is
/// generated directly from the AMQP 0-9-1 spec and is stable.
fn is_not_found(e: &lapin::Error) -> bool {
    use amq_protocol::protocol::{AMQPErrorKind, AMQPSoftError};
    matches!(
        e,
        lapin::Error::ProtocolError(amqp_err)
            if matches!(
                amqp_err.kind(),
                AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND)
            )
    )
}

use crate::{config::ConsumerConfig, delivery::handle_delivery, processor::ProcessorContext};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Redact credentials from an AMQP URL before logging.
///
/// Replaces "user:password@" with "[redacted]@" so broker host / vhost are
/// still visible in logs while credentials never appear in plaintext.
/// When there is no "@" in the URL (no embedded credentials), the URL is
/// returned as-is — there is nothing to redact and the broker hostname is
/// useful in logs.
fn scrub_amqp_url(url: &str) -> String {
    // amqp[s]://user:pass@host:port/vhost  →  amqp[s]://[redacted]@host:port/vhost
    //
    // Primary path: standard scheme + userinfo.
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3]; // "amqp://" or "amqps://"
            let after_at = &url[at_pos + 1..];
            return format!("{scheme}[redacted]@{after_at}");
        }

        // Defensive fallback: URL contains "@" but has no recognisable "://"
        // (e.g. a misconfigured "amqp:user:pass@host" without the double-slash).
        // Rather than logging the raw URL and leaking credentials, redact the
        // entire string.  The operator can check their config for the correct URL.
        return "[redacted — unrecognised URL format containing '@']".to_owned();
    }
    // No "@" means no embedded credentials — return the URL unchanged so the
    // broker hostname remains visible in logs (e.g. "amqps://broker.example.com:5671").
    url.to_owned()
}

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run_consumer(
    cfg: ConsumerConfig,
    ctx: ProcessorContext,
    http: Arc<Client>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency));
    let mut reconnect_delay = Duration::from_secs(2);

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        info!(url = %scrub_amqp_url(&cfg.amqp_url), "Connecting to RabbitMQ");
        let connected_at = std::time::Instant::now();

        match connect_and_consume(
            &cfg,
            ctx.clone(),
            Arc::clone(&semaphore),
            Arc::clone(&http),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) => {
                info!("Consumer loop exited cleanly");
                return Ok(());
            }
            Err(e) if shutdown.is_cancelled() => {
                info!(error = %e, "Consumer exited after shutdown");
                return Ok(());
            }
            Err(e) => {
                // If the connection stayed alive for a meaningful period before
                // failing, treat this as a fresh start and reset the backoff.
                // This prevents a long-lived connection that eventually drops
                // from carrying a near-maximum delay into the very next reconnect.
                if connected_at.elapsed() > Duration::from_secs(30) {
                    reconnect_delay = Duration::from_secs(2);
                }
                counter!("consumer_reconnects_total").increment(1);
                error!(error = %e, delay_secs = reconnect_delay.as_secs(), "Consumer error — reconnecting");
                tokio::select! {
                    _ = sleep(reconnect_delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

// ── One connection lifetime ───────────────────────────────────────────────────

async fn connect_and_consume(
    cfg: &ConsumerConfig,
    ctx: ProcessorContext,
    semaphore: Arc<Semaphore>,
    http: Arc<Client>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // ── AMQP heartbeat ────────────────────────────────────────────────────────
    // RabbitMQ 3.12+ enforces a server-side consumer_timeout (default 30 min).
    // A recipient undergoing repeated transient-failure backoffs can hold an
    // un-ACK'd message for many minutes; without heartbeats the broker sees a
    // silent connection and may cancel the consumer or close the channel.
    //
    // Heartbeat is negotiated during the AMQP Connection.Tune handshake.
    // The broker picks min(client, server); 60 s matches the RabbitMQ default
    // so this is effectively a no-op against a stock broker and a safety net
    // against one configured with a higher value.  Appending to the URI keeps
    // the approach compatible with lapin 2.x without additional dependencies.
    let amqp_url_with_heartbeat = append_heartbeat_param(&cfg.amqp_url, 60);
    let conn =
        Connection::connect(&amqp_url_with_heartbeat, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    declare_topology(&conn, &channel, cfg).await?;

    let mut consumer = channel
        .basic_consume(
            &cfg.queue,
            "anvil-notify",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(queue = %cfg.queue, "Listening for messages");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Shutdown: draining in-flight tasks");
                let _ = semaphore.acquire_many(cfg.max_concurrency as u32).await;
                return Ok(());
            }
            delivery = consumer.next() => {
                let delivery = match delivery {
                    Some(Ok(d)) => d,
                    Some(Err(e)) => { error!(error = %e, "AMQP error"); return Err(e.into()); }
                    None => { warn!("Consumer stream ended"); return Err(anyhow::anyhow!("stream closed")); }
                };

                let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore closed");
                let ctx    = ctx.clone();
                let cfg    = cfg.clone();
                let http   = Arc::clone(&http);
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    handle_delivery(delivery, ctx, http, cfg, shutdown).await;
                });
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Append `?heartbeat=<secs>` to an AMQP URL if not already present.
///
/// The heartbeat is negotiated during the AMQP `Connection.Tune` handshake:
/// the broker picks `min(client, server)` so this value is a ceiling, not a
/// floor.  Setting it ensures a heartbeat IS negotiated even if the broker's
/// default is 0 (disabled) or very high.
///
/// If the URL already contains a `heartbeat` query parameter it is left
/// untouched — the operator's explicit value takes precedence.
fn append_heartbeat_param(url: &str, heartbeat_secs: u16) -> String {
    if url.contains("heartbeat=") {
        return url.to_owned();
    }
    if url.contains('?') {
        format!("{url}&heartbeat={heartbeat_secs}")
    } else {
        format!("{url}?heartbeat={heartbeat_secs}")
    }
}

#[cfg(test)]
mod heartbeat_tests {
    use super::append_heartbeat_param;

    #[test]
    fn appends_to_plain_url() {
        let url = "amqp://guest:guest@localhost:5672";
        assert_eq!(
            append_heartbeat_param(url, 60),
            "amqp://guest:guest@localhost:5672?heartbeat=60"
        );
    }

    #[test]
    fn appends_to_url_with_existing_query() {
        let url = "amqp://guest:guest@localhost:5672/%2f?connection_timeout=10000";
        assert_eq!(
            append_heartbeat_param(url, 60),
            "amqp://guest:guest@localhost:5672/%2f?connection_timeout=10000&heartbeat=60"
        );
    }

    #[test]
    fn leaves_existing_heartbeat_untouched() {
        let url = "amqp://guest:guest@localhost:5672?heartbeat=30";
        assert_eq!(append_heartbeat_param(url, 60), url);
    }
}

#[cfg(test)]
mod scrub_url_tests {
    use super::scrub_amqp_url;

    #[test]
    fn redacts_standard_credentials() {
        assert_eq!(
            scrub_amqp_url("amqp://user:secret@broker.example.com:5672"),
            "amqp://[redacted]@broker.example.com:5672"
        );
    }

    #[test]
    fn redacts_amqps_credentials() {
        assert_eq!(
            scrub_amqp_url("amqps://user:secret@broker.example.com:5671/vhost"),
            "amqps://[redacted]@broker.example.com:5671/vhost"
        );
    }

    #[test]
    fn passthrough_when_no_at_sign() {
        // No embedded credentials — broker hostname should be visible.
        let url = "amqps://broker.example.com:5671";
        assert_eq!(scrub_amqp_url(url), url);
    }

    #[test]
    fn redacts_malformed_url_with_at_sign() {
        // Misconfigured URL without "://" but still containing "@" —
        // must never leak credentials, even if the format is unrecognised.
        let result = scrub_amqp_url("amqp:user:secret@broker.example.com");
        assert!(!result.contains("secret"), "credentials leaked: {result}");
    }
}

// ── Topology ──────────────────────────────────────────────────────────────────

async fn declare_topology(
    conn: &Connection,
    channel: &Channel,
    cfg: &ConsumerConfig,
) -> anyhow::Result<()> {
    // ── Passive existence checks ──────────────────────────────────────────────
    // RabbitMQ returns a channel-level 406 PRECONDITION_FAILED if a queue or
    // exchange is re-declared with arguments that differ from the existing
    // definition (e.g. a queue that already exists without a DLX argument, or
    // with a different `durable` flag).  This error closes the channel and
    // surfaces in the reconnect loop as a cryptic "channel closed" message.
    //
    // We do a passive declare first: if the queue already exists, lapin will
    // succeed silently; if the arguments would conflict, RabbitMQ returns the
    // 406 PRECONDITION_FAILED error here where we can report it clearly before
    // the active declare ever fires.  If the queue does NOT yet exist, the
    // passive declare returns a 404 NOT_FOUND — we detect this by checking
    // whether the error message contains "404" or "NOT_FOUND" and proceed with the normal
    // active declare.
    let dlq_name = format!("{}.dlq", cfg.queue);
    let dlx_name = format!("{}.dlx", cfg.exchange);

    for queue_name in [dlq_name.as_str(), cfg.queue.as_str()] {
        // Each passive check uses its own throw-away channel.
        // RabbitMQ closes the channel on a 404 NOT_FOUND response; by using a
        // dedicated probe channel we protect the working `channel` from being
        // closed when the queue simply does not exist yet.
        let probe = conn.create_channel().await?;
        match probe
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
        {
            Ok(_) => {
                // Queue exists — active declare below will be a no-op (same args).
                info!(
                    queue = queue_name,
                    "Queue already exists — skipping active declare"
                );
            }
            Err(ref e) if is_not_found(e) => {
                // Queue does not exist yet — normal first-run path.
                info!(
                    queue = queue_name,
                    "Queue does not exist yet — will declare"
                );
            }
            Err(e) => {
                // 406 PRECONDITION_FAILED or other error — surface it clearly.
                // Common causes:
                //   • Queue was declared without the x-dead-letter-exchange arg and
                //     now AnvilNotify is trying to add it (delete the queue first).
                //   • Queue `durable` flag differs from the existing declaration.
                //   • A different DLX name was used previously.
                return Err(anyhow::anyhow!(
                    "Passive queue check for '{queue_name}' failed — this is usually a \
                     queue argument mismatch (e.g. x-dead-letter-exchange or durable flag \
                     changed). Delete the queue from the broker and restart to re-declare it. \
                     Broker error: {e}"
                ));
            }
        }
    }

    // ── DLX / DLQ ─────────────────────────────────────────────────────────────
    // The DLX is a fanout exchange: when RabbitMQ dead-letters a message it
    // publishes with the original routing key, but fanout ignores the key and
    // broadcasts to every bound queue (just the DLQ here).  Using Direct would
    // require an exact routing-key match between the dead-lettered message and
    // the DLQ binding, which is fragile when queue names change.
    channel
        .exchange_declare(
            &dlx_name,
            lapin::ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            &dlq_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            &dlq_name,
            &dlx_name,
            &cfg.queue,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    // ── Main queue (with DLX argument) ────────────────────────────────────────
    let mut queue_args = FieldTable::default();
    queue_args.insert(
        "x-dead-letter-exchange".into(),
        lapin::types::AMQPValue::LongString(dlx_name.clone().into()),
    );

    channel
        .queue_declare(
            &cfg.queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_args,
        )
        .await?;

    // ── Exchange + binding ────────────────────────────────────────────────────
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

    channel
        .queue_bind(
            &cfg.queue,
            &cfg.exchange,
            &cfg.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    // Prefetch exactly max_concurrency messages so the broker queues up as many
    // messages as the semaphore allows, enabling true parallel message processing.
    //
    // Previously this was hard-coded to 1, which silently capped throughput to a
    // single in-flight AMQP message regardless of the max_concurrency setting.
    // Setting prefetch = max_concurrency means the semaphore in the delivery loop
    // is the actual back-pressure control: the broker delivers up to
    // max_concurrency messages, and each is processed concurrently until its
    // permit is released.
    //
    // Trade-off: a burst of max_concurrency large multi-recipient events will
    // hold max_concurrency semaphore permits simultaneously, so peak memory is:
    //   max_concurrency × (attachments per event) × max_attachment_bytes
    // Size your container accordingly (see config/default.toml [amqp]).
    //
    // With multiple replicas, each instance independently enforces its own
    // max_concurrency ceiling via the semaphore; the broker distributes messages
    // round-robin across consumers.
    let prefetch = cfg.max_concurrency.min(u16::MAX as usize) as u16;
    channel
        .basic_qos(prefetch, BasicQosOptions::default())
        .await?;

    info!(
        queue    = %cfg.queue,
        exchange = %cfg.exchange,
        dlx      = %dlx_name,
        dlq      = %dlq_name,
        "AMQP topology declared"
    );

    Ok(())
}
