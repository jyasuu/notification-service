use std::sync::Arc;
use std::time::Duration;

use common::{AppError, EmailEvent, Recipient};
use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use mailer::message::ResolvedAttachment;
use mailer::{fetch_attachments_with_limit, EmailSender, SenderRegistry};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use reqwest::Client;
use store::{EmailLogStore, TemplateStore};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::ConsumerConfig,
    processor::{is_retryable, process_recipient, ProcessorContext, RecipientOutcome},
};

// ── Public entry point ────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn run_consumer(
    cfg: ConsumerConfig,
    store: EmailLogStore,
    template_store: TemplateStore,
    sender: Arc<dyn EmailSender>,
    sender_registry: SenderRegistry,
    filter: RecipientFilter,
    rate_limiter: MailRateLimiter,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let http = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client");
    let http = Arc::new(http);
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency));
    let mut reconnect_delay = Duration::from_secs(2);

    // Build the shared context once; all spawned tasks clone it cheaply.
    let ctx = ProcessorContext {
        store,
        template_store,
        sender,
        sender_registry,
        filter,
        rate_limiter,
    };

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        info!(url = %cfg.amqp_url, "Connecting to RabbitMQ");
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
    declare_topology(&channel, cfg).await?;

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

// ── Per-delivery handler ──────────────────────────────────────────────────────

/// Handle one delivery.
///
/// Attachments are fetched ONCE here at the event level, then the resolved
/// bytes are passed to every recipient task.  This prevents N×M HTTP GETs
/// and pre-signed URL expiry for recipients processed later in the list.
///
/// Recipients are processed **in parallel** via a `JoinSet`: each recipient
/// gets its own task with its own retry loop.  The semaphore permit is held
/// for the entire delivery so total in-flight messages stays bounded, but
/// within a message recipients no longer block each other — a long retry
/// backoff on one address does not delay sends to other addresses in the
/// same event.
///
/// The AMQP message is ACK'd once ALL recipient tasks have finished.
/// It is NACK'd (→ DLQ) only if the message cannot be deserialized or if
/// the event-level attachment fetch fails permanently.
async fn handle_delivery(
    delivery: lapin::message::Delivery,
    ctx: ProcessorContext,
    http: Arc<Client>,
    cfg: ConsumerConfig,
    shutdown: CancellationToken,
) {
    let event: EmailEvent = match serde_json::from_slice(&delivery.data) {
        Ok(e) => e,
        Err(e) => {
            error!(error = %e, "Cannot deserialize event — sending to DLQ");
            let _ = delivery
                .nack(BasicNackOptions {
                    requeue: false,
                    ..Default::default()
                })
                .await;
            return;
        }
    };

    if event.recipients.is_empty() {
        warn!(event_id = %event.event_id, "Event has no recipients — ACKing and skipping");
        let _ = delivery.ack(BasicAckOptions::default()).await;
        return;
    }

    // Guard against pathologically large recipient lists that would monopolise
    // the semaphore permit for an unbounded duration and exhaust DB connections.
    // Events exceeding the limit are sent to the DLQ for operator inspection.
    if event.recipients.len() > cfg.max_recipients_per_event {
        error!(
            event_id        = %event.event_id,
            recipient_count = event.recipients.len(),
            limit           = cfg.max_recipients_per_event,
            "Event exceeds max_recipients_per_event — sending to DLQ"
        );
        let _ = delivery
            .nack(BasicNackOptions {
                requeue: false,
                ..Default::default()
            })
            .await;
        return;
    }

    // ── Fetch attachments once for the whole event ───────────────────────────
    // Attachment bytes are held in memory for the lifetime of this handler
    // (until all recipient tasks complete). Peak memory per event is:
    //   num_attachments × max_attachment_bytes (default: 10 MiB each).
    // With max_concurrency concurrent handlers this becomes:
    //   max_concurrency × num_attachments × max_attachment_bytes.
    // Size-cap `max_attachment_bytes` in config if memory pressure is a concern.
    let resolved_attachments: Vec<ResolvedAttachment> = if event.attachments.is_empty() {
        vec![]
    } else {
        match fetch_attachments_with_limit(
            &http,
            &event.attachments,
            &event.timestamp,
            cfg.max_attachment_bytes,
        )
        .await
        {
            Ok(atts) => atts,
            Err(ref e) => {
                let permanent = matches!(e, AppError::Mailer(m) if m.starts_with("permanent:"));
                error!(
                    event_id  = %event.event_id,
                    error     = %e,
                    permanent,
                    "Attachment fetch failed — NACKing message"
                );
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: !permanent,
                        ..Default::default()
                    })
                    .await;
                return;
            }
        }
    };

    // ── Process all recipients in parallel ───────────────────────────────────
    // Each recipient gets its own task so a long retry backoff for one address
    // does not delay sends to other addresses in the same event.
    //
    // `resolved_attachments` is wrapped in an Arc so every task can hold a
    // cheap reference to the shared bytes rather than cloning the full payload
    // (which could be tens of MiB when attachments are present).
    let attachments = Arc::new(resolved_attachments);

    let mut join_set = JoinSet::new();
    for recipient in event.recipients.clone() {
        let ctx = ctx.clone();
        let event = event.clone();
        let atts = Arc::clone(&attachments);
        let cfg = cfg.clone();
        let shutdown = shutdown.clone();

        join_set.spawn(async move {
            process_one_recipient(&ctx, &event, &recipient, &atts, &cfg, &shutdown).await;
        });
    }

    // Wait for every recipient task to finish (success, permanent failure, or
    // max retries exhausted) before ACKing the message.  Task panics are
    // treated as permanent failures for that recipient — the join error is
    // logged but does not prevent the ACK, since other recipients may have
    // delivered successfully and we must not re-process them.
    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            error!(
                event_id = %event.event_id,
                error    = %e,
                "Recipient task panicked — treating as permanent failure"
            );
        }
    }

    let _ = delivery.ack(BasicAckOptions::default()).await;
}

/// Drive one recipient through the send loop with per-recipient retry.
async fn process_one_recipient(
    ctx: &ProcessorContext,
    event: &EmailEvent,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    cfg: &ConsumerConfig,
    shutdown: &CancellationToken,
) {
    // attempt is seeded from 0 on the first call; if the row already exists
    // (restart / re-delivery), process_recipient returns Duplicate { retry_count }
    // on the first iteration and we update attempt here — no separate DB query.
    let mut attempt: u32 = 0;
    let mut rl_count: u32 = 0;

    loop {
        match process_recipient(ctx, event, recipient, attachments, shutdown).await {
            RecipientOutcome::Sent | RecipientOutcome::Blocked(_) | RecipientOutcome::Skipped => {
                return;
            }

            // Row existed and is non-terminal — seed attempt counter from DB
            // value and immediately retry without consuming a retry slot.
            RecipientOutcome::Duplicate { retry_count } => {
                attempt = retry_count as u32;
                continue;
            }

            RecipientOutcome::Failed(ref e) if !is_retryable(e) => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    error    = %e,
                    "Permanent failure for recipient — marking FAILED"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return;
            }

            RecipientOutcome::Failed(ref e) if attempt >= cfg.max_retries => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    attempt,
                    "Max retries exhausted for recipient"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return;
            }

            // Rate-limited — back off without consuming a retry slot,
            // but cap consecutive rate-limit waits to prevent infinite loops.
            RecipientOutcome::Failed(AppError::RateLimited(ref msg)) => {
                rl_count += 1;
                if rl_count > cfg.max_rl_waits {
                    error!(
                        event_id   = %event.event_id,
                        email      = %recipient.email,
                        rl_count,
                        max_rl_waits = cfg.max_rl_waits,
                        "Rate-limit backoff limit reached — marking FAILED"
                    );
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &recipient.email, msg, true)
                        .await;
                    return;
                }
                let delay = Duration::from_secs(30 * (1u64 << attempt.min(3)));
                warn!(
                    event_id   = %event.event_id,
                    email      = %recipient.email,
                    rl_count,
                    delay_secs = delay.as_secs(),
                    "Rate-limited — backing off without consuming retry slot"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, msg, false)
                    .await;
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        // Shutdown arrived during backoff. The row is already
                        // PENDING (mark_failed with exhausted=false above), which
                        // would leave it stuck with no queue message to pick it up.
                        // Flip it to FAILED so the operator can see it and use the
                        // retry API after the service restarts.
                        warn!(
                            event_id = %event.event_id,
                            email    = %recipient.email,
                            "Shutdown during rate-limit backoff — marking FAILED for manual retry"
                        );
                        let _ = ctx
                            .store
                            .mark_failed(event.event_id, &recipient.email,
                                "service shutdown during rate-limit backoff", true)
                            .await;
                        return;
                    }
                }
            }

            // Transient failure — normal exponential backoff
            RecipientOutcome::Failed(ref e) => {
                attempt += 1;
                rl_count = 0;
                // The `.min(10)` caps the *shift* (not `attempt` itself) to prevent
                // overflow: 1 << 10 = 1024, so with the default retry_base_ms=1000
                // the maximum single delay is ~17 min.  `attempt` may legitimately
                // exceed 10 when seeded from a high DB retry_count after a restart,
                // but the delay stays capped at this ceiling regardless.
                let delay = Duration::from_millis(cfg.retry_base_ms * (1 << attempt.min(10)));
                warn!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    attempt,
                    delay_ms = delay.as_millis(),
                    error    = %e,
                    "Transient failure — retrying"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), false)
                    .await;
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        // Shutdown arrived during retry backoff. The row is already
                        // PENDING; flip it to FAILED so it is visible and recoverable
                        // via the retry API after restart.
                        warn!(
                            event_id = %event.event_id,
                            email    = %recipient.email,
                            attempt,
                            "Shutdown during retry backoff — marking FAILED for manual retry"
                        );
                        let _ = ctx
                            .store
                            .mark_failed(event.event_id, &recipient.email,
                                "service shutdown during retry backoff", true)
                            .await;
                        return;
                    }
                }
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

// ── Topology ──────────────────────────────────────────────────────────────────

async fn declare_topology(channel: &Channel, cfg: &ConsumerConfig) -> anyhow::Result<()> {
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

    let dlx_name = format!("{}.dlx", cfg.exchange);
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

    let dlq_name = format!("{}.dlq", cfg.queue);
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
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut args = FieldTable::default();
    args.insert(
        "x-dead-letter-exchange".into(),
        lapin::types::AMQPValue::LongString(dlx_name.into()),
    );
    channel
        .queue_declare(
            &cfg.queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
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

    info!("AMQP topology declared");
    Ok(())
}
