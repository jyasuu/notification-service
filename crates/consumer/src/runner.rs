use std::sync::Arc;
use std::time::Duration;

use common::{AppError, NotificationEvent, Recipient, RetryPolicy, SendMode};
use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use mailer::fetch_attachments_with_limit;
use mailer::message::ResolvedAttachment;
use metrics::counter;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::ConsumerConfig,
    processor::{
        is_retryable, process_group, process_recipient, ProcessorContext, RecipientOutcome,
    },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Redact credentials from an AMQP URL before logging.
///
/// Replaces "user:password@" with "[redacted]@" so broker host / vhost are
/// still visible in logs while credentials never appear in plaintext.
/// Falls back to "[redacted]" for any URL that does not match the expected
/// scheme so credentials are never accidentally surfaced.
fn scrub_amqp_url(url: &str) -> String {
    // amqp[s]://user:pass@host:port/vhost  →  amqp[s]://[redacted]@host:port/vhost
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3]; // "amqp://" or "amqps://"
            let after_at = &url[at_pos + 1..];
            return format!("{scheme}[redacted]@{after_at}");
        }
    }
    "[redacted]".to_string()
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
    // ── Deserialize — try new NotificationEvent shape, fall back to legacy EmailEvent ──
    //
    // The canonical shape is `NotificationEvent` (channel-agnostic envelope).
    // Publishers that still emit the legacy flat `EmailEvent` are promoted
    // transparently so no Outbox migration is required for existing business systems.
    //
    // The first error is logged at debug level before the fallback attempt so
    // that a genuinely malformed NotificationEvent (not a legacy payload) surfaces
    // the real field-level error rather than the less-informative EmailEvent error.
    #[allow(deprecated)]
    let event: NotificationEvent = {
        match serde_json::from_slice::<NotificationEvent>(&delivery.data) {
            Ok(e) => e,
            Err(first_err) => {
                tracing::debug!(
                    error = %first_err,
                    "NotificationEvent deserialization failed — attempting legacy EmailEvent fallback"
                );
                match serde_json::from_slice::<common::EmailEvent>(&delivery.data) {
                    Ok(legacy) => legacy.into_notification_event(),
                    Err(e) => {
                        error!(
                            notification_event_error = %first_err,
                            legacy_email_event_error = %e,
                            "Cannot deserialize event as NotificationEvent or legacy EmailEvent — sending to DLQ"
                        );
                        let _ = delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                ..Default::default()
                            })
                            .await;
                        return;
                    }
                }
            }
        }
    };

    // ── Extract email channel options ────────────────────────────────────────
    // If there are no email options, ACK cleanly so other (future) channels
    // can still process the event without it being re-queued.
    let email_opts = match event.channel_overrides.email.as_ref() {
        Some(opts) => opts.clone(),
        None => {
            warn!(event_id = %event.event_id, "Event has no email channel options — ACKing and skipping");
            let _ = delivery.ack(BasicAckOptions::default()).await;
            return;
        }
    };

    if email_opts.recipients.is_empty() {
        warn!(event_id = %event.event_id, "Event has no recipients — ACKing and skipping");
        let _ = delivery.ack(BasicAckOptions::default()).await;
        return;
    }

    // Guard against pathologically large recipient lists that would monopolise
    // the semaphore permit for an unbounded duration and exhaust DB connections.
    if email_opts.recipients.len() > cfg.max_recipients_per_event {
        error!(
            event_id        = %event.event_id,
            recipient_count = email_opts.recipients.len(),
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
    let resolved_attachments: Vec<ResolvedAttachment> = if email_opts.attachments.is_empty() {
        vec![]
    } else {
        match fetch_attachments_with_limit(
            &http,
            &email_opts.attachments,
            &event.timestamp,
            cfg.max_attachment_bytes,
        )
        .await
        {
            Ok(atts) => atts,
            Err(ref e) => {
                let permanent = e.is_permanent_mailer();
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

    // ── Dispatch on send_mode ─────────────────────────────────────────────────
    let attachments = Arc::new(resolved_attachments);
    let email_opts = Arc::new(email_opts);

    match email_opts.send_mode {
        // ── Group mode: one email, all To: addresses visible to each other ────
        SendMode::Group => {
            let ctx = ctx.clone();
            let event = event.clone();
            let email_opts = Arc::clone(&email_opts);
            let atts = Arc::clone(&attachments);
            let cfg = cfg.clone();
            let shutdown = shutdown.clone();

            // Group sends are driven by a single task — no per-recipient
            // parallelism. The runner's retry loop in process_one_group handles
            // back-off and max-retry logic identically to individual mode.
            process_one_group(&ctx, &event, &email_opts, &atts, &cfg, &shutdown).await;
        }

        // ── Individual mode: separate email per recipient (default) ───────────
        SendMode::Individual => {
            let mut join_set = JoinSet::new();
            for recipient in email_opts.recipients.clone() {
                let ctx = ctx.clone();
                let event = event.clone();
                let email_opts = Arc::clone(&email_opts);
                let atts = Arc::clone(&attachments);
                let cfg = cfg.clone();
                let shutdown = shutdown.clone();

                join_set.spawn(async move {
                    process_one_recipient(
                        &ctx,
                        &event,
                        &email_opts,
                        &recipient,
                        &atts,
                        &cfg,
                        &shutdown,
                    )
                    .await;
                });
            }

            // Wait for every recipient task to finish before ACKing. Task panics
            // are treated as permanent failures for that recipient — the join
            // error is logged but does not prevent the ACK, since other
            // recipients may have delivered successfully.
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    error!(
                        event_id = %event.event_id,
                        error    = %e,
                        "Recipient task panicked — treating as permanent failure"
                    );
                }
            }
        }
    }

    let _ = delivery.ack(BasicAckOptions::default()).await;
}

/// Drive one recipient through the send loop with per-recipient retry.
async fn process_one_recipient(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
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
        match process_recipient(ctx, event, email_opts, recipient, attachments, shutdown).await {
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

            // NoRetry policy — any failure (including rate-limits) is
            // treated as immediately exhausted.  Mark FAILED now rather than
            // waiting for back-off cycles.  The row remains visible in status
            // queries and can be replayed via the operator retry API.
            RecipientOutcome::Failed(ref e) if email_opts.retry_policy == RetryPolicy::NoRetry => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    error    = %e,
                    "NoRetry policy — marking FAILED without automatic retry"
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

            // GroupFailedWithIndividualRows is only emitted by the group-send
            // path in processor.  It should never appear here in the
            // individual-send loop; treat it as an unexpected error and stop.
            RecipientOutcome::GroupFailedWithIndividualRows(ref e) => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    error    = %e,
                    "Unexpected GroupFailedWithIndividualRows in individual-send loop — marking FAILED"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return;
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
                //
                // We additionally clamp the computed delay to 30 minutes so that
                // a large retry_base_ms (e.g. 5 000 ms × 2^10 ≈ 85 min) does not
                // strand the un-ACK'd AMQP message beyond any reasonable consumer
                // timeout.  Operators who need longer hold times should instead
                // increase max_retries and keep retry_base_ms ≤ 2 000.
                const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1000; // 30 minutes
                let delay = Duration::from_millis(
                    (cfg.retry_base_ms * (1 << attempt.min(10))).min(MAX_RETRY_DELAY_MS)
                );
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

// ── Group send retry wrapper ─────────────────────────────────────────────────

/// Drive a group send through the retry loop.
///
/// Mirrors `process_one_recipient` but calls `process_group` instead, which
/// builds one `EmailMessage` with all recipients sharing the `To:` header.
async fn process_one_group(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    attachments: &[mailer::message::ResolvedAttachment],
    cfg: &ConsumerConfig,
    shutdown: &tokio_util::sync::CancellationToken,
) {
    let mut attempt: u32 = 0;
    let mut rl_count: u32 = 0;

    loop {
        match process_group(ctx, event, email_opts, attachments, shutdown).await {
            RecipientOutcome::Sent | RecipientOutcome::Blocked(_) | RecipientOutcome::Skipped => {
                return;
            }

            RecipientOutcome::Duplicate { retry_count } => {
                attempt = retry_count as u32;
                continue;
            }

            RecipientOutcome::Failed(ref e) if !is_retryable(e) => {
                error!(
                    event_id = %event.event_id,
                    error    = %e,
                    "Permanent failure for group send — marking FAILED"
                );
                if let Some(primary) = email_opts.recipients.first() {
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &primary.email, &e.to_string(), true)
                        .await;
                }
                return;
            }

            RecipientOutcome::Failed(ref e) if attempt >= cfg.max_retries => {
                error!(
                    event_id = %event.event_id,
                    attempt,
                    "Max retries exhausted for group send"
                );
                if let Some(primary) = email_opts.recipients.first() {
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &primary.email, &e.to_string(), true)
                        .await;
                }
                return;
            }

            // NoRetry policy — fail immediately, same as the individual path.
            RecipientOutcome::Failed(ref e) if email_opts.retry_policy == RetryPolicy::NoRetry => {
                error!(
                    event_id = %event.event_id,
                    error    = %e,
                    "NoRetry policy — marking group send FAILED without automatic retry"
                );
                if let Some(primary) = email_opts.recipients.first() {
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &primary.email, &e.to_string(), true)
                        .await;
                }
                return;
            }

            RecipientOutcome::Failed(AppError::RateLimited(ref msg)) => {
                rl_count += 1;
                if rl_count > cfg.max_rl_waits {
                    error!(
                        event_id     = %event.event_id,
                        rl_count,
                        max_rl_waits = cfg.max_rl_waits,
                        "Rate-limit backoff limit reached for group send — marking FAILED"
                    );
                    if let Some(primary) = email_opts.recipients.first() {
                        let _ = ctx
                            .store
                            .mark_failed(event.event_id, &primary.email, msg, true)
                            .await;
                    }
                    return;
                }
                let delay = Duration::from_secs(30 * (1u64 << attempt.min(3)));
                warn!(
                    event_id   = %event.event_id,
                    rl_count,
                    delay_secs = delay.as_secs(),
                    "Group send rate-limited — backing off"
                );
                if let Some(primary) = email_opts.recipients.first() {
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &primary.email, msg, false)
                        .await;
                }
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        if let Some(primary) = email_opts.recipients.first() {
                            let _ = ctx
                                .store
                                .mark_failed(event.event_id, &primary.email,
                                    "service shutdown during rate-limit backoff", true)
                                .await;
                        }
                        return;
                    }
                }
            }

            // ── Individual-row fallback ──────────────────────────────────────
            // `process_group` already wrote a `notification_log` row for *every*
            // recipient (GroupRetryMode::Individual) before the send attempt
            // failed.  Re-sending the whole group email would duplicate
            // recipients who were already delivered to by the SMTP server
            // before the connection dropped.
            //
            // Instead, fall back to `process_one_recipient` for each address.
            // Recipients whose row is already SENT or BLOCKED will be skipped
            // by the idempotency check inside `process_recipient`, so only
            // genuinely unsent addresses receive a new (individual) email.
            //
            // Trade-off: retried recipients receive a separate email whose
            // `To:` header shows only their own address; the shared-`To:`
            // visibility of the original group email is not preserved on retry.
            RecipientOutcome::GroupFailedWithIndividualRows(ref e) => {
                warn!(
                    event_id         = %event.event_id,
                    error            = %e,
                    recipient_count  = email_opts.recipients.len(),
                    "Group send failed after per-recipient rows written \
                     — falling back to individual retry path"
                );
                let mut join_set = tokio::task::JoinSet::new();
                for recipient in email_opts.recipients.clone() {
                    let ctx = ctx.clone();
                    let event = event.clone();
                    let opts = email_opts.clone();
                    let atts = attachments.to_vec();
                    let cfg = cfg.clone();
                    let shutdown = shutdown.clone();
                    join_set.spawn(async move {
                        process_one_recipient(
                            &ctx, &event, &opts, &recipient, &atts, &cfg, &shutdown,
                        )
                        .await;
                    });
                }
                while let Some(result) = join_set.join_next().await {
                    if let Err(e) = result {
                        error!(
                            event_id = %event.event_id,
                            error    = %e,
                            "Individual-retry task panicked during group-send fallback"
                        );
                    }
                }
                return;
            }

            RecipientOutcome::Failed(ref e) => {
                attempt += 1;
                rl_count = 0;
                // Same cap as process_one_recipient — see comment there.
                const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1000; // 30 minutes
                let delay = Duration::from_millis(
                    (cfg.retry_base_ms * (1 << attempt.min(10))).min(MAX_RETRY_DELAY_MS)
                );
                warn!(
                    event_id = %event.event_id,
                    attempt,
                    delay_ms = delay.as_millis(),
                    error    = %e,
                    "Group send transient failure — retrying"
                );
                if let Some(primary) = email_opts.recipients.first() {
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &primary.email, &e.to_string(), false)
                        .await;
                }
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        if let Some(primary) = email_opts.recipients.first() {
                            let _ = ctx
                                .store
                                .mark_failed(event.event_id, &primary.email,
                                    "service shutdown during retry backoff", true)
                                .await;
                        }
                        return;
                    }
                }
            }
        }
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
    // whether the error message contains "404" and proceed with the normal
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
                // Queue exists and arguments are compatible — active declare is
                // a no-op, but we still call it below for bind idempotency.
            }
            Err(e)
                if {
                    let s = e.to_string().to_lowercase();
                    s.contains("404")
                        || s.contains("not found")
                        || s.contains("not_found")
                        || s.contains("not-found")
                        || s.contains("notfound")
                } =>
            {
                // Queue does not yet exist — proceed with the normal active declare.
                // The probe channel is now closed by the broker; we discard it.
                // NOTE: lapin::channel logs an ERROR internally when the broker
                // closes the probe channel with 404; that is expected noise on
                // first boot and can be silenced with RUST_LOG=lapin=warn.
                tracing::debug!(
                    queue = queue_name,
                    "Queue not found on passive probe — will be created by active declare"
                );
            }
            Err(e) => {
                // Any other error (e.g. 406 PRECONDITION_FAILED for argument
                // mismatch) is surfaced here with a clear, actionable message
                // rather than a cryptic "channel closed" from the reconnect loop.
                return Err(anyhow::anyhow!(
                    "passive check for queue '{}' failed — \
                     the broker may have it declared with different arguments \
                     (check x-dead-letter-exchange, durable flag, etc.): {e}",
                    queue_name
                ));
            }
        }
        // probe is dropped here; the broker-closed channel is cleaned up.
    }

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
