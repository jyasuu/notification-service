//! Per-delivery handler and per-recipient / per-group retry loops.
//!
//! This module owns everything that happens after a single AMQP `Delivery`
//! is pulled off the wire:
//!
//! 1. **Deserialise** — decode the raw bytes into a `NotificationEvent`,
//!    falling back to the legacy `EmailEvent` shape for older publishers.
//! 2. **Validate** — check for an email channel, non-empty recipients, and
//!    the configured `max_recipients_per_event` ceiling.
//! 3. **Fetch attachments** — resolve pre-signed URLs once for the whole event.
//! 4. **Dispatch** — for each recipient, spawn a task and run
//!    `process_one_recipient`; for group sends, run `process_one_group`.
//! 5. **ACK / NACK** — ACK once all recipient tasks finish; NACK to DLQ on
//!    unrecoverable event-level failures (bad JSON, expired attachments).
//!
//! The connection loop and AMQP topology setup live in `runner.rs`; the
//! per-recipient processor logic (idempotency, template rendering, send) lives
//! in `processor.rs`.  This module is the glue between them.

use std::sync::Arc;
use std::time::Duration;

use common::{is_valid_email, AppError, NotificationEvent, Recipient, RetryPolicy, SendMode};
use lapin::{message::Delivery, options::*};
use mailer::fetch_attachments_with_limit;
use mailer::message::ResolvedAttachment;
use reqwest::Client;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::{
    config::ConsumerConfig,
    processor::{
        is_retryable, process_group, process_recipient, EffectiveCcBcc, ProcessorContext,
        RecipientOutcome,
    },
};

// ── Public entry point ────────────────────────────────────────────────────────

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
pub(crate) async fn handle_delivery(
    delivery: Delivery,
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
                    permanent,
                    error     = %e,
                    "Attachment fetch failed — {}",
                    if permanent { "sending to DLQ" } else { "re-queueing" }
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

    // ── CC/BCC validation and filtering (once per event) ──────────────────
    // Invalid CC/BCC addresses are a permanent failure for the whole event.
    // Blocked CC/BCC addresses are silently excluded — logged at WARN level —
    // and delivery continues.  This runs once here rather than inside each
    // per-recipient task to avoid N×M filter evaluations and log noise.
    for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
        if !is_valid_email(&r.email) {
            warn!(
                event_id = %event.event_id,
                email    = %r.email,
                "Invalid CC/BCC address — sending to DLQ"
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
    let cc_bcc = {
        let cc: Vec<Recipient> = email_opts
            .cc
            .iter()
            .filter(|r| match ctx.filter.check(&r.email) {
                Ok(()) => true,
                Err(AppError::Blocked(ref reason)) => {
                    warn!(
                        event_id = %event.event_id,
                        email    = %r.email,
                        reason   = %reason,
                        "CC address blocked by filter — excluding from delivery"
                    );
                    false
                }
                Err(_) => true, // fail-open: unknown filter errors never silently drop
            })
            .cloned()
            .collect();
        let bcc: Vec<Recipient> = email_opts
            .bcc
            .iter()
            .filter(|r| match ctx.filter.check(&r.email) {
                Ok(()) => true,
                Err(AppError::Blocked(ref reason)) => {
                    warn!(
                        event_id = %event.event_id,
                        email    = %r.email,
                        reason   = %reason,
                        "BCC address blocked by filter — excluding from delivery"
                    );
                    false
                }
                Err(_) => true,
            })
            .cloned()
            .collect();
        Arc::new(EffectiveCcBcc { cc, bcc })
    };

    // ── Dispatch: group send or per-recipient individual sends ───────────────
    if email_opts.send_mode == SendMode::Group {
        process_one_group(
            &ctx,
            &event,
            &email_opts,
            &resolved_attachments,
            cc_bcc,
            &cfg,
            &shutdown,
        )
        .await;
    } else {
        // Individual mode — spawn one task per recipient so they are processed
        // concurrently within the event.  Each task has its own retry loop;
        // a slow or failing recipient does not block the others.
        let mut join_set = JoinSet::new();
        for recipient in email_opts.recipients.clone() {
            let ctx = ctx.clone();
            let event = event.clone();
            let opts = email_opts.clone();
            let atts = resolved_attachments.clone();
            let cfg = cfg.clone();
            let shutdown = shutdown.clone();
            let cc_bcc = Arc::clone(&cc_bcc);
            join_set.spawn(async move {
                process_one_recipient(
                    &ctx, &event, &opts, &recipient, &atts, &cc_bcc, &cfg, &shutdown,
                )
                .await;
            });
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!(
                    event_id = %event.event_id,
                    error    = %e,
                    "Recipient task panicked"
                );
            }
        }
    }

    let _ = delivery.ack(BasicAckOptions::default()).await;
}

// ── Per-recipient retry loop ──────────────────────────────────────────────────

/// Drive one recipient through the full retry loop.
///
/// Each call to `process_recipient` produces a `RecipientOutcome`; this
/// function decides whether to retry, back off, or give up, and keeps
/// looping until a terminal outcome is reached.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_one_recipient(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    cc_bcc: &Arc<EffectiveCcBcc>,
    cfg: &ConsumerConfig,
    shutdown: &CancellationToken,
) {
    // attempt is seeded from 0 on the first call; if the row already exists
    // (restart / re-delivery), process_recipient returns Duplicate { retry_count }
    // on the first iteration and we update attempt here — no separate DB query.
    let mut attempt: u32 = 0;
    let mut rl_count: u32 = 0;

    loop {
        match process_recipient(
            ctx,
            event,
            email_opts,
            recipient,
            attachments,
            cc_bcc,
            shutdown,
        )
        .await
        {
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
                // mark_failed with exhausted=false sets status to PENDING,
                // keeping the row recoverable.  The next iteration re-processes
                // after the backoff delay; on restart the Duplicate path in
                // process_recipient seeds attempt from the stored retry_count.
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, msg, false)
                    .await;
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        // Shutdown arrived during backoff.  The row is currently
                        // PENDING (exhausted=false above), but there is no AMQP
                        // message left to re-drive it after restart.  Flip it to
                        // FAILED (exhausted=true) so the operator can see it and
                        // replay via the retry API.
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
                // The `.min(10)` caps the *shift* (not `attempt` itself) so the
                // multiplier never exceeds 1024.  `attempt` may legitimately exceed
                // 10 when seeded from a high DB retry_count after a restart, but
                // the delay stays capped at this ceiling regardless.
                //
                // saturating_mul prevents silent u64 wrapping when an operator
                // configures an unusually large retry_base_ms — the product
                // saturates to u64::MAX and the subsequent MIN clamp brings it back
                // to MAX_RETRY_DELAY_MS, producing the correct 30-minute ceiling
                // rather than a wrapped near-zero delay.
                //
                // We additionally clamp to 30 minutes so that a large
                // retry_base_ms does not strand the un-ACK'd AMQP message beyond
                // any reasonable consumer timeout.  Operators who need longer hold
                // times should increase max_retries and keep retry_base_ms ≤ 2 000.
                const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1000; // 30 minutes
                let delay = Duration::from_millis(
                    cfg.retry_base_ms
                        .saturating_mul(1u64 << attempt.min(10))
                        .min(MAX_RETRY_DELAY_MS),
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

// ── Group send retry loop ─────────────────────────────────────────────────────

/// Drive a group send through the retry loop.
///
/// Mirrors `process_one_recipient` but calls `process_group` instead, which
/// builds one `EmailMessage` with all recipients sharing the `To:` header.
pub(crate) async fn process_one_group(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    attachments: &[ResolvedAttachment],
    cc_bcc: Arc<EffectiveCcBcc>,
    cfg: &ConsumerConfig,
    shutdown: &CancellationToken,
) {
    let mut attempt: u32 = 0;
    let mut rl_count: u32 = 0;

    loop {
        match process_group(
            ctx,
            event,
            email_opts,
            attachments,
            &cc_bcc,
            cfg.max_recipients_per_event,
            shutdown,
        )
        .await
        {
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
                    let cc_bcc = cc_bcc.clone();
                    join_set.spawn(async move {
                        process_one_recipient(
                            &ctx, &event, &opts, &recipient, &atts, &cc_bcc, &cfg, &shutdown,
                        )
                        .await;
                    });
                }
                let recipient_count = email_opts.recipients.len();
                let mut panics = 0usize;
                while let Some(result) = join_set.join_next().await {
                    if let Err(e) = result {
                        error!(
                            event_id = %event.event_id,
                            error    = %e,
                            "Individual-retry task panicked during group-send fallback"
                        );
                        panics += 1;
                    }
                }
                if panics > 0 {
                    error!(
                        event_id        = %event.event_id,
                        recipient_count,
                        panics,
                        "Group-send individual fallback complete — some tasks panicked"
                    );
                } else {
                    tracing::info!(
                        event_id        = %event.event_id,
                        recipient_count,
                        "Group-send individual fallback complete"
                    );
                }
                return;
            }

            RecipientOutcome::Failed(ref e) => {
                attempt += 1;
                rl_count = 0;
                // Same cap and saturating_mul as process_one_recipient — see comment there.
                const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1000; // 30 minutes
                let delay = Duration::from_millis(
                    cfg.retry_base_ms
                        .saturating_mul(1u64 << attempt.min(10))
                        .min(MAX_RETRY_DELAY_MS),
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
