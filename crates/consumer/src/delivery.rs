//! Per-delivery handler and per-recipient / per-group retry loops.
//!
//! This module owns everything that happens after a single AMQP [`Delivery`]
//! is pulled off the wire:
//!
//! 1. **Deserialise** — decode the raw bytes into a [`NotificationEvent`],
//!    falling back to the legacy `EmailEvent` shape for older publishers.
//! 2. **Validate** — check for an email channel, non-empty recipients, and
//!    the configured `max_recipients_per_event` ceiling.
//! 3. **Fetch attachments** — resolve pre-signed URLs once for the whole event.
//! 4. **Filter CC / BCC** — validate and block-list-check copy addresses once
//!    per event (TO recipients are filtered later, per-recipient, in
//!    `process_recipient` steps 5–6).
//! 5. **Dispatch** — for each recipient spawn a task and run
//!    [`process_one_recipient`]; for group sends run [`process_one_group`].
//! 6. **ACK / NACK** — ACK once all recipient tasks finish; NACK to DLQ on
//!    unrecoverable event-level failures (bad JSON, expired attachments).
//!
//! The connection loop and AMQP topology setup live in `runner.rs`; the
//! per-recipient processor logic (idempotency, template rendering, send) lives
//! in `processor.rs`.  This module is the glue between them.
//!
//! ## TO vs CC/BCC filtering asymmetry
//!
//! CC/BCC addresses are validated and block-list-checked **once per event**
//! inside `handle_delivery` before any tasks are spawned.  A blocked copy
//! address is excluded with a WARN log; delivery to TO recipients continues
//! unaffected.
//!
//! TO recipients receive **no event-level filter**.  Their address validation
//! (step 0) and block-list checks (steps 5–6) run inside `process_recipient`,
//! *after* the idempotency INSERT.  A blocked TO address therefore consumes a
//! DB row and a task slot before being excluded.  This is intentional: the
//! idempotency row lets operators observe the blocked delivery via the status
//! API and keeps the retry-API path consistent for all recipient types.
//!
//! ## Mail delivery flow
//!
//! ```text
//! AMQP broker
//!  │
//!  │  Delivery (raw bytes)
//!  ▼
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ handle_delivery                                                      │
//! │                                                                      │
//! │  1. Deserialise ────────────────────────────────────────────────────► FAIL → NACK → DLQ
//! │       NotificationEvent  (or legacy EmailEvent fallback)             │
//! │                                                                      │
//! │  2a. No email channel_overrides? ──────────────────────────────────► mark_skipped → ACK
//! │  2b. recipients empty?           ──────────────────────────────────► mark_skipped → ACK
//! │  2c. recipients.len() > max_per_event? ────────────────────────────► mark_skipped → NACK → DLQ
//! │                                                                      │
//! │  3. Fetch attachments (once for all recipients) ────────────────────► FAIL permanent → NACK → DLQ
//! │                                                                      │    transient  → NACK → requeue
//! │  4. Filter CC / BCC (once per event)                                 │
//! │       invalid address    ──────────────────────────────────────────► WARN + exclude
//! │       config-file filter ──────────────────────────────────────────► WARN + exclude
//! │       DB block_list      ──────────────────────────────────────────► WARN + exclude
//! │       (delivery continues in all cases)                              │
//! │                                                                      │
//! │       ┌─ NOTE ──────────────────────────────────────────────────┐   │
//! │       │ TO recipients are NOT filtered here.  Their validity    │   │
//! │       │ and block-list checks run inside process_recipient      │   │
//! │       │ (steps 0, 5, 6) after the idempotency INSERT.           │   │
//! │       └─────────────────────────────────────────────────────────┘   │
//! │                                                                      │
//! │  5. Dispatch ──────────────────────────────────────────────────────► │
//! │       send_mode = Group      ──────────────────────────────────────► process_one_group
//! │       send_mode = Individual (default)                               │
//! │         └─ one task per recipient (parallel JoinSet) ──────────────► process_one_recipient
//! │                                                                      │
//! │  6. ACK (after all tasks complete)                                   │
//! └──────────────────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ process_one_recipient  (retry loop — individual mode)                │
//! │                                                                      │
//! │  Calls process_recipient() on each iteration → RecipientOutcome      │
//! │                                                                      │
//! │    Sent / Blocked / Skipped ──────────────────────────────────────► return (terminal)
//! │                                                                      │
//! │    Duplicate { retry_count } ─────────────────────────────────────► seed attempt from DB, retry immediately
//! │                                                                      │
//! │    Failed (permanent error)   ────────────────────────────────────► mark_failed(exhausted=true) → return
//! │    Failed (attempt ≥ max_retries) ────────────────────────────────► mark_failed(exhausted=true) → return
//! │    Failed (RetryPolicy::NoRetry) ─────────────────────────────────► mark_failed(exhausted=true) → return
//! │                                                                      │
//! │    Failed (RateLimited, rl_count ≤ max_rl_waits) ─────────────────► mark_failed(exhausted=false)
//! │      └─ sleep(30s × 2^rl_count, max 4h) ──────────────────────────► retry
//! │    Failed (RateLimited, rl_count > max_rl_waits) ─────────────────► mark_failed(exhausted=true) → return
//! │                                                                      │
//! │    Failed (transient) ─────────────────────────────────────────────► mark_failed(exhausted=false)
//! │      └─ sleep(retry_base_ms × 2^attempt, max 30 min) ─────────────► retry
//! │                                                                      │
//! │    GroupFailedWithIndividualRows ──────────────────────────────────► unexpected here
//! │                                                                      │   → mark_failed(exhausted=true) → return
//! │  Shutdown during any backoff ─────────────────────────────────────► mark_failed(exhausted=true) → return
//! └──────────────────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ process_one_group  (retry loop — group mode)                         │
//! │                                                                      │
//! │  Calls process_group() on each iteration (one EmailMessage, all      │
//! │  recipients share the To: header).  Outcomes mirror                  │
//! │  process_one_recipient except:                                       │
//! │                                                                      │
//! │    GroupFailedWithIndividualRows ─────────────────────────────────► spawn process_one_recipient
//! │      process_group already wrote a log row per recipient             │  per address (parallel JoinSet)
//! │      → fall back to individual sends for unsent addresses            │
//! │      → already-SENT rows skipped by idempotency guard in            │
//! │        process_recipient                                             │
//! │      → retried recipients receive individual (non-shared To:) email  │
//! │                                                                      │
//! │    Failed / RateLimited / transient / Shutdown ───────────────────► same as process_one_recipient
//! │      except mark_failed targets recipients[0] (the primary row)      │
//! └──────────────────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ process_recipient  (single attempt — called by both retry loops)     │
//! │                                                                      │
//! │   0. Validate recipient email address  ────────────────────────────► invalid → Blocked (permanent)
//! │   1. Resolve template (cached, TTL-based)                            │
//! │   2. Validate from_override address                                  │
//! │   3. Render subject / body_html / body_text                          │
//! │   4. Idempotency: INSERT PENDING  ─────────────────────────────────► ON CONFLICT → Duplicate { retry_count }
//! │   5. Config-file recipient filter  ────────────────────────────────► blocked → mark_blocked → Blocked
//! │   6. DB block_list_store check     ────────────────────────────────► blocked → mark_blocked → Blocked
//! │   7. Rate-limiter token (wait or Shutdown)                           │
//! │   8. Select sender (named account registry → global fallback)        │
//! │   9. sender.send(EmailMessage)                                       │
//! │  10. mark_sent / mark_failed / mark_blocked                          │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use std::time::Duration;

use common::{
    is_valid_email, AppError, GroupRetryMode, NotificationEvent, Recipient, RetryPolicy, SendMode,
};
use lapin::{message::Delivery, options::*};
use mailer::fetch_attachments_with_limit;
use mailer::message::ResolvedAttachment;
use reqwest::Client;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    config::ConsumerConfig,
    processor::{
        is_retryable, process_group, process_recipient, EffectiveCcBcc, ProcessorContext,
        RecipientOutcome,
    },
};

/// Maximum retry backoff delay, applied in both the individual and group retry
/// loops. Capped at 30 minutes to stay safely below RabbitMQ's default
/// consumer_timeout (also 30 min). The shift exponent is bounded at 10 so
/// the multiplier never exceeds 1024×; saturating_mul prevents wrapping on
/// large retry_base_ms values.
const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1000; // 30 minutes

// ── Public entry point ────────────────────────────────────────────────────────

/// Handle one AMQP delivery end-to-end.
///
/// **Attachments** are fetched once at the event level and the resolved bytes
/// are forwarded to every recipient task, preventing N×M HTTP round-trips and
/// pre-signed URL expiry for recipients processed later in the list.
///
/// **CC / BCC filtering** runs once here before dispatch.  Invalid or
/// block-listed copy addresses are excluded (WARN-logged) and delivery
/// continues.  TO recipients are *not* filtered at this stage; their validity
/// and block-list checks run inside [`process_recipient`] after the
/// idempotency INSERT (see module-level doc for the rationale).
///
/// **Recipients** are processed in parallel via a `JoinSet`: each TO recipient
/// gets its own task with its own retry loop so a slow or failing address does
/// not block others in the same event.
///
/// **ACK / NACK** — the AMQP message is ACK'd once all recipient tasks finish.
/// It is NACK'd (→ DLQ) only on unrecoverable event-level failures:
/// deserialization errors or permanent attachment-fetch failures.
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
    // If there are no email options the event has no work for this service to
    // do.  We write a SKIPPED row so operators can see the event in the status
    // API and diagnose the publisher bug, then ACK cleanly so the message is
    // not re-queued or sent to DLQ.
    //
    // Logged at ERROR (not WARN) because a missing email channel_overrides is
    // almost always a publisher bug — it should never happen in production and
    // must not go unnoticed.
    let email_opts = match event.channel_overrides.email.as_ref() {
        Some(opts) => opts.clone(),
        None => {
            error!(
                event_id   = %event.event_id,
                event_type = %event.event_type,
                "Event has no email channel_overrides — publisher bug? \
                 Writing SKIPPED row so the event is visible via the status API."
            );
            // Use the event_id string as the sentinel recipient_id so the row
            // is queryable via GET /emails/{event_id} without a real address.
            let _ = ctx
                .store
                .mark_skipped(
                    event.event_id,
                    &event.event_type,
                    &format!("event:{}", event.event_id),
                    "no email channel_overrides in event — publisher must re-publish with corrected data",
                    event.timestamp,
                    &event.payload,
                )
                .await;
            let _ = delivery.ack(BasicAckOptions::default()).await;
            return;
        }
    };

    if email_opts.recipients.is_empty() {
        error!(
            event_id   = %event.event_id,
            event_type = %event.event_type,
            "Event has no recipients — publisher bug? \
             Writing SKIPPED row so the event is visible via the status API."
        );
        let _ = ctx
            .store
            .mark_skipped(
                event.event_id,
                &event.event_type,
                &format!("event:{}", event.event_id),
                "email channel_overrides.recipients is empty — publisher must re-publish with at least one recipient",
                event.timestamp,
                &event.payload,
            )
            .await;
        let _ = delivery.ack(BasicAckOptions::default()).await;
        return;
    }

    // Guard against pathologically large recipient lists that would monopolise
    // the semaphore permit for an unbounded duration and exhaust DB connections.
    // Write a SKIPPED row with a sentinel recipient_id before NACKing so an
    // operator can diagnose the rejection via GET /emails/{event_id} rather
    // than seeing a 404 and wondering whether the event was ever received.
    if email_opts.recipients.len() > cfg.max_recipients_per_event {
        error!(
            event_id        = %event.event_id,
            event_type      = %event.event_type,
            recipient_count = email_opts.recipients.len(),
            limit           = cfg.max_recipients_per_event,
            "Event exceeds max_recipients_per_event — writing SKIPPED row and sending to DLQ"
        );
        let reason = format!(
            "recipient count {} exceeds max_recipients_per_event {} — \
             reduce the list or raise the limit before re-publishing",
            email_opts.recipients.len(),
            cfg.max_recipients_per_event,
        );
        let _ = ctx
            .store
            .mark_skipped(
                event.event_id,
                &event.event_type,
                &format!("event:{}", event.event_id),
                &reason,
                event.timestamp,
                &event.payload,
            )
            .await;
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
    // Invalid or blocked CC/BCC addresses are silently excluded — logged at
    // WARN level — and delivery continues.  This mirrors the block-list
    // behaviour for CC/BCC: a bad copy address is not a reason to abort a
    // delivery to valid TO recipients.  Contrast with TO recipients, where
    // an invalid address returns a permanent AppError::Mailer for that
    // recipient.
    //
    // This runs once here rather than inside each per-recipient task to avoid
    // N×M filter evaluations and log noise.
    let cc_bcc = {
        // Helper that checks both the static config filter and the DB-backed
        // block_list_store for a CC/BCC address.  Config-file rules win (checked
        // first); DB entries can be added/removed at runtime via the HTTP API.
        // Returns true (keep) or false (exclude), logging the block reason.
        // Fails open on non-Blocked errors from either check so an unexpected
        // filter error never silently drops a copy recipient.
        async fn cc_bcc_allowed(
            ctx: &ProcessorContext,
            event_id: Uuid,
            r: &Recipient,
            field: &str,
        ) -> bool {
            // 0. Basic address validity — strip rather than DLQ the event.
            if !is_valid_email(&r.email) {
                warn!(
                    event_id = %event_id,
                    email    = %r.email,
                    "{} address is invalid — excluding from delivery", field
                );
                return false;
            }
            // 1. Static config filter.
            match ctx.filter.check(&r.email) {
                Ok(()) => {}
                Err(AppError::Blocked(ref reason)) => {
                    warn!(
                        event_id = %event_id,
                        email    = %r.email,
                        reason   = %reason,
                        "{} address blocked by config filter — excluding from delivery", field
                    );
                    return false;
                }
                Err(_) => {} // fail-open
            }
            // 2. DB-backed block/allow-list (checked after config so config always wins).
            match ctx.block_list_store.check(&r.email).await {
                Ok(()) => true,
                Err(AppError::Blocked(ref reason)) => {
                    warn!(
                        event_id = %event_id,
                        email    = %r.email,
                        reason   = %reason,
                        "{} address blocked by DB block_list — excluding from delivery", field
                    );
                    false
                }
                Err(_) => true, // fail-open
            }
        }

        let mut cc: Vec<Recipient> = Vec::with_capacity(email_opts.cc.len());
        for r in &email_opts.cc {
            if cc_bcc_allowed(&ctx, event.event_id, r, "CC").await {
                cc.push(r.clone());
            }
        }
        let mut bcc: Vec<Recipient> = Vec::with_capacity(email_opts.bcc.len());
        for r in &email_opts.bcc {
            if cc_bcc_allowed(&ctx, event.event_id, r, "BCC").await {
                bcc.push(r.clone());
            }
        }
        Arc::new(EffectiveCcBcc { cc, bcc })
    };

    // ── Dispatch: group send or per-recipient individual sends ───────────────
    if email_opts.send_mode == SendMode::Group {
        // Warn when group_retry_mode = Whole (the default): if any recipient's
        // SMTP delivery succeeds in a partial attempt before the send fails,
        // that recipient will receive the email twice on retry.
        // Use group_retry_mode = Individual to avoid this — it writes one log
        // row per recipient so already-SENT addresses are skipped on retry.
        if email_opts.group_retry_mode == GroupRetryMode::Whole {
            warn!(
                event_id   = %event.event_id,
                event_type = %event.event_type,
                recipients = email_opts.recipients.len(),
                "Group send with group_retry_mode=whole: if this send partially \
                 succeeds and is retried, recipients who already received the \
                 email may receive it again. Consider setting \
                 group_retry_mode=individual to avoid double-sends on retry."
            );
        }
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

/// Drive one recipient through the retry loop (individual send mode).
///
/// Calls [`process_recipient`] on each iteration and acts on the returned
/// [`RecipientOutcome`]: terminal outcomes (`Sent`, `Blocked`, `Skipped`,
/// permanent failures) return immediately; transient failures sleep with
/// exponential backoff and retry; `RateLimited` outcomes use a separate
/// counter and a fixed-base backoff capped at `max_rl_waits`.
///
/// [`GroupFailedWithIndividualRows`](RecipientOutcome::GroupFailedWithIndividualRows)
/// is only emitted by the group path and is treated as an unexpected error here.
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
                let delay = Duration::from_secs(30 * (1u64 << rl_count.min(3)));
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

/// Drive a group send through the retry loop (group send mode).
///
/// Calls [`process_group`] on each iteration, which builds one
/// [`EmailMessage`](mailer::message::EmailMessage) with all recipients sharing
/// the `To:` header.  Retry logic mirrors [`process_one_recipient`] with one
/// additional outcome:
///
/// **[`GroupFailedWithIndividualRows`](RecipientOutcome::GroupFailedWithIndividualRows)**
/// — emitted when `process_group` writes a `notification_log` row per
/// recipient before the send attempt fails.  Re-sending the whole group email
/// would duplicate recipients already delivered to by the SMTP server.
/// Instead, this function falls back to [`process_one_recipient`] for each
/// address; recipients whose row is already `SENT` or `BLOCKED` are skipped
/// by the idempotency guard inside `process_recipient`.  Trade-off: retried
/// recipients receive an individual email whose `To:` header shows only their
/// own address.
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
                let delay = Duration::from_secs(30 * (1u64 << rl_count.min(3)));
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
