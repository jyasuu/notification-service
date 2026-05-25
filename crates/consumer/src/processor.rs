use std::sync::Arc;

use common::{
    is_valid_email, AppError, FromOverride, GroupRetryMode, MailerKind, NotificationEvent,
    NotificationStatus, Recipient,
};
use mailer::message::ResolvedAttachment;
use mailer::{
    render_html_template, render_template, EmailMessage, EmailSender, MailboxRef, SenderRegistry,
};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use store::{
    EmailInsertPendingArgs, InsertResult, NotificationStore, TemplateStore, CHANNEL_EMAIL,
};
use tracing::{error, info, instrument, warn};

/// Pre-filtered CC and BCC recipient lists, computed **once per event** before
/// per-recipient tasks are spawned.
///
/// CC/BCC are event-level options shared across every TO recipient.  Computing
/// the filter result once in `delivery.rs` and passing it in here prevents
/// N×M filter evaluations (once per TO × per CC/BCC address) and multiplied
/// log noise for events with many recipients.
#[derive(Debug, Clone)]
pub struct EffectiveCcBcc {
    /// CC recipients that passed the filter (invalid/blocked addresses removed).
    pub cc: Vec<common::Recipient>,
    /// BCC recipients that passed the filter.
    pub bcc: Vec<common::Recipient>,
}

/// Shared, cheaply-cloneable context passed to every per-recipient processor call.
#[derive(Clone)]
pub struct ProcessorContext {
    pub store: Arc<dyn NotificationStore>,
    pub template_store: TemplateStore,
    /// Global default sender (SMTP or webhook) used when no named account matches.
    pub sender: Arc<dyn EmailSender>,
    /// Registry of named per-business-system SMTP accounts.
    pub sender_registry: SenderRegistry,
    pub filter: RecipientFilter,
    pub rate_limiter: MailRateLimiter,
}

/// Result of processing one recipient within an event.
#[derive(Debug)]
pub enum RecipientOutcome {
    Sent,
    Blocked(String),
    /// Recipient is already in a terminal state (SENT or BLOCKED) — skip.
    Skipped,
    /// Row already exists and is non-terminal; carries the current DB
    /// `retry_count` so the runner can seed its in-memory attempt counter
    /// without a second round-trip.
    Duplicate {
        retry_count: i32,
    },
    Failed(AppError),
    /// Group send failed after individual `notification_log` rows were already written
    /// for every recipient (`group_retry_mode = Individual`).  The runner
    /// should fall back to the individual-send path so only unsent recipients
    /// are retried, rather than re-sending the whole group email.
    GroupFailedWithIndividualRows(AppError),
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Process a single recipient for an event (individual send mode).
///
/// `email_opts` is the resolved `EmailOptions` extracted from the event's
/// `channel_overrides.email`. The caller (runner) is responsible for
/// unwrapping and validating that email options are present before calling
/// this function.
///
/// `attachments` are pre-fetched at the event level (once for all recipients)
/// and passed in as resolved bytes. This avoids re-fetching pre-signed URLs
/// for every recipient, which would waste bandwidth and risk URL expiry for
/// later recipients in the list.
#[instrument(skip(ctx, event, email_opts, recipient, attachments, cc_bcc, shutdown),
             fields(event_id = %event.event_id, email = %recipient.email))]
pub async fn process_recipient(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    cc_bcc: &EffectiveCcBcc,
    shutdown: &tokio_util::sync::CancellationToken,
) -> RecipientOutcome {
    // ── 0. Recipient email validation (before DB write) ─────────────────────
    if !is_valid_email(&recipient.email) {
        return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
            "invalid recipient email address: {}",
            recipient.email
        )));
    }

    // ── 1. Template lookup (before DB write) ────────────────────────────────
    let prefetched_template = match ctx
        .template_store
        .resolve(&event.event_type, CHANNEL_EMAIL)
        .await
    {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. from_override validation (before DB write) ───────────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(email_opts.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            let msg = format!("invalid from_override email address: {addr}");
            return RecipientOutcome::Failed(AppError::permanent_mailer(msg));
        }
    }

    // ── 2b. CC / BCC ──────────────────────────────────────────────────────────
    // Validation and filtering were done once at the delivery level in
    // `delivery.rs` before per-recipient tasks were spawned.  Use the
    // pre-filtered lists directly — avoids N×M filter evaluations and log
    // noise for events with many TO recipients.
    let effective_cc = &cc_bcc.cc;
    let effective_bcc = &cc_bcc.bcc;

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    // Use map_err + ? rather than .ok() so that a serialization failure
    // surfaces as a permanent error instead of silently storing NULL and
    // losing the from_override / attachments / cc / bcc in the DB row.
    // In practice serde_json::to_value never fails on these well-typed structs,
    // but making failures loud protects against future field changes.
    let from_override_json = match email_opts
        .from_override
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| AppError::permanent_mailer(format!("failed to serialize from_override: {e}")))
    {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let attachments_json = if email_opts.attachments.is_empty() {
        None
    } else {
        match serde_json::to_value(&email_opts.attachments).map_err(|e| {
            AppError::permanent_mailer(format!("failed to serialize attachments: {e}"))
        }) {
            Ok(v) => Some(v),
            Err(e) => return RecipientOutcome::Failed(e),
        }
    };

    // Serialize the post-filter (effective) CC/BCC lists so the DB record
    // accurately reflects what was actually delivered, not the raw unfiltered
    // input.  Storing pre-filter lists would show addresses that were never
    // delivered to, and would waste a filter cycle on every retry.
    let cc_json = match serialize_recipient_list(effective_cc, "cc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let bcc_json = match serialize_recipient_list(effective_bcc, "bcc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    match ctx
        .store
        .insert_pending(&EmailInsertPendingArgs {
            event_id: event.event_id,
            event_type: &event.event_type,
            recipient_email: &recipient.email,
            recipient_name: recipient.name.as_deref(),
            payload: &event.payload,
            from_override: from_override_json.as_ref(),
            attachments: attachments_json.as_ref(),
            sender_account: email_opts.sender_account.as_deref(),
            cc: cc_json.as_ref(),
            bcc: bcc_json.as_ref(),
            send_mode: email_opts.send_mode.as_str(),
            group_retry_mode: None, // individual mode — group_retry_mode is not applicable
            event_timestamp: event.timestamp,
        })
        .await
    {
        Ok(InsertResult::Inserted) => {}
        Ok(InsertResult::Duplicate {
            retry_count,
            status,
        }) => match NotificationStatus::try_from(status.as_str()) {
            Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked) => {
                info!("Skipping already-terminal recipient");
                return RecipientOutcome::Skipped;
            }
            Ok(_) => return RecipientOutcome::Duplicate { retry_count },
            Err(e) => return RecipientOutcome::Failed(e),
        },
        Err(e) => return RecipientOutcome::Failed(e),
    }

    // ── 4. Recipient filter ───────────────────────────────────────────────────
    if let Err(AppError::Blocked(reason)) = ctx.filter.check(&recipient.email) {
        warn!(reason = %reason, "Recipient blocked — dropping");
        let _ = ctx
            .store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // ── 5. Template rendering ────────────────────────────────────────────────
    // Render all three components and collect every error before returning.
    // The original code surfaced only the first failure in the tuple match,
    // silently discarding the second and third errors.  Collecting all errors
    // gives operators a complete picture when triaging a broken template.
    let subject_result = render_template(&prefetched_template.subject, &event.payload);
    let html_result = render_html_template(&prefetched_template.body_html, &event.payload);
    let text_result = render_template(&prefetched_template.body_text, &event.payload);

    let (subject, body_html, body_text) = match (subject_result, html_result, text_result) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (sr, hr, tr) => {
            // Log every component that failed, then return the first error.
            // The original tuple-match (Err(e), _, _) | (_, Err(e), _) | ...
            // silently discarded the second and third failures.
            if let Err(ref e) = sr {
                tracing::warn!(component = "subject",   error = %e, "Template render failed");
            }
            if let Err(ref e) = hr {
                tracing::warn!(component = "body_html", error = %e, "Template render failed");
            }
            if let Err(ref e) = tr {
                tracing::warn!(component = "body_text", error = %e, "Template render failed");
            }
            let first_err = sr
                .err()
                .or(hr.err())
                .or(tr.err())
                .expect("at least one Err");
            return RecipientOutcome::Failed(first_err);
        }
    };

    let msg = EmailMessage {
        event_id: event.event_id,
        to_email: recipient.email.clone(),
        to_name: recipient.name.clone(),
        to_extra: vec![], // individual mode: one To: address only
        subject,
        body_html,
        body_text,
        from_email_override,
        from_name_override,
        attachments: attachments.to_vec(),
        cc: effective_cc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
        bcc: effective_bcc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    // Only increment the counter when we had to actually wait — i.e. the
    // service is being throttled.  Incrementing unconditionally (before the
    // call) inflated the metric even when a token was immediately available,
    // making it useless as a "we are being throttled" alert signal.
    match ctx.rate_limiter.wait_for_token(shutdown).await {
        rate_limiter::TokenResult::Acquired => {}
        rate_limiter::TokenResult::AcquiredAfterWait => {
            counter!("email_rate_limit_waits_total",
                "event_type" => event.event_type.clone())
            .increment(1);
        }
        rate_limiter::TokenResult::Shutdown => {
            return RecipientOutcome::Failed(AppError::Queue(
                "service shutdown during rate-limit wait".into(),
            ));
        }
    }

    // ── 7. Send ───────────────────────────────────────────────────────────────
    let sender = ctx
        .sender_registry
        .resolve(email_opts.sender_account.as_deref())
        .unwrap_or_else(|| Arc::clone(&ctx.sender));

    let send_start = std::time::Instant::now();
    match sender.send(&msg).await {
        Ok(()) => {
            let elapsed = send_start.elapsed().as_secs_f64();
            // IMPORTANT: mark_sent failure after a successful SMTP send means
            // the email was delivered but the row stays PENDING.  On AMQP
            // re-delivery the idempotency check will see PENDING (Duplicate)
            // and re-send, producing a duplicate.  Log at WARN so the operator
            // can inspect and manually mark the row SENT if needed.
            if let Err(e) = ctx.store.mark_sent(event.event_id, &recipient.email).await {
                warn!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    error    = %e,
                    "Email delivered but mark_sent DB write failed — \
                     row remains PENDING; re-delivery will attempt to re-send"
                );
                counter!("email_mark_sent_failed_total",
                    "event_type" => event.event_type.clone())
                .increment(1);
            }
            counter!("emails_sent_total",
                "event_type" => event.event_type.clone())
            .increment(1);
            histogram!("email_send_duration_seconds",
                "event_type" => event.event_type.clone())
            .record(elapsed);
            info!("Email delivered");
            RecipientOutcome::Sent
        }
        Err(e) => {
            counter!("emails_failed_total",
                "event_type" => event.event_type.clone(),
                "reason"     => error_reason_label(&e)
            )
            .increment(1);
            warn!(error = %e, "Send failed");
            RecipientOutcome::Failed(e)
        }
    }
}

/// Serialize a non-empty recipient slice to a JSON value for storage in `notification_log`.
///
/// Returns `None` for an empty slice (stored as SQL NULL), or `Some(Value)`
/// otherwise. Errors surface as `AppError::permanent_mailer` so callers can
/// propagate them without a second DB write.
///
/// Used by both `process_recipient` and `process_group` to serialize the
/// effective (post-filter) CC and BCC recipient lists. The `field` string
/// ("cc" / "bcc") is included in error messages to aid diagnostics.
fn serialize_recipient_list(
    list: &[Recipient],
    field: &str,
) -> Result<Option<serde_json::Value>, AppError> {
    if list.is_empty() {
        return Ok(None);
    }
    serde_json::to_value(
        list.iter()
            .map(|r| serde_json::json!({"email": r.email, "name": r.name}))
            .collect::<Vec<_>>(),
    )
    .map(Some)
    .map_err(|e| AppError::permanent_mailer(format!("failed to serialize {field}: {e}")))
}

/// Maximum number of recipients allowed in a single group send.
///
/// This is a defence-in-depth guard inside the processor itself.  The primary
/// enforcement happens in `runner.rs` (`max_recipients_per_event`) before
/// `process_group` is called, but having the check here ensures the limit is
/// respected regardless of which call-site invokes this function in the future.
///
/// The value intentionally mirrors the runner's default (500) but is not
/// read from config — it is a hard ceiling baked into the function contract.
/// If the runner's configured limit is lower (the common case) the runner
/// guard fires first and this one is never reached.
pub const MAX_GROUP_RECIPIENTS: usize = 500;

/// Process all recipients as a single group email (group send mode).
///
/// All addresses in `email_opts.recipients` appear together in the `To:`
/// header of one email.
///
/// ## Idempotency / retry behaviour
///
/// The strategy depends on `email_opts.group_retry_mode`:
///
/// **`GroupRetryMode::Whole`** (default) — only the primary (first) recipient
/// gets a `notification_log` row.  On retry the whole group email is re-sent as a
/// unit.  Simple, but if SMTP accepted the message for some recipients before
/// the connection dropped, those recipients may receive the email twice.
///
/// **`GroupRetryMode::Individual`** — a `notification_log` row is inserted for
/// **every** recipient before the send attempt.  On failure the function
/// returns `RecipientOutcome::GroupFailedWithIndividualRows` so the runner
/// can fall back to `process_one_recipient` for each address, skipping those
/// that already have a `SENT` row.  Retried recipients receive a separate
/// email (the `To:` header shows only their own address); the shared-`To:`
/// visibility of the original group email is not preserved on retry.
#[instrument(skip(ctx, event, email_opts, attachments, shutdown),
             fields(event_id = %event.event_id, recipient_count = email_opts.recipients.len()))]
pub async fn process_group(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    attachments: &[ResolvedAttachment],
    shutdown: &tokio_util::sync::CancellationToken,
) -> RecipientOutcome {
    let recipients = &email_opts.recipients;

    // The primary recipient is used for notification_log tracking. We take the first
    // address; the rest go into to_extra on the EmailMessage.
    let primary = match recipients.first() {
        Some(r) => r,
        None => {
            return RecipientOutcome::Failed(AppError::permanent_mailer(
                "group send: recipients list is empty",
            ));
        }
    };

    // ── 0a. Recipient count guard (defence-in-depth) ─────────────────────────
    // The runner enforces `max_recipients_per_event` before calling this
    // function, so this path is normally unreachable.  The check is duplicated
    // here so that any future call-site that bypasses the runner guard (e.g. a
    // test harness, a new code path) still hits a hard ceiling before any
    // allocations, DB writes, or network calls are made.
    if recipients.len() > MAX_GROUP_RECIPIENTS {
        return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
            "group send: recipient count {} exceeds maximum allowed ({})",
            recipients.len(),
            MAX_GROUP_RECIPIENTS,
        )));
    }

    // ── 0b. Validate all To: addresses ───────────────────────────────────────
    for r in recipients {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid recipient email address: {}",
                r.email
            )));
        }
    }

    // ── 1. Template lookup ───────────────────────────────────────────────────
    let prefetched_template = match ctx
        .template_store
        .resolve(&event.event_type, CHANNEL_EMAIL)
        .await
    {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. from_override + cc/bcc validation and filter check ────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(email_opts.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid from_override email address: {addr}"
            )));
        }
    }
    // Invalid CC/BCC addresses are a permanent failure.
    // Blocked addresses are excluded and logged at WARN level; delivery
    // continues for the remaining CC/BCC recipients and all allowed TO
    // recipients.  Same semantics as process_recipient.  Blocked CC/BCC do
    // NOT get notification_log rows; consult structured logs
    // (email=<addr>, message="CC address blocked") for the audit trail.
    for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid cc/bcc email address: {}",
                r.email
            )));
        }
    }
    let effective_cc: Vec<Recipient> = email_opts
        .cc
        .iter()
        .filter(|r| match ctx.filter.check(&r.email) {
            Ok(()) => true,
            Err(AppError::Blocked(ref reason)) => {
                warn!(
                    event_id = %event.event_id,
                    email    = %r.email,
                    reason   = %reason,
                    "CC address blocked by filter — excluding from group delivery"
                );
                false
            }
            // Unexpected non-Blocked errors are treated as pass-through (fail-open):
            // an unknown filter error should never silently drop a CC recipient.
            Err(e) => {
                error!(
                    event_id = %event.event_id,
                    email    = %r.email,
                    error    = %e,
                    "Unexpected (non-Blocked) error from recipient filter for CC address — \
                     passing through (fail-open). Investigate filter health."
                );
                true
            }
        })
        .cloned()
        .collect();
    let effective_bcc: Vec<Recipient> = email_opts
        .bcc
        .iter()
        .filter(|r| match ctx.filter.check(&r.email) {
            Ok(()) => true,
            Err(AppError::Blocked(ref reason)) => {
                warn!(
                    event_id = %event.event_id,
                    email    = %r.email,
                    reason   = %reason,
                    "BCC address blocked by filter — excluding from group delivery"
                );
                false
            }
            // Unexpected non-Blocked errors are treated as pass-through (fail-open):
            // an unknown filter error should never silently drop a BCC recipient.
            Err(e) => {
                error!(
                    event_id = %event.event_id,
                    email    = %r.email,
                    error    = %e,
                    "Unexpected (non-Blocked) error from recipient filter for BCC address — \
                     passing through (fail-open). Investigate filter health."
                );
                true
            }
        })
        .cloned()
        .collect();

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    // Use map_err + ? for the same reason as in process_recipient: failures
    // should be loud, not silently stored as NULL.
    let from_override_json = match email_opts
        .from_override
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| AppError::permanent_mailer(format!("failed to serialize from_override: {e}")))
    {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let attachments_json = if email_opts.attachments.is_empty() {
        None
    } else {
        match serde_json::to_value(&email_opts.attachments).map_err(|e| {
            AppError::permanent_mailer(format!("failed to serialize attachments: {e}"))
        }) {
            Ok(v) => Some(v),
            Err(e) => return RecipientOutcome::Failed(e),
        }
    };
    // Serialize the post-filter (effective) CC/BCC lists so the DB record
    // accurately reflects what was actually delivered, not the raw unfiltered
    // input.  Storing pre-filter lists would show addresses that were never
    // delivered to, and would waste a filter cycle on every retry.
    let cc_json = match serialize_recipient_list(&effective_cc, "cc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let bcc_json = match serialize_recipient_list(&effective_bcc, "bcc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let group_retry_mode_str = match email_opts.group_retry_mode {
        GroupRetryMode::Whole => "whole",
        GroupRetryMode::Individual => "individual",
    };

    // Helper function to build EmailInsertPendingArgs for a given recipient.
    #[allow(clippy::too_many_arguments)]
    fn make_args<'a>(
        r: &'a Recipient,
        event: &'a NotificationEvent,
        email_opts: &'a common::EmailOptions,
        from_override_json: Option<&'a serde_json::Value>,
        attachments_json: Option<&'a serde_json::Value>,
        cc_json: Option<&'a serde_json::Value>,
        bcc_json: Option<&'a serde_json::Value>,
        group_retry_mode_str: &'a str,
    ) -> EmailInsertPendingArgs<'a> {
        EmailInsertPendingArgs {
            event_id: event.event_id,
            event_type: &event.event_type,
            recipient_email: &r.email,
            recipient_name: r.name.as_deref(),
            payload: &event.payload,
            from_override: from_override_json,
            attachments: attachments_json,
            sender_account: email_opts.sender_account.as_deref(),
            cc: cc_json,
            bcc: bcc_json,
            send_mode: email_opts.send_mode.as_str(),
            group_retry_mode: Some(group_retry_mode_str),
            event_timestamp: event.timestamp,
        }
    }

    // Always insert the primary row first.
    let primary_insert = match ctx
        .store
        .insert_pending(&make_args(
            primary,
            event,
            email_opts,
            from_override_json.as_ref(),
            attachments_json.as_ref(),
            cc_json.as_ref(),
            bcc_json.as_ref(),
            group_retry_mode_str,
        ))
        .await
    {
        Ok(r) => r,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    match primary_insert {
        InsertResult::Duplicate {
            retry_count,
            ref status,
        } => match NotificationStatus::try_from(status.as_str()) {
            Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked) => {
                info!("Group send: skipping already-terminal event");
                return RecipientOutcome::Skipped;
            }
            Ok(_) => {
                return RecipientOutcome::Duplicate { retry_count };
            }
            Err(e) => return RecipientOutcome::Failed(e),
        },
        InsertResult::Inserted => {}
    }

    // For GroupRetryMode::Individual, eagerly insert rows for every secondary recipient.
    if email_opts.group_retry_mode == GroupRetryMode::Individual {
        for r in recipients.iter().skip(1) {
            if let Err(e) = ctx
                .store
                .insert_pending(&make_args(
                    r,
                    event,
                    email_opts,
                    from_override_json.as_ref(),
                    attachments_json.as_ref(),
                    cc_json.as_ref(),
                    bcc_json.as_ref(),
                    group_retry_mode_str,
                ))
                .await
            {
                return RecipientOutcome::Failed(e);
            }
        }
    }

    // ── 4. Recipient filter — partition To: addresses into allowed / blocked ───
    // Design note — TO vs CC/BCC asymmetry:
    //   • Blocked TO recipients are excluded from the delivery; if *all* TO
    //     addresses are blocked the entire group send is dropped.  If only *some*
    //     are blocked, the mail is delivered to the remaining (allowed) addresses
    //     and the blocked ones are marked BLOCKED in the DB.
    //   • Blocked CC/BCC addresses (step 2 above) are silently excluded and
    //     delivery always continues — silencing a whole delivery for one unwanted
    //     copy recipient would be disproportionate.
    //   Operators: to recover from an all-blocked drop, remove the blocked TO
    //   addresses from the event payload and re-publish, or update the blocklist.
    let mut allowed_recipients: Vec<&Recipient> = Vec::with_capacity(recipients.len());
    for r in recipients {
        match ctx.filter.check(&r.email) {
            Ok(()) => allowed_recipients.push(r),
            Err(AppError::Blocked(ref reason)) => {
                warn!(
                    blocked_email = %r.email,
                    reason = %reason,
                    recipient_count = recipients.len(),
                    "Group send: TO recipient blocked — excluding from delivery"
                );
                let _ = ctx
                    .store
                    .mark_blocked(event.event_id, &r.email, reason)
                    .await;
                counter!("emails_blocked_total", "event_type" => event.event_type.clone())
                    .increment(1);
            }
            // Non-Blocked filter errors are treated as pass-through (fail-open)
            // so an unexpected filter error never silently drops a recipient.
            Err(_) => allowed_recipients.push(r),
        }
    }

    // If every TO address was blocked there is nothing left to send.
    if allowed_recipients.is_empty() {
        // The primary row was already marked BLOCKED in the loop above.
        // For Individual mode, all other rows were also marked there.
        warn!(
            event_id = %event.event_id,
            "Group send: all TO recipients blocked — dropping delivery"
        );
        return RecipientOutcome::Blocked("all TO recipients blocked by filter".into());
    }

    // Shadow `recipients` so the rest of the function operates only on the
    // allowed subset — template rendering, EmailMessage construction, DB
    // mark_sent calls, and the `to_count` log field all stay consistent.
    let recipients = allowed_recipients;

    // ── 5. Template rendering ────────────────────────────────────────────────
    // Render all three components and collect every error before returning.
    // The original code surfaced only the first failure in the tuple match,
    // silently discarding the second and third errors.  Collecting all errors
    // gives operators a complete picture when triaging a broken template.
    let subject_result = render_template(&prefetched_template.subject, &event.payload);
    let html_result = render_html_template(&prefetched_template.body_html, &event.payload);
    let text_result = render_template(&prefetched_template.body_text, &event.payload);

    let (subject, body_html, body_text) = match (subject_result, html_result, text_result) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (sr, hr, tr) => {
            // Log every component that failed, then return the first error.
            // The original tuple-match (Err(e), _, _) | (_, Err(e), _) | ...
            // silently discarded the second and third failures.
            if let Err(ref e) = sr {
                tracing::warn!(component = "subject",   error = %e, "Template render failed");
            }
            if let Err(ref e) = hr {
                tracing::warn!(component = "body_html", error = %e, "Template render failed");
            }
            if let Err(ref e) = tr {
                tracing::warn!(component = "body_text", error = %e, "Template render failed");
            }
            let first_err = sr
                .err()
                .or(hr.err())
                .or(tr.err())
                .expect("at least one Err");
            return RecipientOutcome::Failed(first_err);
        }
    };

    let to_extra: Vec<MailboxRef> = recipients
        .iter()
        .skip(1)
        .map(|r| MailboxRef {
            email: r.email.clone(),
            name: r.name.clone(),
        })
        .collect();

    let msg = EmailMessage {
        event_id: event.event_id,
        to_email: primary.email.clone(),
        to_name: primary.name.clone(),
        to_extra,
        subject,
        body_html,
        body_text,
        from_email_override,
        from_name_override,
        attachments: attachments.to_vec(),
        cc: effective_cc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
        bcc: effective_bcc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    // Only increment the counter when we had to actually wait — i.e. the
    // service is being throttled.  Incrementing unconditionally (before the
    // call) inflated the metric even when a token was immediately available,
    // making it useless as a "we are being throttled" alert signal.
    match ctx.rate_limiter.wait_for_token(shutdown).await {
        rate_limiter::TokenResult::Acquired => {}
        rate_limiter::TokenResult::AcquiredAfterWait => {
            counter!("email_rate_limit_waits_total",
                "event_type" => event.event_type.clone())
            .increment(1);
        }
        rate_limiter::TokenResult::Shutdown => {
            return RecipientOutcome::Failed(AppError::Queue(
                "service shutdown during rate-limit wait".into(),
            ));
        }
    }

    // ── 7. Send ───────────────────────────────────────────────────────────────
    let sender = ctx
        .sender_registry
        .resolve(email_opts.sender_account.as_deref())
        .unwrap_or_else(|| Arc::clone(&ctx.sender));

    let send_start = std::time::Instant::now();
    match sender.send(&msg).await {
        Ok(()) => {
            let elapsed = send_start.elapsed().as_secs_f64();
            // IMPORTANT: mark_sent failure after a successful SMTP send means
            // the email was delivered but the row stays PENDING.  On AMQP
            // re-delivery the idempotency check will see PENDING (Duplicate)
            // and re-send, producing a duplicate.  Log at WARN so the operator
            // can inspect and manually mark the row SENT if needed.
            if let Err(e) = ctx.store.mark_sent(event.event_id, &primary.email).await {
                warn!(
                    event_id = %event.event_id,
                    email    = %primary.email,
                    error    = %e,
                    "Group email delivered but mark_sent DB write failed for primary — \
                     row remains PENDING; re-delivery will attempt to re-send"
                );
                counter!("email_mark_sent_failed_total",
                    "event_type" => event.event_type.clone())
                .increment(1);
            }
            // For GroupRetryMode::Individual, also mark every secondary row SENT.
            if email_opts.group_retry_mode == GroupRetryMode::Individual {
                for r in recipients.iter().skip(1) {
                    if let Err(e) = ctx.store.mark_sent(event.event_id, &r.email).await {
                        warn!(
                            event_id = %event.event_id,
                            email    = %r.email,
                            error    = %e,
                            "Group email delivered but mark_sent DB write failed for secondary recipient — \
                             row remains PENDING; re-delivery will attempt to re-send"
                        );
                        counter!("email_mark_sent_failed_total",
                            "event_type" => event.event_type.clone())
                        .increment(1);
                    }
                }
            }
            counter!("emails_sent_total",
                "event_type" => event.event_type.clone())
            .increment(1);
            histogram!("email_send_duration_seconds",
                "event_type" => event.event_type.clone())
            .record(elapsed);
            info!(to_count = recipients.len(), "Group email delivered");
            RecipientOutcome::Sent
        }
        Err(e) => {
            counter!("emails_failed_total",
                "event_type" => event.event_type.clone(),
                "reason"     => error_reason_label(&e)
            )
            .increment(1);
            warn!(error = %e, "Group send failed");
            match email_opts.group_retry_mode {
                GroupRetryMode::Individual => RecipientOutcome::GroupFailedWithIndividualRows(e),
                GroupRetryMode::Whole => RecipientOutcome::Failed(e),
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn resolve_from_override(ov: Option<&FromOverride>) -> (Option<String>, Option<String>) {
    match ov {
        None => (None, None),
        Some(o) => (Some(o.email.clone()), o.name.clone()),
    }
}

pub fn is_retryable(err: &AppError) -> bool {
    match err {
        AppError::Duplicate(_)
        | AppError::NotFound(_)
        | AppError::Template(_)
        | AppError::Blocked(_) => false,
        // UnknownStatus is a data-integrity error: the DB row has a status value
        // this binary doesn't recognise.  Retrying will hit the same row and return
        // the same unrecognised status indefinitely, burning retry budget and
        // generating log noise.  Treat it as a permanent failure so it goes to DLQ
        // where an operator can investigate.
        AppError::UnknownStatus(_) => false,
        _ if err.is_permanent_mailer() => false,
        _ => true,
    }
}

fn error_reason_label(err: &AppError) -> &'static str {
    match err {
        AppError::RateLimited(_) => "rate_limited",
        AppError::Mailer {
            kind: MailerKind::Permanent,
            ..
        } => "permanent",
        AppError::Mailer { .. } => "transient",
        AppError::Database(_) => "database",
        AppError::Template(_) => "template",
        AppError::Queue(_) => "queue",
        AppError::NotFound(_) => "not_found",
        AppError::Deserialize(_) => "deserialize",
        AppError::UnknownStatus(_) => "unknown_status",
        _ => "other",
    }
}
