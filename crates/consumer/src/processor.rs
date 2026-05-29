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
    BlockListStore, EmailInsertPendingArgs, InsertResult, NotificationStore, TemplateStore,
    CHANNEL_EMAIL,
};
use tracing::{info, instrument, warn};

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
    /// Static config-file recipient filter (block/allow lists from config.toml).
    pub filter: RecipientFilter,
    /// DB-backed block/allow-list. Checked after `filter`; entries can be added
    /// or removed at runtime via the HTTP API without a service restart.
    pub block_list_store: BlockListStore,
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

    // ── 2c. Template rendering (before DB write) ──────────────────────────────
    // Rendering is done here, before the idempotency DB write, so that a
    // permanently broken template (bad Handlebars syntax) returns Failed without
    // ever creating a PENDING row.  Without this, a bad template creates a row
    // on the first attempt, returns Failed, and the retry loop burns through
    // max_retries on a Duplicate path before giving up — producing log noise and
    // wasting retry budget on an error that is not transient.
    let subject_result = render_template(&prefetched_template.subject, &event.payload);
    let html_result = render_html_template(&prefetched_template.body_html, &event.payload);
    let text_result = render_template(&prefetched_template.body_text, &event.payload);

    let (subject, body_html, body_text) = match (subject_result, html_result, text_result) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (sr, hr, tr) => {
            if let Err(ref e) = sr {
                tracing::warn!(component = "subject",   error = %e, "Template render failed");
            }
            if let Err(ref e) = hr {
                tracing::warn!(component = "body_html", error = %e, "Template render failed");
            }
            if let Err(ref e) = tr {
                tracing::warn!(component = "body_text", error = %e, "Template render failed");
            }
            let first_err = sr.err().or(hr.err()).or(tr.err()).unwrap_or_else(|| {
                unreachable!("match arm requires at least one Err among (sr, hr, tr)")
            });
            return RecipientOutcome::Failed(first_err);
        }
    };

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
            to_recipients: None,    // individual mode — every recipient has its own row
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

    // ── 4. Recipient filter (config-file) ────────────────────────────────────
    if let Err(AppError::Blocked(reason)) = ctx.filter.check(&recipient.email) {
        warn!(reason = %reason, "Recipient blocked by config filter — dropping");
        let _ = ctx
            .store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // ── 4b. DB-backed block/allow-list ────────────────────────────────────────
    // Checked after the static filter so config-file rules always win.
    // DB entries can be added/removed at runtime via the HTTP API.
    if let Err(AppError::Blocked(reason)) = ctx.block_list_store.check(&recipient.email).await {
        warn!(reason = %reason, "Recipient blocked by DB block_list — dropping");
        let _ = ctx
            .store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // (subject, body_html, body_text rendered above at step 2c, before the DB write)

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

    // ── 6 & 7. Rate-limit + send ─────────────────────────────────────────────
    execute_send(
        ctx,
        &msg,
        email_opts.sender_account.as_deref(),
        &event.event_type,
        shutdown,
        SendTargets::Individual {
            event_id: event.event_id,
            email: &recipient.email,
        },
    )
    .await
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
#[instrument(skip(ctx, event, email_opts, attachments, cc_bcc, shutdown),
             fields(event_id = %event.event_id, recipient_count = email_opts.recipients.len()))]
pub async fn process_group(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    attachments: &[ResolvedAttachment],
    cc_bcc: &EffectiveCcBcc,
    max_recipients: usize,
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
    // The primary enforcement happens in `delivery.rs` (`max_recipients_per_event`)
    // before `process_group` is called; the same limit is passed in here so both
    // layers always use the same configured value.  This check fires only if a
    // future call-site bypasses the outer guard (e.g. a test harness).
    if recipients.len() > max_recipients {
        return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
            "group send: recipient count {} exceeds maximum allowed ({})",
            recipients.len(),
            max_recipients,
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
    // CC/BCC validation and filtering were done once at the delivery level
    // (delivery.rs) before this function was called, for the same reason as in
    // process_recipient: avoids N×M filter evaluations and log noise.
    let effective_cc = &cc_bcc.cc;
    let effective_bcc = &cc_bcc.bcc;

    // ── 2c. Template rendering (before DB write) ──────────────────────────────────
    // Same rationale as process_recipient step 2c: fail before writing any DB row
    // so a permanently broken template does not burn retry budget.
    let subject_result = render_template(&prefetched_template.subject, &event.payload);
    let html_result = render_html_template(&prefetched_template.body_html, &event.payload);
    let text_result = render_template(&prefetched_template.body_text, &event.payload);

    let (subject, body_html, body_text) = match (subject_result, html_result, text_result) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (sr, hr, tr) => {
            if let Err(ref e) = sr {
                tracing::warn!(component = "subject",   error = %e, "Template render failed");
            }
            if let Err(ref e) = hr {
                tracing::warn!(component = "body_html", error = %e, "Template render failed");
            }
            if let Err(ref e) = tr {
                tracing::warn!(component = "body_text", error = %e, "Template render failed");
            }
            let first_err = sr.err().or(hr.err()).or(tr.err()).unwrap_or_else(|| {
                unreachable!("match arm requires at least one Err among (sr, hr, tr)")
            });
            return RecipientOutcome::Failed(first_err);
        }
    };

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
    let cc_json = match serialize_recipient_list(effective_cc, "cc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let bcc_json = match serialize_recipient_list(effective_bcc, "bcc") {
        Ok(v) => v,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let group_retry_mode_str = match email_opts.group_retry_mode {
        GroupRetryMode::Whole => "whole",
        GroupRetryMode::Individual => "individual",
    };

    // Shared (event-level) fields for every insert_pending call in this group send.
    // Bundled into a struct so the inner `fn` below takes two parameters instead
    // of eight, keeping clippy happy without the lifetime-inference limitation
    // that prevents closures from expressing higher-ranked lifetimes (for<'a>).
    struct SharedArgs<'a> {
        event: &'a NotificationEvent,
        email_opts: &'a common::EmailOptions,
        from_override_json: Option<&'a serde_json::Value>,
        attachments_json: Option<&'a serde_json::Value>,
        cc_json: Option<&'a serde_json::Value>,
        bcc_json: Option<&'a serde_json::Value>,
        group_retry_mode_str: &'a str,
        /// Serialized full To: list. Non-None only for GroupRetryMode::Whole so
        /// that the single primary row records who else received the email.
        to_recipients_json: Option<&'a serde_json::Value>,
    }
    fn make_args<'a>(r: &'a Recipient, s: &'a SharedArgs<'a>) -> EmailInsertPendingArgs<'a> {
        EmailInsertPendingArgs {
            event_id: s.event.event_id,
            event_type: &s.event.event_type,
            recipient_email: &r.email,
            recipient_name: r.name.as_deref(),
            payload: &s.event.payload,
            from_override: s.from_override_json,
            attachments: s.attachments_json,
            sender_account: s.email_opts.sender_account.as_deref(),
            cc: s.cc_json,
            bcc: s.bcc_json,
            send_mode: s.email_opts.send_mode.as_str(),
            group_retry_mode: Some(s.group_retry_mode_str),
            to_recipients: s.to_recipients_json,
            event_timestamp: s.event.timestamp,
        }
    }

    // For GroupRetryMode::Whole, serialize the full recipient list once so the
    // primary row records every address that received this group email.
    // For GroupRetryMode::Individual every recipient gets its own row, so
    // to_recipients is redundant and left NULL.
    let to_recipients_json: Option<serde_json::Value> =
        if email_opts.group_retry_mode == GroupRetryMode::Whole {
            match serde_json::to_value(&email_opts.recipients).map_err(|e| {
                AppError::permanent_mailer(format!("failed to serialize to_recipients: {e}"))
            }) {
                Ok(v) => Some(v),
                Err(e) => return RecipientOutcome::Failed(e),
            }
        } else {
            None
        };

    let shared = SharedArgs {
        event,
        email_opts,
        from_override_json: from_override_json.as_ref(),
        attachments_json: attachments_json.as_ref(),
        cc_json: cc_json.as_ref(),
        bcc_json: bcc_json.as_ref(),
        group_retry_mode_str,
        to_recipients_json: to_recipients_json.as_ref(),
    };

    // ── 3a. Idempotency inserts ───────────────────────────────────────────────
    //
    // For GroupRetryMode::Individual every recipient gets its own row so that
    // re-delivery after a partial send can skip already-SENT addresses.  All
    // rows are written in a single call so the set is written atomically:
    // either every row exists or none do.  A crash during the insert rolls
    // back the transaction; on re-delivery the batch is retried in full.
    //
    // For GroupRetryMode::Whole only the primary row is written (one row
    // tracks the whole group send as a unit).
    let rows_to_insert: Vec<_> = if email_opts.group_retry_mode == GroupRetryMode::Individual {
        recipients.iter().map(|r| make_args(r, &shared)).collect()
    } else {
        vec![make_args(primary, &shared)]
    };

    let insert_results = match ctx.store.insert_pending_batch(&rows_to_insert).await {
        Ok(r) => r,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // The primary row is always first.  Its result drives the terminal-state
    // check; secondary results are not inspected here because:
    //   • InsertResult::Inserted — new row, proceed normally.
    //   • InsertResult::Duplicate { Sent | Blocked } — already terminal; the
    //     filter step below (step 4) will mark them BLOCKED if re-filtered, or
    //     execute_send will find them SENT via the mark_sent no-op.
    //   • InsertResult::Duplicate { non-terminal } — treated as a retry for
    //     that recipient; execute_send will re-attempt delivery.
    let primary_insert = match insert_results.into_iter().next() {
        Some(r) => r,
        None => {
            return RecipientOutcome::Failed(AppError::permanent_mailer(
                "insert_pending_batch returned empty results",
            ))
        }
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
                if let Err(ref db_err) = ctx
                    .store
                    .mark_blocked(event.event_id, &r.email, reason)
                    .await
                {
                    warn!(
                        event_id = %event.event_id,
                        email    = %r.email,
                        error    = %db_err,
                        "Group send: mark_blocked DB write failed — row remains PENDING; \
                         operator should manually mark it BLOCKED or it will be retried"
                    );
                }
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

    // (subject, body_html, body_text rendered above at step 2c, before the DB write)

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

    // ── 6 & 7. Rate-limit + send ─────────────────────────────────────────────
    execute_send(
        ctx,
        &msg,
        email_opts.sender_account.as_deref(),
        &event.event_type,
        shutdown,
        SendTargets::Group {
            event_id: event.event_id,
            primary_email: &primary.email,
            secondaries: if email_opts.group_retry_mode == GroupRetryMode::Individual {
                recipients
                    .iter()
                    .skip(1)
                    .map(|r| r.email.as_str())
                    .collect()
            } else {
                vec![]
            },
            retry_mode: &email_opts.group_retry_mode,
            to_count: recipients.len(),
        },
    )
    .await
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Describes which `notification_log` rows to mark SENT after a successful send.
///
/// Passed to [`execute_send`] so the shared rate-limit + send + mark_sent
/// logic can handle both individual and group sends without duplicating code.
enum SendTargets<'a> {
    /// Individual send: exactly one row to mark SENT.
    Individual {
        event_id: uuid::Uuid,
        email: &'a str,
    },
    /// Group send: primary row always marked SENT; secondaries marked SENT only
    /// when `retry_mode` is `Individual` (i.e. `secondaries` is non-empty).
    Group {
        event_id: uuid::Uuid,
        primary_email: &'a str,
        /// Pre-filtered slice of secondary recipient emails to mark SENT.
        /// Empty for `GroupRetryMode::Whole`.
        secondaries: Vec<&'a str>,
        retry_mode: &'a GroupRetryMode,
        to_count: usize,
    },
}

/// Shared steps 6–7: rate-limit wait → sender selection → send → mark_sent → metrics.
///
/// Called by both [`process_recipient`] and [`process_group`] to eliminate the
/// duplicate rate-limit / send / mark_sent / metric blocks.  Each caller passes
/// the appropriate [`SendTargets`] variant so `mark_sent` and the log message
/// stay correct for both individual and group sends.
async fn execute_send(
    ctx: &ProcessorContext,
    msg: &mailer::EmailMessage,
    sender_account: Option<&str>,
    event_type: &str,
    shutdown: &tokio_util::sync::CancellationToken,
    targets: SendTargets<'_>,
) -> RecipientOutcome {
    // ── Rate-limit token ──────────────────────────────────────────────────────
    // Only increment the counter when we had to actually wait — i.e. the
    // service is being throttled.  Incrementing unconditionally inflated the
    // metric even when a token was immediately available, making it useless as
    // a "we are being throttled" alert signal.
    match ctx.rate_limiter.wait_for_token(shutdown).await {
        rate_limiter::TokenResult::Acquired => {}
        rate_limiter::TokenResult::AcquiredAfterWait => {
            counter!("email_rate_limit_waits_total",
                "event_type" => event_type.to_owned())
            .increment(1);
        }
        rate_limiter::TokenResult::Shutdown => {
            return RecipientOutcome::Failed(AppError::Queue(
                "service shutdown during rate-limit wait".into(),
            ));
        }
    }

    // ── Sender selection ──────────────────────────────────────────────────────
    let sender = match ctx.sender_registry.resolve(sender_account) {
        Some(s) => s,
        None => {
            if let Some(account) = sender_account {
                warn!(
                    account,
                    event_type,
                    "Named sender_account not found in registry — falling back to global sender. \
                     Check [sender_accounts.{account}] in config."
                );
            }
            Arc::clone(&ctx.sender)
        }
    };

    // ── Send ──────────────────────────────────────────────────────────────────
    let send_start = std::time::Instant::now();
    match sender.send(msg).await {
        Ok(()) => {
            let elapsed = send_start.elapsed().as_secs_f64();

            // ── mark_sent ─────────────────────────────────────────────────────
            // IMPORTANT: a mark_sent failure after a successful SMTP send means
            // the email was delivered but the row stays PENDING.  On AMQP
            // re-delivery the idempotency check will see PENDING (Duplicate) and
            // re-send, producing a duplicate.  Log at WARN so the operator can
            // inspect and manually mark the row SENT if needed.
            match &targets {
                SendTargets::Individual { event_id, email } => {
                    if let Err(e) = ctx.store.mark_sent(*event_id, email).await {
                        warn!(
                            event_id = %event_id,
                            email    = %email,
                            error    = %e,
                            "Email delivered but mark_sent DB write failed — \
                             row remains PENDING; re-delivery will attempt to re-send"
                        );
                        counter!("email_mark_sent_failed_total",
                            "event_type" => event_type.to_owned())
                        .increment(1);
                    }
                }
                SendTargets::Group {
                    event_id,
                    primary_email,
                    secondaries,
                    ..
                } => {
                    if let Err(e) = ctx.store.mark_sent(*event_id, primary_email).await {
                        warn!(
                            event_id = %event_id,
                            email    = %primary_email,
                            error    = %e,
                            "Group email delivered but mark_sent DB write failed for primary — \
                             row remains PENDING; re-delivery will attempt to re-send"
                        );
                        counter!("email_mark_sent_failed_total",
                            "event_type" => event_type.to_owned())
                        .increment(1);
                    }
                    // Mark secondary rows SENT for GroupRetryMode::Individual.
                    //
                    // IMPORTANT: this loop marks rows one by one after SMTP has
                    // already accepted the message.  If the process crashes mid-loop,
                    // some secondary rows remain PENDING.  On AMQP re-delivery the
                    // idempotency check sees PENDING (Duplicate) and re-sends,
                    // potentially causing duplicates.  This is the same trade-off as
                    // the primary mark_sent above; both are logged at WARN so operators
                    // can manually correct stuck rows.  Individual mode accepts this
                    // risk in exchange for per-recipient retry granularity on the first
                    // (pre-crash) attempt.
                    //
                    // `mark_sent_failures` tracks how many secondaries failed so we
                    // can emit a single event-level counter (`email_group_mark_sent_partial_total`)
                    // that is easy to alert on, in addition to the per-failure
                    // `email_mark_sent_failed_total` increments below.
                    let mut mark_sent_failures: usize = 0;
                    for email in secondaries {
                        if let Err(e) = ctx.store.mark_sent(*event_id, email).await {
                            warn!(
                                event_id = %event_id,
                                email    = %email,
                                error    = %e,
                                "Group email delivered but mark_sent DB write failed for secondary \
                                 recipient — row remains PENDING; re-delivery will attempt to re-send"
                            );
                            counter!("email_mark_sent_failed_total",
                                "event_type" => event_type.to_owned())
                            .increment(1);
                            mark_sent_failures += 1;
                        }
                    }
                    // Fire once per group send if at least one secondary failed.
                    // Alert on this metric to catch partial-completion events before
                    // they result in re-send duplicates on the next AMQP redelivery.
                    if mark_sent_failures > 0 {
                        counter!("email_group_mark_sent_partial_total",
                            "event_type" => event_type.to_owned())
                        .increment(1);
                    }
                }
            }

            counter!("emails_sent_total",
                "event_type" => event_type.to_owned())
            .increment(1);
            histogram!("email_send_duration_seconds",
                "event_type" => event_type.to_owned())
            .record(elapsed);

            match &targets {
                SendTargets::Individual { .. } => info!("Email delivered"),
                SendTargets::Group { to_count, .. } => {
                    info!(to_count = %to_count, "Group email delivered")
                }
            }

            RecipientOutcome::Sent
        }
        Err(e) => {
            counter!("emails_failed_total",
                "event_type" => event_type.to_owned(),
                "reason"     => error_reason_label(&e)
            )
            .increment(1);

            match &targets {
                SendTargets::Individual { .. } => warn!(error = %e, "Send failed"),
                SendTargets::Group { .. } => warn!(error = %e, "Group send failed"),
            }

            match targets {
                SendTargets::Individual { .. } => RecipientOutcome::Failed(e),
                SendTargets::Group { retry_mode, .. } => match retry_mode {
                    GroupRetryMode::Individual => {
                        RecipientOutcome::GroupFailedWithIndividualRows(e)
                    }
                    GroupRetryMode::Whole => RecipientOutcome::Failed(e),
                },
            }
        }
    }
}

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
