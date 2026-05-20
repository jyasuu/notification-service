use std::sync::Arc;

use common::{is_valid_email, AppError, FromOverride, MailerKind, NotificationEvent, Recipient};
use mailer::message::ResolvedAttachment;
use mailer::{
    render_html_template, render_template, EmailMessage, EmailSender, MailboxRef, SenderRegistry,
};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use store::{EmailLogStore, InsertPendingArgs, InsertResult, TemplateStore};
use tracing::{info, instrument, warn};

/// Shared, cheaply-cloneable context passed to every per-recipient processor call.
#[derive(Clone)]
pub struct ProcessorContext {
    pub store: EmailLogStore,
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
#[instrument(skip(ctx, event, email_opts, recipient, attachments, shutdown),
             fields(event_id = %event.event_id, email = %recipient.email))]
pub async fn process_recipient(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
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
    let prefetched_template = match ctx.template_store.resolve(&event.event_type).await {
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

    // ── 2b. CC / BCC address validation (before DB write) ───────────────────
    for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid cc/bcc email address: {}",
                r.email
            )));
        }
    }

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    let from_override_json = email_opts
        .from_override
        .as_ref()
        .and_then(|o| serde_json::to_value(o).ok());
    let attachments_json = if email_opts.attachments.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.attachments).ok()
    };

    let cc_json = if email_opts.cc.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.cc).ok()
    };
    let bcc_json = if email_opts.bcc.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.bcc).ok()
    };

    match ctx
        .store
        .insert_pending(InsertPendingArgs {
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
            event_timestamp: event.timestamp,
        })
        .await
    {
        Ok(InsertResult::Inserted) => {}
        Ok(InsertResult::Duplicate {
            retry_count,
            status,
        }) => match status.as_str() {
            "SENT" | "BLOCKED" => {
                info!("Skipping already-terminal recipient");
                return RecipientOutcome::Skipped;
            }
            _ => return RecipientOutcome::Duplicate { retry_count },
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
    let (subject, body_html, body_text) = match (
        render_template(&prefetched_template.subject, &event.payload),
        render_html_template(&prefetched_template.body_html, &event.payload),
        render_template(&prefetched_template.body_text, &event.payload),
    ) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            return RecipientOutcome::Failed(e);
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
        cc: email_opts
            .cc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
        bcc: email_opts
            .bcc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    if !ctx.rate_limiter.wait_for_token(shutdown).await {
        return RecipientOutcome::Failed(AppError::Queue(
            "service shutdown during rate-limit wait".into(),
        ));
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
            let _ = ctx.store.mark_sent(event.event_id, &recipient.email).await;
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

/// Process all recipients as a single group email (group send mode).
///
/// All addresses in `email_opts.recipients` appear together in the `To:`
/// header of one email. Only the first recipient gets an `email_log` row —
/// the delivery is tracked and retried as a unit.
///
/// This is the correct path when `email_opts.send_mode == SendMode::Group`.
/// The runner calls this once per event instead of spawning per-recipient tasks.
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

    // The primary recipient is used for email_log tracking. We take the first
    // address; the rest go into to_extra on the EmailMessage.
    let primary = match recipients.first() {
        Some(r) => r,
        None => {
            return RecipientOutcome::Failed(AppError::permanent_mailer(
                "group send: recipients list is empty",
            ));
        }
    };

    // ── 0. Validate all To: addresses ────────────────────────────────────────
    for r in recipients {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid recipient email address: {}",
                r.email
            )));
        }
    }

    // ── 1. Template lookup ───────────────────────────────────────────────────
    let prefetched_template = match ctx.template_store.resolve(&event.event_type).await {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. from_override + cc/bcc validation ─────────────────────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(email_opts.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid from_override email address: {addr}"
            )));
        }
    }
    for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid cc/bcc email address: {}",
                r.email
            )));
        }
    }

    // ── 3. Idempotency — track via primary recipient ─────────────────────────
    let from_override_json = email_opts
        .from_override
        .as_ref()
        .and_then(|o| serde_json::to_value(o).ok());
    let attachments_json = if email_opts.attachments.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.attachments).ok()
    };
    let cc_json = if email_opts.cc.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.cc).ok()
    };
    let bcc_json = if email_opts.bcc.is_empty() {
        None
    } else {
        serde_json::to_value(&email_opts.bcc).ok()
    };

    match ctx
        .store
        .insert_pending(InsertPendingArgs {
            event_id: event.event_id,
            event_type: &event.event_type,
            recipient_email: &primary.email,
            recipient_name: primary.name.as_deref(),
            payload: &event.payload,
            from_override: from_override_json.as_ref(),
            attachments: attachments_json.as_ref(),
            sender_account: email_opts.sender_account.as_deref(),
            cc: cc_json.as_ref(),
            bcc: bcc_json.as_ref(),
            send_mode: email_opts.send_mode.as_str(),
            event_timestamp: event.timestamp,
        })
        .await
    {
        Ok(InsertResult::Inserted) => {}
        Ok(InsertResult::Duplicate {
            retry_count,
            status,
        }) => match status.as_str() {
            "SENT" | "BLOCKED" => {
                info!("Group send: skipping already-terminal event");
                return RecipientOutcome::Skipped;
            }
            _ => return RecipientOutcome::Duplicate { retry_count },
        },
        Err(e) => return RecipientOutcome::Failed(e),
    }

    // ── 4. Recipient filter — applied to all To: addresses ────────────────────
    // In group mode all addresses share one email.  A blocked address in any
    // position would receive that email alongside the unblocked recipients, so
    // we check every To: address before sending.
    //
    // If any address is blocked we drop the *entire* group delivery and mark
    // the primary log row BLOCKED so the event is visible in status queries.
    // This matches the semantics of CC/BCC validation (a single bad address
    // fails the whole delivery) and prevents accidental disclosure: sending a
    // group email with a blocked address in the To: header would expose that
    // address to all other recipients.
    //
    // Operators who need to send to a mixed list where some addresses may be
    // blocked should use individual send mode so each recipient is tracked and
    // filtered independently.
    for r in recipients {
        if let Err(AppError::Blocked(reason)) = ctx.filter.check(&r.email) {
            warn!(
                blocked_email = %r.email,
                reason = %reason,
                recipient_count = recipients.len(),
                "Group send: recipient blocked — dropping entire group delivery"
            );
            let _ = ctx
                .store
                .mark_blocked(event.event_id, &primary.email, &reason)
                .await;
            counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
            return RecipientOutcome::Blocked(reason);
        }
    }

    // ── 5. Template rendering ────────────────────────────────────────────────
    let (subject, body_html, body_text) = match (
        render_template(&prefetched_template.subject, &event.payload),
        render_html_template(&prefetched_template.body_html, &event.payload),
        render_template(&prefetched_template.body_text, &event.payload),
    ) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            return RecipientOutcome::Failed(e);
        }
    };

    // Build to_extra from all recipients beyond the first.
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
        cc: email_opts
            .cc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
        bcc: email_opts
            .bcc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    if !ctx.rate_limiter.wait_for_token(shutdown).await {
        return RecipientOutcome::Failed(AppError::Queue(
            "service shutdown during rate-limit wait".into(),
        ));
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
            let _ = ctx.store.mark_sent(event.event_id, &primary.email).await;
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
            RecipientOutcome::Failed(e)
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
        _ => "other",
    }
}
