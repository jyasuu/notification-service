use std::sync::Arc;

use common::{is_valid_email, AppError, EmailEvent, FromOverride, Recipient};
use mailer::message::ResolvedAttachment;
use mailer::smtp::is_permanent_smtp_error;
use mailer::{render_html_template, render_template, EmailMessage, EmailSender, SenderRegistry};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use store::{EmailLogStore, InsertResult, TemplateStore};
use tracing::{info, instrument, warn};

/// Shared, cheaply-cloneable context passed to every per-recipient processor call.
#[derive(Clone)]
pub struct ProcessorContext {
    pub store: EmailLogStore,
    pub template_store: TemplateStore,
    /// Global default sender (SMTP or webhook) used when no named account matches.
    pub sender: Arc<dyn EmailSender>,
    /// Registry of named per-business-system SMTP accounts.
    /// When an event carries `sender_account`, the matching entry is used
    /// instead of `sender`. Falls back to `sender` when the name is absent
    /// or not found.
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
    /// without a second round-trip.  The runner should continue retrying.
    Duplicate {
        retry_count: i32,
    },
    Failed(AppError), // transient or permanent — handled by runner
}

/// Process a single recipient for an event.
///
/// `attachments` are pre-fetched at the event level (once for all recipients)
/// and passed in as resolved bytes. This avoids re-fetching pre-signed URLs
/// for every recipient, which would waste bandwidth and risk URL expiry for
/// later recipients in the list.
///
/// Returns the outcome for this recipient. The caller (runner) decides
/// whether to retry on `Failed`.
#[instrument(skip(ctx, event, recipient, attachments, shutdown),
             fields(event_id = %event.event_id, email = %recipient.email))]
pub async fn process_recipient(
    ctx: &ProcessorContext,
    event: &EmailEvent,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    shutdown: &tokio_util::sync::CancellationToken,
) -> RecipientOutcome {
    // ── 0. Recipient email validation (before DB write) ─────────────────────
    if !is_valid_email(&recipient.email) {
        return RecipientOutcome::Failed(AppError::Mailer(format!(
            "permanent: invalid recipient email address: {}",
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
        resolve_from_override(event.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            let msg = format!("invalid from_override email address: {addr}");
            return RecipientOutcome::Failed(AppError::Mailer(format!("permanent: {msg}")));
        }
    }

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    //
    // Two cases:
    //   A. First attempt — insert_pending returns Inserted; proceed.
    //   B. Re-entry after restart / backoff — returns Duplicate { retry_count }.
    //      The row may be PENDING (left by a previous mark_failed) or FAILED.
    //      Either way we should proceed to send, not skip, because the runner
    //      has already decided this recipient needs another attempt.
    //      We only return Skipped when the row is already SENT or BLOCKED —
    //      those are terminal states that must not be replayed.
    //
    // The returned retry_count lets process_recipient surface the current DB
    // count back to the caller (runner) so it can seed its in-memory attempt
    // counter without a second round-trip.
    let from_override_json = event
        .from_override
        .as_ref()
        .and_then(|o| serde_json::to_value(o).ok());
    let attachments_json = if event.attachments.is_empty() {
        None
    } else {
        serde_json::to_value(&event.attachments).ok()
    };

    match ctx
        .store
        .insert_pending(
            event.event_id,
            &event.event_type,
            &recipient.email,
            recipient.name.as_deref(),
            &event.payload,
            from_override_json.as_ref(),
            attachments_json.as_ref(),
            event.sender_account.as_deref(),
        )
        .await
    {
        Ok(InsertResult::Inserted) => {} // fresh row — proceed normally
        Ok(InsertResult::Duplicate { retry_count }) => {
            // Row already exists. Check whether it is in a terminal state.
            match ctx.store.get_status(event.event_id, &recipient.email).await {
                Ok(common::EmailStatus::Sent) | Ok(common::EmailStatus::Blocked) => {
                    info!("Skipping already-terminal recipient");
                    return RecipientOutcome::Skipped;
                }
                // PENDING or FAILED — surface the retry_count to the runner so
                // it can seed its in-memory attempt counter without another query.
                Ok(_) => return RecipientOutcome::Duplicate { retry_count },
                // Can't read status — treat as transient and let runner decide.
                Err(e) => return RecipientOutcome::Failed(e),
            }
        }
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
            // Do NOT call mark_failed here — the runner calls it for every
            // Failed outcome (permanent or transient).  Calling it here too
            // would double-increment retry_count and total_attempts for
            // template errors.
            return RecipientOutcome::Failed(e);
        }
    };

    let msg = EmailMessage {
        event_id: event.event_id,
        to_email: recipient.email.clone(),
        to_name: recipient.name.clone(),
        subject,
        body_html,
        body_text,
        from_email_override,
        from_name_override,
        attachments: attachments.to_vec(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    if !ctx.rate_limiter.wait_for_token(shutdown).await {
        // Shutdown fired while waiting — propagate as a transient error so
        // the runner's shutdown branch marks the row FAILED for manual retry.
        return RecipientOutcome::Failed(AppError::Queue(
            "service shutdown during rate-limit wait".into(),
        ));
    }

    // ── 7. Send ───────────────────────────────────────────────────────────────
    // Resolve the sender: named account from the registry takes priority;
    // fall back to the global default when absent or unrecognised.
    let sender = ctx
        .sender_registry
        .resolve(event.sender_account.as_deref())
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn resolve_from_override(ov: Option<&FromOverride>) -> (Option<String>, Option<String>) {
    match ov {
        None => (None, None),
        Some(o) => (Some(o.email.clone()), o.name.clone()),
    }
}

/// Decide whether a failure type is retryable for a given recipient.
pub fn is_retryable(err: &AppError) -> bool {
    match err {
        AppError::Duplicate(_)
        | AppError::NotFound(_)
        | AppError::Template(_)
        | AppError::Blocked(_) => false,
        _ if is_permanent_smtp_error(err) => false,
        AppError::Mailer(m) if m.starts_with("permanent:") => false,
        _ => true,
    }
}

fn error_reason_label(err: &AppError) -> &'static str {
    match err {
        AppError::RateLimited(_) => "rate_limited",
        AppError::Mailer(m) if m.starts_with("permanent:") => "permanent",
        AppError::Mailer(_) => "transient",
        AppError::Database(_) => "database",
        AppError::Template(_) => "template",
        _ => "other",
    }
}
