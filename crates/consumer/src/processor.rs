use std::sync::Arc;

use common::{is_valid_email, AppError, EmailEvent, FromOverride, Recipient};
use mailer::message::ResolvedAttachment;
use mailer::smtp::is_permanent_smtp_error;
use mailer::{render_html_template, render_template, EmailMessage, EmailSender};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use store::{EmailLogStore, TemplateStore};
use tracing::{info, instrument, warn};

/// Result of processing one recipient within an event.
#[derive(Debug)]
pub enum RecipientOutcome {
    Sent,
    Blocked(String),
    Skipped,          // duplicate — already processed
    Failed(AppError), // transient or permanent — handled by runner
}

/// Process a single recipient for an event.
///
/// `attachments` are pre-fetched at the event level (once for all recipients)
/// and passed in as resolved bytes. This avoids re-fetching per-signed URLs
/// for every recipient, which would waste bandwidth and risk URL expiry for
/// later recipients in the list.
///
/// Returns the outcome for this recipient. The caller (runner) decides
/// whether to retry on `Failed`.
#[instrument(skip(store, template_store, sender, filter, rate_limiter, event, recipient, attachments),
             fields(event_id = %event.event_id, email = %recipient.email))]
#[allow(clippy::too_many_arguments)]
pub async fn process_recipient(
    store: &EmailLogStore,
    template_store: &TemplateStore,
    sender: &Arc<dyn EmailSender>,
    filter: &RecipientFilter,
    rate_limiter: &MailRateLimiter,
    event: &EmailEvent,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
) -> RecipientOutcome {
    // ── 0. Recipient email validation (before DB write) ─────────────────────
    // Validate the recipient address structurally before inserting the PENDING
    // row. An invalid address can never be sent to; failing here keeps the DB
    // clean and surfaces the bug immediately instead of leaving a PENDING row
    // that will always fail at the SMTP layer.
    if !is_valid_email(&recipient.email) {
        return RecipientOutcome::Failed(AppError::Mailer(format!(
            "permanent: invalid recipient email address: {}",
            recipient.email
        )));
    }

    // ── 1. Template lookup (before DB write) ────────────────────────────────
    // Skip template resolution entirely when the publisher supplied a
    // pre-rendered body via `body_override`.  We still validate that
    // `body_override` fields are non-empty to surface misconfigured callers
    // early, before inserting the PENDING row.
    //
    // When `body_override` is absent, resolve the template as normal and
    // keep the result — it is reused in step 5 to avoid a second DB/cache
    // round-trip for the same event_type within the same delivery attempt.
    // Failing early (before insert_pending) keeps the DB clean and prevents
    // PENDING rows that can never succeed.
    let prefetched_template = if let Some(ov) = event.body_override.as_ref() {
        // body_override present: validate fields are non-empty before inserting the row.
        // Failing early keeps the DB clean and surfaces misconfigured callers before
        // inserting a PENDING row that can never succeed.
        if ov.subject.is_empty() || ov.body_html.is_empty() || ov.body_text.is_empty() {
            return RecipientOutcome::Failed(AppError::Mailer(
                "permanent: body_override fields (subject, body_html, body_text) must all be non-empty".into(),
            ));
        }
        None
    } else {
        // No body_override: validate the template exists BEFORE inserting the PENDING row.
        // If we insert first and then discover an unknown event_type, the row is stuck
        // PENDING forever (the idempotency guard skips it on every subsequent
        // redelivery). Failing early keeps the DB clean and surfaces the bug fast.
        match template_store.resolve(&event.event_type).await {
            Ok(t) => Some(t),
            Err(e) => return RecipientOutcome::Failed(e),
        }
    };

    // ── 2. from_override validation (before DB write) ───────────────────────
    // Validate the From address override BEFORE inserting the PENDING row.
    // If we insert first and then discover an invalid address, the row is
    // stuck PENDING forever: every subsequent redelivery hits the idempotency
    // guard (Skipped) and the bad address is never surfaced again.
    // Failing here keeps the DB clean and surfaces the misconfiguration fast.
    let (from_email_override, from_name_override) =
        resolve_from_override(event.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            let msg = format!("invalid from_override email address: {addr}");
            return RecipientOutcome::Failed(AppError::Mailer(format!("permanent: {msg}")));
        }
    }

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    // Serialise from_override and attachments refs for DB storage so they
    // can be recovered verbatim when a manual retry re-publishes the event.
    let from_override_json = event
        .from_override
        .as_ref()
        .and_then(|o| serde_json::to_value(o).ok());
    let attachments_json = if event.attachments.is_empty() {
        None
    } else {
        serde_json::to_value(&event.attachments).ok()
    };

    match store
        .insert_pending(
            event.event_id,
            &event.event_type,
            &recipient.email,
            recipient.name.as_deref(),
            &event.payload,
            from_override_json.as_ref(),
            attachments_json.as_ref(),
        )
        .await
    {
        Ok(_) => {}
        Err(AppError::Duplicate(_)) => {
            info!("Skipping duplicate recipient");
            return RecipientOutcome::Skipped;
        }
        Err(e) => return RecipientOutcome::Failed(e),
    }

    // ── 4. Recipient filter ───────────────────────────────────────────────────
    if let Err(AppError::Blocked(reason)) = filter.check(&recipient.email) {
        warn!(reason = %reason, "Recipient blocked — dropping");
        let _ = store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // ── 5. Template rendering ────────────────────────────────────────────────
    // When body_override is present, use its pre-rendered content verbatim.
    // Otherwise use the template resolved (and kept) in the prefetch block above —
    // no second DB/cache round-trip needed.
    let (subject, body_html, body_text) = if let Some(ov) = &event.body_override {
        // body_override: skip DB lookup and {{placeholder}} rendering entirely.
        // The fields were validated to be non-empty in the prefetch block above.
        (
            ov.subject.clone(),
            ov.body_html.clone(),
            ov.body_text.clone(),
        )
    } else {
        // prefetched_template is Some when body_override is None (guaranteed by
        // the prefetch block above which sets Some(t) on the else branch).
        let template = prefetched_template
            .expect("template must have been pre-fetched when body_override is absent");
        match (
            render_template(&template.subject, &event.payload),
            render_html_template(&template.body_html, &event.payload),
            render_template(&template.body_text, &event.payload),
        ) {
            (Ok(s), Ok(h), Ok(t)) => (s, h, t),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                let _ = store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return RecipientOutcome::Failed(e);
            }
        }
    };

    // from_email_override / from_name_override were already resolved and
    // validated in step 2 (before insert_pending). No work needed here.

    let msg = EmailMessage {
        event_id: event.event_id,
        to_email: recipient.email.clone(),
        to_name: recipient.name.clone(),
        subject,
        body_html,
        body_text,
        from_email_override,
        from_name_override,
        // Clone resolved bytes — cheap Arc or Vec clone; bytes were fetched once
        // at the event level in handle_delivery() and shared across all recipients.
        attachments: attachments.to_vec(),
    };

    // ── 6. Rate-limit token ──────────────────────────────────────────────────
    rate_limiter.wait_for_token().await;

    // ── 7. Send ───────────────────────────────────────────────────────────────
    let send_start = std::time::Instant::now();
    match sender.send(&msg).await {
        Ok(()) => {
            let elapsed = send_start.elapsed().as_secs_f64();
            let _ = store.mark_sent(event.event_id, &recipient.email).await;
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
