use std::sync::Arc;

use common::{AppError, EmailEvent, FromOverride, Recipient};
use mailer::smtp::is_permanent_smtp_error;
use mailer::{fetch_attachments, render_template, templates_for, EmailMessage, EmailSender};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use reqwest::Client;
use store::EmailLogStore;
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
/// Returns the outcome for this recipient. The caller (runner) decides
/// whether to retry on `Failed`.
#[instrument(skip(store, sender, filter, rate_limiter, http, event, recipient),
             fields(event_id = %event.event_id, email = %recipient.email))]
pub async fn process_recipient(
    store: &EmailLogStore,
    sender: &Arc<dyn EmailSender>,
    filter: &RecipientFilter,
    rate_limiter: &MailRateLimiter,
    http: &Client,
    event: &EmailEvent,
    recipient: &Recipient,
) -> RecipientOutcome {
    // ── 1. Template lookup (before DB write) ────────────────────────────────
    // Validate the template exists BEFORE inserting the PENDING row.  If we
    // insert first and then discover an unknown event_type, the row is stuck
    // PENDING forever (the idempotency guard skips it on every subsequent
    // redelivery). Failing early keeps the DB clean and surfaces the bug fast.
    let (subject_tpl, html_tpl, text_tpl) = match templates_for(&event.event_type) {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. Idempotency ───────────────────────────────────────────────────────
    match store
        .insert_pending(
            event.event_id,
            &event.event_type,
            &recipient.email,
            &event.payload,
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

    // ── 3. Recipient filter ──────────────────────────────────────────────────
    if let Err(AppError::Blocked(reason)) = filter.check(&recipient.email) {
        warn!(reason = %reason, "Recipient blocked — dropping");
        let _ = store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // ── 4. Template rendering ────────────────────────────────────────────────
    let (subject, body_html, body_text) = match (
        render_template(subject_tpl, &event.payload),
        render_template(html_tpl, &event.payload),
        render_template(text_tpl, &event.payload),
    ) {
        (Ok(s), Ok(h), Ok(t)) => (s, h, t),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            let _ = store
                .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                .await;
            return RecipientOutcome::Failed(e);
        }
    };

    // ── 5. Resolve and validate per-event From override ─────────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(event.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            let msg = format!("invalid from_override email address: {addr}");
            let _ = store
                .mark_failed(event.event_id, &recipient.email, &msg, true)
                .await;
            return RecipientOutcome::Failed(AppError::Mailer(format!("permanent: {msg}")));
        }
    }

    // ── 6. Fetch attachments ─────────────────────────────────────────────────
    // Each AttachmentRef carries a URL; we download the bytes here so the
    // SMTP / webhook backend receives a ready-to-attach ResolvedAttachment.
    //
    // Error classification (from fetcher.rs):
    //   4xx response      → permanent (expired URL, bad URL, auth failure)
    //   5xx / network     → transient (retried by the runner)
    //   max_age exceeded  → permanent (URL window elapsed before this attempt)
    //   size cap exceeded → permanent (file too large)
    let resolved_attachments = if event.attachments.is_empty() {
        vec![]
    } else {
        match fetch_attachments(http, &event.attachments, &event.timestamp).await {
            Ok(atts) => atts,
            Err(e) => {
                let permanent = is_permanent_attachment_error(&e);
                let _ = store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), permanent)
                    .await;
                return RecipientOutcome::Failed(e);
            }
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
        attachments: resolved_attachments,
    };

    // ── 7. Rate-limit token ──────────────────────────────────────────────────
    rate_limiter.wait_for_token().await;

    // ── 8. Send ───────────────────────────────────────────────────────────────
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

/// Returns true for attachment fetch errors that should NOT be retried.
/// These all carry a "permanent:" prefix (set by fetcher.rs).
fn is_permanent_attachment_error(err: &AppError) -> bool {
    matches!(err, AppError::Mailer(m) if m.starts_with("permanent:"))
}

/// Minimal RFC-5322 email address format check.
fn is_valid_email(addr: &str) -> bool {
    match addr.split_once('@') {
        Some((local, domain)) => !local.is_empty() && domain.contains('.') && domain.len() > 2,
        None => false,
    }
}

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
