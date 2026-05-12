use std::sync::Arc;

use common::{AppError, EmailEvent, FromOverride, Recipient};
use mailer::message::ResolvedAttachment;
use mailer::smtp::is_permanent_smtp_error;
use mailer::{render_template, EmailMessage, EmailSender};
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
    // ── 1. Template lookup (before DB write) ────────────────────────────────
    // Validate the template exists BEFORE inserting the PENDING row.  If we
    // insert first and then discover an unknown event_type, the row is stuck
    // PENDING forever (the idempotency guard skips it on every subsequent
    // redelivery). Failing early keeps the DB clean and surfaces the bug fast.
    let template = match template_store.resolve(&event.event_type).await {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. Idempotency ───────────────────────────────────────────────────────
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
        render_template(&template.subject, &event.payload),
        render_template(&template.body_html, &event.payload),
        render_template(&template.body_text, &event.payload),
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

/// Validate a `from_override` email address before attempting to send.
///
/// This is deliberately stricter than a full RFC-5321 parser but stops
/// well short of one: the goal is to catch obvious typos and structural
/// errors in operator-supplied config before they reach the SMTP server,
/// not to implement the full address grammar.
///
/// Rules applied:
/// - Must contain exactly one `@` separating a non-empty local part and domain.
/// - Local part: 1–64 characters, no leading/trailing dot, no consecutive dots.
///   Allowed characters: alphanumeric, and `!#$%&'*+/=?^_{|}~.-`
/// - Domain: at least two labels separated by `.`, each label 1–63 chars,
///   alphanumeric plus hyphens (not leading/trailing).
/// - Total length ≤ 254 characters (RFC-5321 §4.5.3.1.3).
///
/// Note: intentionally accepts `user@localhost` — valid for internal mail
/// relays and SMTP test servers (e.g. MailHog, Mailpit).
fn is_valid_email(addr: &str) -> bool {
    if addr.len() > 254 {
        return false;
    }
    let (local, domain) = match addr.split_once('@') {
        Some(parts) => parts,
        None => return false,
    };
    is_valid_local(local) && is_valid_domain(domain)
}

fn is_valid_local(local: &str) -> bool {
    if local.is_empty() || local.len() > 64 {
        return false;
    }
    if local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    local.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(c, '!' | '#' | '$' | '%' | '&' | '\'' | '*' | '+' | '/'
                         | '=' | '?' | '^' | '_' | '`' | '{' | '|' | '}' | '~'
                         | '-' | '.')
    })
}

fn is_valid_domain(domain: &str) -> bool {
    if domain.is_empty() || domain.len() > 253 {
        return false;
    }
    let labels: Vec<&str> = domain.split('.').collect();
    // Allow single-label domains (e.g. "localhost") for internal relay support.
    labels.iter().all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
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

#[cfg(test)]
mod email_validation_tests {
    use super::*;

    // ── Valid addresses ───────────────────────────────────────────────────────
    #[test]
    fn accepts_standard_address() {
        assert!(is_valid_email("user@example.com"));
    }
    #[test]
    fn accepts_subdomain() {
        assert!(is_valid_email("user@mail.example.co.uk"));
    }
    #[test]
    fn accepts_plus_tag() {
        assert!(is_valid_email("user+tag@example.com"));
    }
    #[test]
    fn accepts_dots_in_local() {
        assert!(is_valid_email("first.last@example.com"));
    }
    #[test]
    fn accepts_localhost_for_internal_relay() {
        assert!(is_valid_email("user@localhost"));
    }
    #[test]
    fn accepts_hyphen_in_domain_label() {
        assert!(is_valid_email("user@my-company.com"));
    }

    // ── Invalid addresses ─────────────────────────────────────────────────────
    #[test]
    fn rejects_missing_at() {
        assert!(!is_valid_email("userexample.com"));
    }
    #[test]
    fn rejects_empty_local() {
        assert!(!is_valid_email("@example.com"));
    }
    #[test]
    fn rejects_empty_domain() {
        assert!(!is_valid_email("user@"));
    }
    #[test]
    fn rejects_leading_dot_in_local() {
        assert!(!is_valid_email(".user@example.com"));
    }
    #[test]
    fn rejects_trailing_dot_in_local() {
        assert!(!is_valid_email("user.@example.com"));
    }
    #[test]
    fn rejects_consecutive_dots_in_local() {
        assert!(!is_valid_email("us..er@example.com"));
    }
    #[test]
    fn rejects_leading_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@-example.com"));
    }
    #[test]
    fn rejects_trailing_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@example-.com"));
    }
    #[test]
    fn rejects_space_in_address() {
        assert!(!is_valid_email("us er@example.com"));
    }
    #[test]
    fn rejects_address_over_254_chars() {
        let long_local = "a".repeat(65);
        assert!(!is_valid_email(&format!("{long_local}@example.com")));
    }
}
