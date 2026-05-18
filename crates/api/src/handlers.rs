use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use common::{is_valid_email, AppError, AttachmentRef, EmailEvent, EmailStatus, FromOverride, Recipient};
use common::event::Metadata;
use serde_json::json;
use uuid::Uuid;

use crate::{errors::ApiError, state::ApiState};

/// Re-publish an event to the queue so the consumer re-processes it.
///
/// Reconstructs the event from `email_log` rows, including the stored `payload`,
/// `from_override`, and `attachments` columns so the full original event is
/// faithfully replayed — not a stripped-down envelope that loses the From
/// address override or file attachments.
///
/// `only_emails` — when `Some`, only those email addresses are included in the
/// published recipients list. Used by single-recipient retry to avoid
/// re-enqueuing already-terminal (SENT/BLOCKED) recipients alongside the one
/// being retried, which would cause unnecessary AMQP round-trips and log noise.
/// The consumer's idempotency guard still protects against double-sends, but
/// not publishing them in the first place is cleaner.
/// Pass `None` for bulk retry (all reset recipients are wanted).
///
/// Pre-0009 rows that have `from_override = NULL` or `attachments = NULL`
/// fall back to omitting those fields (same behaviour as before).
async fn republish_event(
    state: &ApiState,
    event_id: Uuid,
    only_emails: Option<&[String]>,
) -> Result<(), ApiError> {
    let logs = state.store.get_by_event_id(event_id).await?;
    if logs.is_empty() {
        return Err(ApiError(AppError::NotFound(event_id.to_string())));
    }

    let event_type = logs[0].event_type.clone();

    // All recipients of the same event share the same template payload,
    // from_override, and attachments — use the first non-null value found.
    let template_payload = logs
        .iter()
        .find_map(|l| l.payload.clone())
        .unwrap_or(serde_json::Value::Object(Default::default()));

    let from_override = logs.iter().find_map(|l| l.from_override.clone());

    // Validate the from_override email address before re-enqueuing so a stored
    // bad address is rejected here (400) rather than causing a guaranteed
    // permanent failure on the consumer side for every retry attempt.
    if let Some(ref ov) = from_override {
        if let Some(email) = ov.get("email").and_then(|v| v.as_str()) {
            if !is_valid_email(email) {
                return Err(ApiError(AppError::Mailer(format!(
                    "permanent: stored from_override email address '{email}' is invalid — \
                     fix the email_log row before retrying"
                ))));
            }
        }
    }

    let attachments = logs
        .iter()
        .find_map(|l| l.attachments.clone())
        .unwrap_or(serde_json::Value::Array(vec![]));

    // Use the created_at of the earliest log row as a proxy for the original
    // event timestamp. This serves two purposes:
    //   1. The envelope's `timestamp` field is preserved so consumer-side
    //      attachment expiry checks (max_age_secs) use the correct age.
    //      Using Utc::now() would reset the clock and allow already-expired
    //      attachment URLs to slip past validation on retry.
    //   2. The pre-flight expiry check below uses the same value so the
    //      API and consumer agree on what "expired" means.
    let original_timestamp = logs
        .iter()
        .map(|l| l.created_at)
        .min()
        .unwrap_or_else(Utc::now);

    // Warn callers when stored attachment URLs are provably expired so they
    // learn upfront rather than getting a cryptic permanent-failure on the
    // consumer side.  We only check refs that carry a `max_age_secs` hint;
    // pre-signed URLs without the hint are forwarded unconditionally (the
    // fetcher will classify a 4xx response as a permanent error).
    if let Some(refs) = attachments.as_array() {
        let age_secs = Utc::now()
            .signed_duration_since(original_timestamp)
            .num_seconds()
            .max(0) as u64;

        let expired: Vec<&str> = refs
            .iter()
            .filter_map(|r| {
                let max_age = r.get("max_age_secs")?.as_u64()?;
                if age_secs > max_age {
                    r.get("filename")?.as_str()
                } else {
                    None
                }
            })
            .collect();

        if !expired.is_empty() {
            return Err(ApiError(AppError::Mailer(format!(
                "permanent: {} attachment URL(s) have expired (age {}s > max_age_secs): {}. \
                 The business service must re-publish the event with fresh URLs before retrying.",
                expired.len(),
                age_secs,
                expired.join(", "),
            ))));
        }
    }

    // Only include the recipients that were actually reset. For single-recipient
    // retry this is just the one email; for bulk retry it is all reset addresses.
    // This avoids publishing already-terminal (SENT/BLOCKED) recipients into the
    // queue, which would cause unnecessary AMQP round-trips and consumer log noise
    // (even though the idempotency guard would skip them on re-delivery).
    let recipients: Vec<Recipient> = logs
        .iter()
        .filter(|l| {
            only_emails
                .map(|set| set.iter().any(|e| e == &l.recipient_email))
                .unwrap_or(true)
        })
        .map(|l| Recipient {
            email: l.recipient_email.clone(),
            // Preserve the original display name so templates that use {{name}}
            // render correctly on retried deliveries (pre-0011 rows have NULL
            // which is omitted, matching the original behaviour).
            name: l.recipient_name.clone(),
        })
        .collect();

    // Recover the sender_account name used for the original delivery so the
    // retry uses the same SMTP account and From address (fix: pre-0014 rows
    // have NULL here, which causes the consumer to fall back to the global
    // [mailer] default — acceptable for legacy rows).
    let sender_account = logs.iter().find_map(|l| l.sender_account.clone());

    // Deserialize the stored from_override JSON back into the typed struct so
    // the retry envelope is always built through EmailEvent — guaranteeing the
    // serialized field names match what the consumer deserializes.
    let from_override: Option<FromOverride> = from_override
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    // Deserialize stored attachment refs back to typed structs for the same
    // reason — field names are guaranteed consistent with EmailEvent.
    let attachments: Vec<AttachmentRef> = serde_json::from_value(attachments)
        .unwrap_or_default();

    let event = EmailEvent {
        event_id,
        timestamp: original_timestamp,
        event_type,
        recipients,
        payload: template_payload,
        from_override,
        metadata: Metadata { source: None },
        attachments,
        sender_account,
    };
    let body =
        serde_json::to_vec(&event).map_err(|e| ApiError(AppError::Queue(e.to_string())))?;
    state.publisher.publish(body).await.map_err(ApiError)?;
    Ok(())
}

// ── Health ────────────────────────────────────────────────────────────────────

/// GET /health
///
/// Shallow liveness check (always 200 if process is up).
/// A separate /ready endpoint checks DB connectivity.
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// GET /ready
///
/// Readiness probe — verifies the DB pool can acquire a connection.
/// Docker / Kubernetes should use this for `healthcheck`, not /health.
pub async fn ready(State(state): State<ApiState>) -> impl IntoResponse {
    match sqlx::query("SELECT 1").execute(state.store.pool()).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ready" }))),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "error": e.to_string() })),
        ),
    }
}

// ── Status ────────────────────────────────────────────────────────────────────

/// GET /emails/:event_id
///
/// Returns delivery status for every recipient in the event.
///
/// Response shape:
/// ```json
/// {
///   "eventId": "...",
///   "recipients": [
///     { "email": "a@x.com", "status": "SENT",    "retryCount": 0, ... },
///     { "email": "b@x.com", "status": "BLOCKED",  "retryCount": 0, ... },
///     { "email": "c@x.com", "status": "FAILED",   "retryCount": 3, ... }
///   ],
///   "summary": { "total": 3, "sent": 1, "blocked": 1, "failed": 1, "pending": 0 }
/// }
/// ```
pub async fn get_email_status(
    State(state): State<ApiState>,
    Path(event_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let logs = state.store.get_by_event_id(event_id).await?;

    let mut sent = 0u32;
    let mut blocked = 0u32;
    let mut failed = 0u32;
    let mut pending = 0u32;

    let recipients: Vec<_> = logs
        .iter()
        .map(|log| {
            match log.status {
                EmailStatus::Sent => sent += 1,
                EmailStatus::Blocked => blocked += 1,
                EmailStatus::Failed => failed += 1,
                EmailStatus::Pending => pending += 1,
            }
            json!({
                "email":         log.recipient_email,
                "status":        log.status.as_str(),
                "retryCount":    log.retry_count,
                "totalAttempts": log.total_attempts,
                "lastError":     log.last_error,
                "createdAt":     log.created_at,
                "updatedAt":     log.updated_at,
            })
        })
        .collect();

    Ok(Json(json!({
        "eventId":    event_id,
        "recipients": recipients,
        "summary": {
            "total":   recipients.len(),
            "sent":    sent,
            "blocked": blocked,
            "failed":  failed,
            "pending": pending,
        }
    })))
}

/// GET /emails/:event_id/recipients/:email
///
/// Returns delivery status for a single recipient within an event.
pub async fn get_recipient_status(
    State(state): State<ApiState>,
    Path((event_id, email)): Path<(Uuid, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let log = state
        .store
        .get_by_event_and_recipient(event_id, &email)
        .await?;

    Ok(Json(json!({
        "eventId":       log.event_id,
        "email":         log.recipient_email,
        "status":        log.status.as_str(),
        "retryCount":    log.retry_count,
        "totalAttempts": log.total_attempts,
        "lastError":     log.last_error,
        "createdAt":     log.created_at,
        "updatedAt":     log.updated_at,
    })))
}

// ── Template cache ────────────────────────────────────────────────────────────

/// DELETE /templates/:event_type/cache
///
/// Evicts one entry from the in-memory template cache, forcing the next
/// delivery attempt for that event type to re-fetch from the database.
/// Use this after editing a row in the `email_template` table so the change
/// takes effect without a service restart.
///
/// Returns 204 No Content on success (even when the key was not cached).
pub async fn invalidate_template_cache(
    State(state): State<ApiState>,
    Path(event_type): Path<String>,
) -> impl IntoResponse {
    state.template_store.invalidate(&event_type).await;
    StatusCode::NO_CONTENT
}

/// DELETE /templates/cache
///
/// Clears the entire in-memory template cache.  All subsequent deliveries
/// will re-fetch their templates from the database.
///
/// Returns 204 No Content.
pub async fn invalidate_all_template_cache(State(state): State<ApiState>) -> impl IntoResponse {
    state.template_store.reload_all().await;
    StatusCode::NO_CONTENT
}

/// POST /emails/:event_id/recipients/:email/retry
///
/// Atomically resets one FAILED recipient row back to PENDING and re-enqueues
/// the event. The UPDATE uses a `WHERE status = 'FAILED'` guard so concurrent
/// requests cannot race: only the request that actually flips the row wins;
/// others receive 404.
///
/// Returns:
/// * 202 — reset successfully
/// * 404 — no FAILED record for this event+recipient
pub async fn retry_recipient(
    State(state): State<ApiState>,
    Path((event_id, email)): Path<(Uuid, String)>,
) -> Result<impl IntoResponse, ApiError> {
    // Atomic UPDATE: only succeeds when status = 'FAILED'.
    // Replaces the old fetch-then-update pattern that had a TOCTOU race.
    state.store.reset_for_retry(event_id, &email).await?;
    // Only re-enqueue the one recipient that was reset — not the whole event.
    // SENT/BLOCKED recipients in the same event must not be re-published.
    republish_event(&state, event_id, Some(&[email.clone()])).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "message":  "Reset to PENDING and re-enqueued for immediate retry.",
            "eventId":  event_id,
            "email":    email,
        })),
    ))
}

/// POST /emails/:event_id/retry
///
/// Bulk retry — atomically resets ALL FAILED recipients for this event to
/// PENDING in a single UPDATE.  Avoids the TOCTOU race of the old
/// fetch-then-loop approach.
pub async fn retry_event(
    State(state): State<ApiState>,
    Path(event_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let reset = state.store.reset_all_failed_for_event(event_id).await?;

    if reset.is_empty() {
        return Err(ApiError(AppError::NotFound(format!(
            "No FAILED recipients for event {event_id}"
        ))));
    }

    republish_event(&state, event_id, Some(&reset)).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "message":          "FAILED recipients reset to PENDING and re-enqueued.",
            "eventId":          event_id,
            "recipientsReset":  reset.len(),
            "recipients":       reset,
        })),
    ))
}
