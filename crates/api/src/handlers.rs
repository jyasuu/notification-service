use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use common::{AppError, EmailStatus};
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
/// Pre-0009 rows that have `from_override = NULL` or `attachments = NULL`
/// fall back to omitting those fields (same behaviour as before).
///
/// The consumer's idempotency guard (`ON CONFLICT DO NOTHING`) ensures rows
/// that are already PENDING are not double-inserted; they simply stay PENDING.
async fn republish_event(state: &ApiState, event_id: Uuid) -> Result<(), ApiError> {
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
    let attachments = logs
        .iter()
        .find_map(|l| l.attachments.clone())
        .unwrap_or(serde_json::Value::Array(vec![]));

    let recipients: Vec<serde_json::Value> = logs
        .iter()
        .map(|l| json!({ "email": l.recipient_email }))
        .collect();

    let envelope = json!({
        "event_id":      event_id,
        "timestamp":     Utc::now().to_rfc3339(),
        "type":          event_type,
        "recipients":    recipients,
        "payload":       template_payload,
        "from_override": from_override,   // null when not stored (pre-0009 rows)
        "attachments":   attachments,
    });
    let body =
        serde_json::to_vec(&envelope).map_err(|e| ApiError(AppError::Queue(e.to_string())))?;
    state
        .publisher
        .publish(body)
        .await
        .map_err(|e| ApiError(e))?;
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
                "email":      log.recipient_email,
                "status":     log.status.as_str(),
                "retryCount": log.retry_count,
                "lastError":  log.last_error,
                "createdAt":  log.created_at,
                "updatedAt":  log.updated_at,
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
        "eventId":      log.event_id,
        "email":        log.recipient_email,
        "status":       log.status.as_str(),
        "retryCount":   log.retry_count,
        "lastError":    log.last_error,
        "createdAt":    log.created_at,
        "updatedAt":    log.updated_at,
    })))
}

// ── Retry ─────────────────────────────────────────────────────────────────────

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
    republish_event(&state, event_id).await?;

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

    republish_event(&state, event_id).await?;

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
