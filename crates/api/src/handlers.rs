use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use common::{
    is_valid_email, AppError, AttachmentRef, ChannelOverrides, EmailOptions, EmailStatus,
    FromOverride, Metadata, NotificationEvent, Recipient, RetryPolicy,
};
use metrics::counter;
use serde_json::json;
use uuid::Uuid;

use crate::{errors::ApiError, state::ApiState};

/// Re-publish an event to the queue so the consumer re-processes it.
///
/// Reconstructs the event from `notification_log` + `email_notification_log` rows,
/// including the stored `payload`, `from_override`, `attachments`, `cc`, `bcc`,
/// `send_mode`, and `event_timestamp` columns so the full original event is
/// faithfully replayed — not a stripped-down envelope that loses the From address
/// override, file attachments, CC/BCC recipients, the delivery mode, or the
/// original publication timestamp.
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
/// Pre-0020 rows that have `cc = NULL` or `bcc = NULL` fall back to empty
/// lists (same behaviour as before that migration).
/// Pre-0023 rows that have `send_mode = NULL` fall back to `Individual`
/// and `event_timestamp = NULL` falls back to the earliest `created_at`
/// (same behaviour as before those columns were added).
async fn republish_event(
    state: &ApiState,
    event_id: Uuid,
    only_emails: Option<&[String]>,
) -> Result<(), ApiError> {
    // ── 1. Fetch per-recipient rows (for recipient list reconstruction) ───────
    // Use the targeted query when only_emails is provided so single-recipient
    // retries don't load every row for the event.
    let logs = state
        .store
        .get_recipients_for_event(event_id, only_emails)
        .await?;

    // ── 2. Fetch the authoritative event-level detail (single source of truth) ─
    //
    // All rows for the same event_id share identical event-level fields
    // (payload, from_override, attachments, cc, bcc, send_mode,
    // group_retry_mode, sender_account).  `get_event_delivery_detail` reads
    // them from the first row and asserts consistency in debug builds,
    // replacing the previous `find_map` scatter that would silently pick
    // whichever row happened to be first if rows diverged after data repair.
    let detail = state.store.get_event_delivery_detail(event_id).await?;

    // ── 3. Validate stored from_override address ──────────────────────────────
    // Reject before re-enqueuing so a stored bad address surfaces as a 400
    // here rather than a guaranteed permanent failure on every consumer retry.
    if let Some(ref ov) = detail.from_override {
        if let Some(email) = ov.get("email").and_then(|v| v.as_str()) {
            if !is_valid_email(email) {
                return Err(ApiError(AppError::permanent_mailer(format!(
                    "stored from_override email address '{email}' is invalid — \
                     fix the notification_log row before retrying"
                ))));
            }
        }
    }

    // ── 4. Determine original timestamp ──────────────────────────────────────
    //
    // Use the stored event_timestamp (the NotificationEvent.timestamp written
    // by the business service).  This ensures attachment expiry checks use the
    // publication time, not the consumer processing time.
    //
    // The column is NOT NULL (migration 0024), so this is always present.
    let original_timestamp = detail.event_timestamp;

    // ── 5. Attachment expiry check ────────────────────────────────────────────
    let attachments_raw = detail
        .attachments
        .clone()
        .unwrap_or(serde_json::Value::Array(vec![]));

    if let Some(refs) = attachments_raw.as_array() {
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
            return Err(ApiError(AppError::permanent_mailer(format!(
                "{} attachment URL(s) have expired (age {}s > max_age_secs): {}. \
                 The business service must re-publish the event with fresh URLs before retrying.",
                expired.len(),
                age_secs,
                expired.join(", "),
            ))));
        }
    }

    // ── 6. Reconstruct recipient list ─────────────────────────────────────────
    // Only include the recipients that were actually reset (the caller
    // supplies the subset).  Avoids re-enqueuing already-terminal
    // (SENT/BLOCKED) addresses and the AMQP round-trips they would cause.
    //
    // Also exclude SKIPPED rows — their recipient_email is a sentinel
    // ("event:{uuid}") that is not a real address. SKIPPED rows are never
    // eligible for replay; the publisher must re-send a corrected event.
    let recipients: Vec<Recipient> = logs
        .iter()
        .filter(|l| l.status != EmailStatus::Skipped)
        .filter(|l| {
            only_emails
                .map(|set| set.iter().any(|e| e == &l.recipient_email))
                .unwrap_or(true)
        })
        .map(|l| Recipient {
            email: l.recipient_email.clone(),
            // Preserve display name so {{name}} renders correctly on retry.
            name: l.recipient_name.clone(),
        })
        .collect();

    // Guard: if all rows are SKIPPED there are no real recipients to replay
    // and get_event_delivery_detail will return NotFound (SKIPPED rows have no
    // email_notification_log entry).  Surface a clear 422 rather than a
    // confusing 404.
    if recipients.is_empty() && logs.iter().all(|l| l.status == EmailStatus::Skipped) {
        return Err(ApiError(AppError::permanent_mailer(format!(
            "Event {event_id} was skipped at validation time and has no deliverable recipients. \
             The publisher must re-publish a corrected event."
        ))));
    }

    // ── 7. Deserialize typed fields from the detail ───────────────────────────
    // Use map_err + ? instead of .ok() so that a malformed stored JSONB value
    // surfaces as a 500 here rather than silently yielding None/empty and
    // re-publishing with the wrong sender, no attachments, or missing CC/BCC.
    let from_override: Option<FromOverride> = detail
        .from_override
        .map(|v| {
            serde_json::from_value(v).map_err(|e| {
                ApiError(AppError::permanent_mailer(format!(
                    "stored from_override is malformed and cannot be deserialized: {e}"
                )))
            })
        })
        .transpose()?;

    let attachments: Vec<AttachmentRef> = serde_json::from_value(attachments_raw).map_err(|e| {
        ApiError(AppError::permanent_mailer(format!(
            "stored attachments JSON is malformed and cannot be deserialized: {e}"
        )))
    })?;

    let cc: Vec<Recipient> = detail
        .cc
        .map(|v| {
            serde_json::from_value(v).map_err(|e| {
                ApiError(AppError::permanent_mailer(format!(
                    "stored cc JSON is malformed and cannot be deserialized: {e}"
                )))
            })
        })
        .transpose()?
        .unwrap_or_default();

    let bcc: Vec<Recipient> = detail
        .bcc
        .map(|v| {
            serde_json::from_value(v).map_err(|e| {
                ApiError(AppError::permanent_mailer(format!(
                    "stored bcc JSON is malformed and cannot be deserialized: {e}"
                )))
            })
        })
        .transpose()?
        .unwrap_or_default();

    let send_mode = detail
        .send_mode
        .as_deref()
        .map(|s| match s {
            "group"       => Ok(common::SendMode::Group),
            "individual"  => Ok(common::SendMode::Individual),
            other => Err(ApiError(AppError::permanent_mailer(format!(
                "unknown send_mode value '{other}' in notification_log row — fix the DB row before retrying"
            )))),
        })
        .transpose()?
        .unwrap_or(common::SendMode::Individual);

    let group_retry_mode = detail
        .group_retry_mode
        .as_deref()
        .map(|s| {
            common::GroupRetryMode::try_from(s).map_err(|_| {
                ApiError(AppError::permanent_mailer(format!(
                    "unknown group_retry_mode value '{s}' in notification_log row — \
                     fix the DB row before retrying"
                )))
            })
        })
        .transpose()?
        .unwrap_or_default();

    // ── 8. Double-send warning for group+whole retries ────────────────────────
    if send_mode == common::SendMode::Group && group_retry_mode == common::GroupRetryMode::Whole {
        tracing::warn!(
            %event_id,
            "Retrying a group-mode event with retry_mode=whole: recipients whose delivery was \
             already accepted by the SMTP server in a prior attempt may receive a duplicate email. \
             Consider using group_retry_mode = \"individual\" in future events to avoid this."
        );
    }

    // ── 9. Filter validation ──────────────────────────────────────────────────
    // CC/BCC: hard-reject blocked addresses so the retry doesn't round-trip
    // to the consumer only to produce an immediate blocked failure.
    for r in cc.iter().chain(bcc.iter()) {
        if let Err(common::AppError::Blocked(reason)) = state.filter.check(&r.email) {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "cc/bcc address {} is blocked: {reason}. \
                 Remove the blocked address before retrying.",
                r.email
            ))));
        }
    }
    // TO: warn only — the operator may have already updated the blocklist and
    // wants the consumer to confirm the removal.
    for r in recipients.iter() {
        if let Err(common::AppError::Blocked(reason)) = state.filter.check(&r.email) {
            tracing::warn!(
                %event_id,
                email = %r.email,
                %reason,
                "Retrying event with a blocked TO recipient — consumer will mark it BLOCKED again. \
                 Remove the address from the blocklist first, or the retry will be a no-op."
            );
        }
    }

    // ── 10. Build and publish the replay envelope ─────────────────────────────
    //
    // Guard: if the stored payload is JSON null the template renderer will
    // fail with a Template error on every consumer attempt, silently DLQ-ing
    // the message.  Surface this as a 400 here so the operator knows the row
    // needs repairing before a retry will work.
    if detail.payload.is_null() {
        return Err(ApiError(AppError::permanent_mailer(
            "stored payload is null — the notification_log row must be repaired with a valid JSON payload before this event can be retried"
                .to_owned(),
        )));
    }

    let event = NotificationEvent {
        event_id,
        timestamp: original_timestamp,
        event_type: detail.event_type,
        payload: detail.payload,
        metadata: Metadata { source: None },
        channel_overrides: ChannelOverrides {
            email: Some(EmailOptions {
                recipients,
                cc,
                bcc,
                from_override,
                attachments,
                sender_account: detail.sender_account,
                send_mode,
                group_retry_mode,
                retry_policy: RetryPolicy::default(),
            }),
        },
    };
    let body = serde_json::to_vec(&event).map_err(|e| ApiError(AppError::Queue(e.to_string())))?;

    // ── Atomicity note ────────────────────────────────────────────────────────
    // The DB rows were already reset to PENDING by the caller before this
    // function runs. If the AMQP publish below fails, those rows stay PENDING
    // with no message in the queue to drive them forward — they are "orphaned".
    //
    // Recovery: the Prometheus counter `retry_publish_failed_total` fires on
    // every such failure. Alert on it. An operator can recover by calling the
    // retry endpoint again once the broker is healthy; the consumer's idempotency
    // check prevents double-processing if the first publish did succeed.
    state.publisher.publish(body).await.map_err(|e| {
        counter!("retry_publish_failed_total",
            "event_id" => event_id.to_string())
        .increment(1);
        tracing::error!(
            %event_id,
            error = %e,
            "AMQP publish failed after DB rows reset to PENDING — \
             rows are now orphaned (stuck PENDING with no queue message). \
             Re-call the retry endpoint once the broker recovers. \
             Monitor `retry_publish_failed_total` to alert on this condition."
        );
        ApiError(e)
    })?;
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
///
/// Uses a short 500 ms timeout so that a saturated connection pool (all
/// connections busy, acquire_timeout pending) returns 503 quickly rather
/// than blocking the probe for the full 5-second pool acquire_timeout.
/// A 503 here causes Kubernetes to stop routing traffic, which is the
/// correct behaviour when the service cannot reach the database.
pub async fn ready(State(state): State<ApiState>) -> impl IntoResponse {
    let probe = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sqlx::query("SELECT 1").execute(state.store.pool()),
    )
    .await;
    match probe {
        Ok(Ok(_)) => (StatusCode::OK, Json(json!({ "status": "ready" }))),
        Ok(Err(e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "error": e.to_string() })),
        ),
        Err(_elapsed) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "error": "db pool acquire timed out (500ms)" })),
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
///   "summary": { "total": 3, "sent": 1, "blocked": 1, "failed": 1, "pending": 0, "skipped": 0 }
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
    let mut skipped = 0u32;

    let recipients: Vec<_> = logs
        .iter()
        .map(|log| {
            match log.status {
                EmailStatus::Sent => sent += 1,
                EmailStatus::Blocked => blocked += 1,
                EmailStatus::Failed => failed += 1,
                EmailStatus::Pending => pending += 1,
                // SKIPPED rows use a sentinel recipient_id ("event:{uuid}") —
                // they are counted in the summary but not eligible for retry.
                EmailStatus::Skipped => skipped += 1,
            }
            json!({
                "email":         log.recipient_email,
                "status":        log.status.as_str(),
                "retryCount":    log.retry_count,
                "totalAttempts": log.total_attempts,
                "lastError":     log.last_error,
                // Non-null only for group sends with group_retry_mode = "whole":
                // the primary row carries the full To: list so operators can
                // see every recipient without querying each address individually.
                "toRecipients":  log.to_recipients,
                "createdAt":     log.created_at,
                "updatedAt":     log.updated_at,
            })
        })
        .collect();

    // Capture the count before `recipients` is moved into the json! macro.
    // `get_by_event_id` caps the query at 500 rows; surface this as a
    // `truncated` flag so callers can detect the cut-off rather than
    // silently receiving an incomplete list.
    let total = recipients.len();
    let truncated = total >= 500;

    Ok(Json(json!({
        "eventId":    event_id,
        "recipients": recipients,
        "summary": {
            "total":     total,
            "sent":      sent,
            "blocked":   blocked,
            "failed":    failed,
            "pending":   pending,
            "skipped":   skipped,
            // true when the 500-row safety cap was hit; the actual recipient
            // count may be higher. Query individual recipients via
            // GET /emails/{event_id}/recipients/{email} for full detail.
            "truncated": truncated,
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

/// GET /templates
///
/// Returns all rows from `notification_template` (including inactive) ordered
/// by type then channel.  Body fields (`body_html`, `body_text`) are
/// intentionally omitted from the list response — use `GET /templates/{event_type}`
/// to retrieve the full content of a specific template.
pub async fn list_templates(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let rows = state.template_store.list().await?;
    let body: Vec<_> = rows
        .iter()
        .map(|r| {
            json!({
                "event_type": r.event_type,
                "channel":    r.channel,
                "subject":    r.subject,
                "version":    r.version,
                "active":     r.active,
                "updated_at": r.updated_at,
            })
        })
        .collect();
    Ok(Json(json!({ "templates": body })))
}

/// GET /templates/:event_type
///
/// Returns all channel variants for a single event type (including inactive).
/// Returns 404 when no rows exist for that type.
pub async fn get_template(
    State(state): State<ApiState>,
    Path(event_type): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let rows = state.template_store.get(&event_type).await?;
    if rows.is_empty() {
        return Err(ApiError(AppError::NotFound(format!(
            "No template found for event type '{event_type}'"
        ))));
    }
    let body: Vec<_> = rows
        .iter()
        .map(|r| {
            json!({
                "event_type": r.event_type,
                "channel":    r.channel,
                "subject":    r.subject,
                "body_html":  r.body_html,
                "body_text":  r.body_text,
                "version":    r.version,
                "active":     r.active,
                "updated_at": r.updated_at,
            })
        })
        .collect();
    Ok(Json(json!({ "templates": body })))
}

/// POST /templates
///
/// Upsert a template row for `(event_type, channel)`.  Inserts on first call;
/// on subsequent calls updates the content and bumps `version` when any field
/// has changed (no-op when content is identical, matching the migration logic).
///
/// Request body:
/// ```json
/// {
///   "event_type": "ORDER_CONFIRMATION",
///   "channel":    "email",
///   "subject":    "Order {{ orderId }} confirmed",
///   "body_html":  "<h1>Hi {{ name }}</h1>...",
///   "body_text":  "Hi {{ name }}, ...",
///   "active":     true
/// }
/// ```
/// `channel` defaults to `"email"` when omitted.
/// `active` defaults to `true` when omitted.
///
/// Returns:
/// * 201 — new row inserted
/// * 200 — existing row updated (or unchanged)
/// * 422 — validation failure (missing or empty required field)
pub async fn upsert_template(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let event_type = body
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'event_type'")))?
        .trim();
    let channel = body
        .get("channel")
        .and_then(|v| v.as_str())
        .unwrap_or("email")
        .trim();
    let subject = body
        .get("subject")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'subject'")))?
        .trim();
    let body_html = body
        .get("body_html")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'body_html'")))?
        .trim();
    let body_text = body
        .get("body_text")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'body_text'")))?
        .trim();
    let active = body.get("active").and_then(|v| v.as_bool()).unwrap_or(true);

    if event_type.is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "'event_type' must not be empty",
        )));
    }
    if channel.is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "'channel' must not be empty",
        )));
    }
    if subject.is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "'subject' must not be empty",
        )));
    }

    let (version, inserted) = state
        .template_store
        .upsert(event_type, channel, subject, body_html, body_text, active)
        .await?;

    let status = if inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    Ok((
        status,
        Json(json!({
            "event_type": event_type,
            "channel":    channel,
            "version":    version,
            "active":     active,
            "inserted":   inserted,
        })),
    ))
}

/// PATCH /templates/:event_type
///
/// Partially update a template row identified by `(event_type, channel)`.
/// Only the fields present in the request body are applied; absent fields are
/// left unchanged.  The update is a single atomic `UPDATE … RETURNING` — no
/// read-then-write race condition.
///
/// Request body (all fields optional):
/// ```json
/// {
///   "channel":   "email",
///   "subject":   "New subject line",
///   "body_html": "<p>Updated HTML</p>",
///   "body_text": "Updated text",
///   "active":    true
/// }
/// ```
/// `channel` defaults to `"email"` when omitted.
///
/// Returns 404 when the `(event_type, channel)` row does not exist —
/// use `POST /templates` to create it first.
pub async fn patch_template(
    State(state): State<ApiState>,
    Path(event_type): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let channel = body
        .get("channel")
        .and_then(|v| v.as_str())
        .unwrap_or("email")
        .trim();

    // Extract only the fields that were explicitly supplied.
    let subject = body.get("subject").and_then(|v| v.as_str());
    let body_html = body.get("body_html").and_then(|v| v.as_str());
    let body_text = body.get("body_text").and_then(|v| v.as_str());
    let active = body.get("active").and_then(|v| v.as_bool());

    let result = state
        .template_store
        .patch(&event_type, channel, subject, body_html, body_text, active)
        .await?;

    match result {
        None => Err(ApiError(AppError::NotFound(format!(
            "No template found for event type '{event_type}' channel '{channel}'"
        )))),
        Some((version, active)) => Ok(Json(json!({
            "event_type": event_type,
            "channel":    channel,
            "version":    version,
            "active":     active,
        }))),
    }
}

/// DELETE /template-cache/:event_type///
/// Evicts one entry from the in-memory template cache, forcing the next
/// delivery attempt for that event type to re-fetch from the database.
/// Use this after editing a row in the `notification_template` table so the change
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
    state.template_store.invalidate_all().await;
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
    // Reject obviously invalid addresses before touching the DB.
    // URL-decoding is handled by axum's Path extractor; `is_valid_email`
    // guards against malformed path segments that would otherwise produce
    // a confusing 404 from `reset_for_retry`.
    if !is_valid_email(&email) {
        return Err(ApiError(AppError::permanent_mailer(format!(
            "'{email}' is not a valid email address"
        ))));
    }

    // Atomic UPDATE: only succeeds when status = 'FAILED'.
    // Replaces the old fetch-then-update pattern that had a TOCTOU race.
    state.store.reset_for_retry(event_id, &email).await?;
    // Only re-enqueue the one recipient that was reset — not the whole event.
    // SENT/BLOCKED recipients in the same event must not be re-published.
    republish_event(&state, event_id, Some(std::slice::from_ref(&email))).await?;

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
        // Distinguish "event was skipped at validation time" from "event
        // never arrived" — both look like a 404 to the caller otherwise,
        // but the remediation is completely different.
        let logs = state.store.get_by_event_id(event_id).await?;
        let has_skipped = logs.iter().any(|l| l.status == EmailStatus::Skipped);
        if has_skipped {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "Event {event_id} was skipped at validation time (no email channel, \
                 empty recipients, or recipient count exceeded limit) and cannot be retried. \
                 The publisher must re-publish a corrected event."
            ))));
        }
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

// ── Blocklist admin ───────────────────────────────────────────────────────────

/// GET /admin/blocklist
///
/// Returns all active block/allow-list entries from the database.
pub async fn list_blocklist(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let entries = state.block_list_store.list_entries().await?;
    let body: Vec<_> = entries
        .iter()
        .map(|e| {
            json!({
                "id":        e.id,
                "kind":      e.kind,
                "value":     e.value,
                "reason":    e.reason,
                "createdAt": e.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "entries": body })))
}

/// POST /admin/blocklist
///
/// Add or reactivate a block/allow-list entry.
///
/// Request body:
/// ```json
/// { "kind": "blocked_email", "value": "bad@example.com", "reason": "opt-out" }
/// ```
/// `kind` must be one of: `blocked_email`, `blocked_domain`,
/// `allowed_email`, `allowed_domain`.
pub async fn add_blocklist_entry(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let kind = body
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'kind'")))?;
    let value = body
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError(AppError::permanent_mailer("missing field 'value'")))?;
    let reason = body.get("reason").and_then(|v| v.as_str());

    let valid_kinds = [
        "blocked_email",
        "blocked_domain",
        "allowed_email",
        "allowed_domain",
    ];
    if !valid_kinds.contains(&kind) {
        return Err(ApiError(AppError::permanent_mailer(format!(
            "invalid kind '{kind}' — must be one of: {}",
            valid_kinds.join(", ")
        ))));
    }

    // Validate the value field is non-empty and structurally plausible.
    // Email kinds must pass the same format check used in the consumer;
    // domain kinds must be non-empty and contain at least one dot.
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "'value' must not be empty",
        )));
    }
    match kind {
        "blocked_email" | "allowed_email" => {
            if !is_valid_email(value) {
                return Err(ApiError(AppError::permanent_mailer(format!(
                    "'{value}' is not a valid email address for kind '{kind}'"
                ))));
            }
        }
        "blocked_domain" | "allowed_domain" => {
            // Simple structural check: must contain a dot and no '@'.
            if value.contains('@') || !value.contains('.') {
                return Err(ApiError(AppError::permanent_mailer(format!(
                    "'{value}' is not a valid domain for kind '{kind}' \
                     (expected format: 'example.com')"
                ))));
            }
        }
        _ => unreachable!("kind already validated above"),
    }

    let entry = state
        .block_list_store
        .add_entry(kind, value, reason)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id":        entry.id,
            "kind":      entry.kind,
            "value":     entry.value,
            "reason":    entry.reason,
            "createdAt": entry.created_at,
        })),
    ))
}

/// DELETE /admin/blocklist/:id
///
/// Soft-delete (deactivate) an entry by id.
/// Returns 404 when no active entry has that id.
pub async fn remove_blocklist_entry(
    State(state): State<ApiState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse, ApiError> {
    state.block_list_store.remove_entry(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /admin/blocklist/cache
///
/// Evict the block_list cache snapshot, forcing the next check to reload
/// from the database.  Use this after direct DB edits.
pub async fn invalidate_blocklist_cache(State(state): State<ApiState>) -> impl IntoResponse {
    state.block_list_store.invalidate().await;
    StatusCode::NO_CONTENT
}

/// POST /admin/blocklist/cache
///
/// Eagerly reload the block_list cache from the database and return the
/// number of active entries found.  Useful after a bulk DB import to
/// pre-warm the cache immediately rather than waiting for the next TTL
/// expiry or `check` call.
pub async fn reload_blocklist_cache(State(state): State<ApiState>) -> impl IntoResponse {
    // Invalidate the old snapshot first so `list_entries` is the authoritative
    // view, then touch `check` on a dummy address to populate the cache eagerly.
    state.block_list_store.invalidate().await;
    match state.block_list_store.list_entries().await {
        Ok(entries) => {
            // Warm the cache by triggering a snapshot load.
            let _ = state
                .block_list_store
                .check("__cache_warmup__@example.com")
                .await;
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({ "reloaded": true, "entry_count": entries.len() })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "reload_blocklist_cache: failed to list entries");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

// ── Send mail ─────────────────────────────────────────────────────────────────

/// Request body for `POST /emails/send`.
///
/// The caller supplies the full notification event inline rather than going
/// through the outbox.  This is the direct-send path: the API validates the
/// request, publishes the event to RabbitMQ, and returns 202 immediately.  The
/// consumer picks it up and handles template rendering, sending, and retries
/// exactly as it would for any outbox-sourced event.
///
/// # When to use this endpoint
///
/// Use it for low-volume, operator-initiated or integration-test sends where
/// writing to an outbox table is inconvenient.  For high-volume transactional
/// mail from a business service, the outbox pattern is preferred because it
/// gives you at-least-once delivery guarantees without a synchronous RabbitMQ
/// dependency at write time.
///
/// # Idempotency
///
/// Callers may supply `event_id` to make the call idempotent: if a
/// `notification_log` row already exists for that `event_id` + recipient, the
/// consumer will skip the duplicate on delivery (the idempotency guard in
/// `process_recipient`).  When `event_id` is omitted a fresh UUID is generated.
#[derive(Debug, serde::Deserialize)]
pub struct SendEmailRequest {
    /// Optional stable ID.  Omit to let the service generate one.
    #[serde(default)]
    pub event_id: Option<Uuid>,

    /// Logical event type driving template selection (e.g. `"ORDER_CONFIRMATION"`).
    pub event_type: String,

    /// Arbitrary key/value template variables forwarded to the Handlebars renderer.
    pub payload: serde_json::Value,

    /// Email-specific options: recipients, CC/BCC, attachments, sender account, etc.
    pub email: EmailOptions,

    /// Optional source tag stored in `notification_log.metadata` for tracing.
    #[serde(default)]
    pub source: Option<String>,
}

/// POST /emails/send
///
/// Directly enqueue a notification event for immediate delivery.
///
/// Validates the request (non-empty recipients, valid addresses, valid
/// `from_override` if present, non-empty `event_type`), assigns or uses the
/// supplied `event_id`, then publishes the event to RabbitMQ and returns 202.
///
/// Returns:
/// * 202 — event accepted and enqueued; `event_id` is in the response body
/// * 400 — validation failure (empty recipients, bad address, etc.)
/// * 422 — permanently invalid field (e.g. malformed `from_override` email)
/// * 503 — RabbitMQ unavailable
pub async fn send_email(
    State(state): State<ApiState>,
    Json(req): Json<SendEmailRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // ── 1. Basic validation ────────────────────────────────────────────────────
    if req.event_type.trim().is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "event_type must not be empty",
        )));
    }

    if req.email.recipients.is_empty() {
        return Err(ApiError(AppError::permanent_mailer(
            "email.recipients must contain at least one entry",
        )));
    }

    // Enforce the same recipient ceiling the consumer applies so callers get a
    // 400 here rather than a 202 followed by a silent FAILED row written by the
    // consumer after the message has already been enqueued and dequeued.
    if req.email.recipients.len() > state.max_recipients_per_event {
        return Err(ApiError(AppError::permanent_mailer(format!(
            "recipient count {} exceeds max_recipients_per_event {}",
            req.email.recipients.len(),
            state.max_recipients_per_event,
        ))));
    }

    // Validate every TO recipient address up-front so the caller gets a clear
    // 422 rather than a silent FAILED row in the consumer.
    for r in &req.email.recipients {
        if !is_valid_email(&r.email) {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "invalid recipient email address: '{}'",
                r.email
            ))));
        }
    }

    // Validate CC addresses.
    for r in &req.email.cc {
        if !is_valid_email(&r.email) {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "invalid cc email address: '{}'",
                r.email
            ))));
        }
    }

    // Validate BCC addresses.
    for r in &req.email.bcc {
        if !is_valid_email(&r.email) {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "invalid bcc email address: '{}'",
                r.email
            ))));
        }
    }

    // Validate from_override if present.
    if let Some(ref ov) = req.email.from_override {
        if !is_valid_email(&ov.email) {
            return Err(ApiError(AppError::permanent_mailer(format!(
                "invalid from_override email address: '{}'",
                ov.email
            ))));
        }
    }

    // ── 2. Validate attachment refs ────────────────────────────────────────────
    // Use now() as both event_timestamp and check_time so max_age_secs checks
    // are meaningful: a caller-supplied future timestamp would let an already-
    // expired URL slip through, so we anchor validation to the actual send time.
    let now = Utc::now();
    for att in &req.email.attachments {
        att.validate(&now, now)
            .map_err(|e| ApiError(AppError::permanent_mailer(e)))?;
    }

    // ── 3. Build the NotificationEvent ────────────────────────────────────────
    let event_id = req.event_id.unwrap_or_else(Uuid::now_v7);
    let id_source = if req.event_id.is_some() {
        "caller-supplied"
    } else {
        "generated"
    };
    // Clone event_type before consuming req so the log and response body can
    // reference it after req.email is moved into channel_overrides.
    let event_type = req.event_type.clone();
    let event = NotificationEvent {
        event_id,
        timestamp: now,
        event_type: req.event_type,
        payload: req.payload,
        metadata: Metadata { source: req.source },
        channel_overrides: ChannelOverrides {
            email: Some(req.email),
        },
    };

    // ── 4. Publish ─────────────────────────────────────────────────────────────
    let body = serde_json::to_vec(&event).map_err(|e| {
        ApiError(AppError::permanent_mailer(format!(
            "failed to serialize event: {e}"
        )))
    })?;

    counter!("api_send_email_total").increment(1);
    state.publisher.publish(body).await?;

    tracing::info!(
        event_id        = %event_id,
        event_type      = %event_type,
        event_id_source = id_source,
        "send_email: event enqueued"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "message":   "Event accepted and enqueued for delivery.",
            "eventId":   event_id,
            "eventType": event_type,
        })),
    ))
}
