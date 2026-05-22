use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppError;

/// Delivery status shared across all notification channels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotificationStatus {
    Pending,
    Sent,
    Failed,
    Blocked,
}

impl NotificationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationStatus::Pending => "PENDING",
            NotificationStatus::Sent => "SENT",
            NotificationStatus::Failed => "FAILED",
            NotificationStatus::Blocked => "BLOCKED",
        }
    }
}

impl TryFrom<&str> for NotificationStatus {
    type Error = AppError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "PENDING" => Ok(NotificationStatus::Pending),
            "SENT" => Ok(NotificationStatus::Sent),
            "FAILED" => Ok(NotificationStatus::Failed),
            "BLOCKED" => Ok(NotificationStatus::Blocked),
            other => Err(AppError::UnknownStatus(other.to_owned())),
        }
    }
}

impl std::fmt::Display for NotificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Back-compat alias — callers that still use `EmailStatus` continue to compile.
pub use NotificationStatus as EmailStatus;

/// Channel-agnostic delivery log row.
///
/// Maps 1-to-1 with `notification_log` + `email_notification_log`.
/// Keyed by `(event_id, recipient_email)` — one row per recipient per event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationLog {
    pub id: Uuid,
    pub event_id: Uuid,
    /// The specific recipient this row tracks.
    pub recipient_email: String,
    /// Optional display name for the recipient (e.g. "Alice Smith").
    /// Stored so it can be faithfully re-published on manual retry,
    /// preventing templates that use {{name}} from rendering the raw
    /// placeholder on retried deliveries.
    /// Nullable for rows written before migration 0011.
    pub recipient_name: Option<String>,
    pub event_type: String,
    pub status: NotificationStatus,
    /// How many automatic retry attempts have been made in the current attempt
    /// window.  Reset to 0 when an operator manually retries via the HTTP API
    /// so the recipient gets a fresh set of automatic retries.
    pub retry_count: i32,
    /// Lifetime delivery attempt counter — never reset, even on manual retry.
    /// Useful for auditing and detecting persistently failing addresses.
    pub total_attempts: i32,
    pub last_error: Option<String>,
    /// Original template payload stored for retry reconstruction.
    /// Nullable for rows written before migration 0007.
    pub payload: Option<serde_json::Value>,
    /// Per-event From address override stored for retry reconstruction.
    /// Nullable for rows written before migration 0009.
    pub from_override: Option<serde_json::Value>,
    /// URL-based attachment references stored for retry reconstruction.
    /// Nullable for rows written before migration 0009.
    pub attachments: Option<serde_json::Value>,
    /// Named SMTP sender account used for the original delivery.
    /// Stored so manual retries via the HTTP API send from the same account.
    /// NULL means the global [mailer] default was used.
    /// Nullable for rows written before migration 0014.
    pub sender_account: Option<String>,
    /// CC recipients stored for retry reconstruction.
    /// JSON array of `{"email": "...", "name": "..."}` objects, or NULL.
    /// Nullable for rows written before migration 0020.
    pub cc: Option<serde_json::Value>,
    /// BCC recipients stored for retry reconstruction.
    /// JSON array of `{"email": "...", "name": "..."}` objects, or NULL.
    /// Nullable for rows written before migration 0020.
    pub bcc: Option<serde_json::Value>,
    /// Delivery mode of the original event: `"individual"` or `"group"`.
    ///
    /// Stored so manual retries via the HTTP API faithfully replay the
    /// original behaviour.  Without this, group-mode events (all recipients
    /// share one email) would be incorrectly retried as individual-mode
    /// (separate email per address), silently changing what recipients see.
    ///
    /// Nullable for rows written before migration 0023; treated as
    /// `SendMode::Individual` on retry (same default as before group-mode
    /// was introduced).
    pub send_mode: Option<String>,
    /// The `NotificationEvent.timestamp` written by the business service.
    ///
    /// Distinct from `created_at` (the DB insertion time).  Used by
    /// `republish_event()` for attachment expiry checks so the consumer and
    /// API agree on what "expired" means regardless of queue or processing lag.
    ///
    /// Nullable for rows written before migration 0023; `republish_event()`
    /// falls back to `created_at` as a proxy for those legacy rows.
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Back-compat alias — callers that still use `EmailLog` continue to compile.
pub use NotificationLog as EmailLog;
