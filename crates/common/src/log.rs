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

// Back-compat alias.
pub use NotificationStatus as EmailStatus;

// ── Channel-agnostic delivery state ──────────────────────────────────────────

/// The channel-agnostic half of a delivery row.
///
/// Maps 1-to-1 with `notification_log` columns that apply to *every* channel
/// (email, SMS, push, …).  Channel-specific replay data lives in the sibling
/// structs (`EmailDeliveryDetail`, and future `SmsDeliveryDetail`, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationLogRow {
    pub id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub channel: String,
    /// Channel-native recipient identity: email address, E.164 phone, device
    /// token, etc.
    pub recipient_id: String,
    pub status: NotificationStatus,
    /// Resets to 0 on each manual operator retry.
    pub retry_count: i32,
    /// Lifetime counter — never reset.
    pub total_attempts: i32,
    pub last_error: Option<String>,
    /// Original template payload forwarded to the renderer.
    pub payload: Option<serde_json::Value>,
    /// The `NotificationEvent.timestamp` from the publishing business service.
    /// Used for attachment-expiry checks.  Nullable for pre-0023 rows.
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── Email-specific replay data ────────────────────────────────────────────────

/// The email-specific half of a delivery row.
///
/// Maps to `email_notification_log`.  All fields here are needed only to
/// faithfully replay an email on manual retry; they have no meaning for
/// other channels.
///
/// These fields are *event-level* (shared across every recipient row of the
/// same event) rather than per-recipient.  `republish_event()` reads them
/// from the first row for the event — they are guaranteed identical across
/// rows by the consumer write path (one event → one set of options written
/// to every row it produces).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailDeliveryDetail {
    /// The specific email address this row was delivered to.
    pub recipient_email: String,
    /// Optional display name for the recipient.
    pub recipient_name: Option<String>,

    // ── Event-level fields (same across all rows for the same event_id) ──────
    /// Per-event From address override.  JSONB: `{"email":"…","name":"…"}`.
    /// NULL → use global [mailer] defaults.
    pub from_override: Option<serde_json::Value>,
    /// Named SMTP sender account key.  NULL → use global [mailer] defaults.
    pub sender_account: Option<String>,
    /// `"individual"` or `"group"`.
    pub send_mode: Option<String>,
    /// `"whole"` or `"individual"` (group retry strategy).
    pub group_retry_mode: Option<String>,
    /// Attachment URL references.  JSONB array.
    pub attachments: Option<serde_json::Value>,
    /// CC recipients (post-filter).  JSONB array of `{"email":"…","name":"…"}`.
    pub cc: Option<serde_json::Value>,
    /// BCC recipients (post-filter).  JSONB array.
    pub bcc: Option<serde_json::Value>,
}

// ── Composed view (backwards-compatible flat struct) ─────────────────────────

/// Flat delivery log used by the API handlers and tests.
///
/// Composes [`NotificationLogRow`] (channel-agnostic state) with
/// [`EmailDeliveryDetail`] (email-specific replay data).  Preserving the flat
/// shape keeps existing call sites unchanged while the two sub-structs can
/// evolve independently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationLog {
    // ── From NotificationLogRow ───────────────────────────────────────────────
    pub id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    /// Delivery channel: "email", and in the future "sms", "push", etc.
    /// Stored explicitly so `core()` can reflect the actual channel rather
    /// than hardcoding "email", which would produce wrong values once other
    /// channels share this struct.
    pub channel: String,
    pub status: NotificationStatus,
    pub retry_count: i32,
    pub total_attempts: i32,
    pub last_error: Option<String>,
    pub payload: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // ── From EmailDeliveryDetail ──────────────────────────────────────────────
    pub recipient_email: String,
    pub recipient_name: Option<String>,
    pub from_override: Option<serde_json::Value>,
    pub sender_account: Option<String>,
    pub send_mode: Option<String>,
    pub group_retry_mode: Option<String>,
    pub attachments: Option<serde_json::Value>,
    pub cc: Option<serde_json::Value>,
    pub bcc: Option<serde_json::Value>,
}

impl NotificationLog {
    /// Extract the channel-agnostic state portion.
    pub fn core(&self) -> NotificationLogRow {
        NotificationLogRow {
            id: self.id,
            event_id: self.event_id,
            event_type: self.event_type.clone(),
            channel: self.channel.clone(),
            recipient_id: self.recipient_email.clone(),
            status: self.status.clone(),
            retry_count: self.retry_count,
            total_attempts: self.total_attempts,
            last_error: self.last_error.clone(),
            payload: self.payload.clone(),
            event_timestamp: self.event_timestamp,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Extract the email-specific replay data.
    pub fn email_detail(&self) -> EmailDeliveryDetail {
        EmailDeliveryDetail {
            recipient_email: self.recipient_email.clone(),
            recipient_name: self.recipient_name.clone(),
            from_override: self.from_override.clone(),
            sender_account: self.sender_account.clone(),
            send_mode: self.send_mode.clone(),
            group_retry_mode: self.group_retry_mode.clone(),
            attachments: self.attachments.clone(),
            cc: self.cc.clone(),
            bcc: self.bcc.clone(),
        }
    }
}

// Back-compat alias.
pub use NotificationLog as EmailLog;
