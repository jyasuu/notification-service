use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EmailStatus {
    Pending,
    Sent,
    Failed,
    Blocked,
}

impl EmailStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EmailStatus::Pending => "PENDING",
            EmailStatus::Sent => "SENT",
            EmailStatus::Failed => "FAILED",
            EmailStatus::Blocked => "BLOCKED",
        }
    }
}

impl std::fmt::Display for EmailStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Maps 1-to-1 with the `email_log` PostgreSQL table.
///
/// Keyed by `(event_id, recipient_email)` — one row per recipient per event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailLog {
    pub id: Uuid,
    pub event_id: Uuid,
    /// The specific recipient this row tracks.
    pub recipient_email: String,
    pub event_type: String,
    pub status: EmailStatus,
    pub retry_count: i32,
    pub last_error: Option<String>,
    /// Original template payload stored for retry reconstruction.
    /// Nullable for rows written before migration 0007.
    pub payload: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
