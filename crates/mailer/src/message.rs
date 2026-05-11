use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A rendered, ready-to-send email. Both backends consume this type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailMessage {
    /// Stable ID for tracing / webhook correlation.
    pub event_id: Uuid,
    pub to_email: String,
    pub to_name: Option<String>,
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
    /// Per-event From address override. When `Some`, backends use these values
    /// instead of their globally configured from_email / from_name defaults.
    pub from_email_override: Option<String>,
    pub from_name_override: Option<String>,
    /// Resolved attachments — bytes already fetched from their source URLs.
    /// Built by `AttachmentFetcher` in the mailer crate before handing off
    /// to the SMTP / webhook backend.
    #[serde(default)]
    pub attachments: Vec<ResolvedAttachment>,
}

/// A file attachment whose bytes have been fetched and are ready to send.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedAttachment {
    pub filename: String,
    pub content_type: String,
    /// Raw file bytes, fetched from the source URL.
    pub data: Vec<u8>,
}
