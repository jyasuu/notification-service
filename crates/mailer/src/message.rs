use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A lightweight mailbox reference used for CC and BCC fields.
///
/// Replaces the previous `(String, Option<String>)` tuple representation so
/// that adding a third field (e.g. a display-name encoding flag) is a
/// non-breaking struct change rather than a positional-tuple break at every
/// call site.  Both SMTP and webhook backends iterate over `Vec<MailboxRef>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxRef {
    pub email: String,
    pub name: Option<String>,
}

/// A rendered, ready-to-send email. Both backends consume this type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailMessage {
    /// Stable ID for tracing / webhook correlation.
    pub event_id: Uuid,
    /// Primary To: address (always present).
    pub to_email: String,
    pub to_name: Option<String>,
    /// Additional To: addresses for group sends.  Empty in individual mode.
    /// When non-empty, all addresses (including `to_email`) appear together
    /// in a single `To:` header so recipients can see each other.
    #[serde(default)]
    pub to_extra: Vec<MailboxRef>,
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
    /// CC recipients added as `Cc:` headers. Visible to all recipients.
    /// Not independently tracked, filtered, or retried.
    #[serde(default)]
    pub cc: Vec<MailboxRef>,
    /// BCC recipients added as `Bcc:` headers. Hidden from other recipients.
    /// Not independently tracked, filtered, or retried.
    #[serde(default)]
    pub bcc: Vec<MailboxRef>,
}

/// A file attachment whose bytes have been fetched and are ready to send.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedAttachment {
    pub filename: String,
    pub content_type: String,
    /// Raw file bytes, fetched from the source URL.
    pub data: Vec<u8>,
}
