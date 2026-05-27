use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

// ── Top-level envelope ────────────────────────────────────────────────────────

/// The canonical event published to the `notifications.requested` queue.
///
/// This is a **channel-agnostic** envelope. All channel-specific concerns
/// (recipients, CC/BCC, attachments, sender account, From overrides) live
/// inside [`ChannelOverrides`] rather than on the top-level struct, so the
/// envelope stays clean as new channels (SMS, push, etc.) are added.
///
/// Business services write this (or a compatible shape) into their Outbox;
/// the Outbox worker forwards it verbatim to RabbitMQ.
///
/// # Minimal example (email only)
/// ```json
/// {
///   "event_id":   "...",
///   "event_type": "ORDER_CONFIRMATION",
///   "payload":    { "order_id": "123" },
///   "channel_overrides": {
///     "email": {
///       "recipients": [{ "email": "alice@example.com" }]
///     }
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEvent {
    /// Stable unique ID — used for idempotency checks per recipient.
    pub event_id: Uuid,

    /// ISO-8601 timestamp set by the publisher.
    pub timestamp: DateTime<Utc>,

    /// Logical type driving template selection (e.g. "ORDER_CONFIRMATION").
    pub event_type: String,

    /// Arbitrary template variables forwarded to the renderer.
    pub payload: Value,

    #[serde(default)]
    pub metadata: Metadata,

    /// Channel-specific options. The envelope is valid with no overrides set;
    /// the consumer decides which channels to activate based on configuration.
    #[serde(default)]
    pub channel_overrides: ChannelOverrides,
}

// ── Channel overrides ─────────────────────────────────────────────────────────

/// Container for per-channel option structs.
///
/// Each field is `Option<…>` — absence means "use channel defaults / skip
/// this channel". A future SMS channel would appear here as
/// `pub sms: Option<SmsOptions>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelOverrides {
    #[serde(default)]
    pub email: Option<EmailOptions>,
}

/// Controls how multiple TO recipients in a single event are delivered.
///
/// `Individual` (default) — each recipient in `recipients` is delivered as a
/// completely separate email with its own `notification_log` row, retry counter, and
/// independent success / failure state.  Recipients cannot see each other's
/// addresses.  This is the correct mode for transactional mail.
///
/// `Group` — all recipients share one email.  Every address appears together
/// in the `To:` header so recipients can see who else received the message.
/// Only the first address gets an `notification_log` row; the delivery is tracked and
/// retried as a unit.  Use for team notifications, shared alerts, or any
/// context where mutual visibility is intentional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendMode {
    /// Each recipient receives a separate, individually tracked email.
    #[default]
    Individual,
    /// All recipients share one email; all appear in the To: header.
    Group,
}

/// Controls how a failed group send is retried.
///
/// `Whole` (default) — the entire group email is retried as a unit.  On
/// re-delivery the service sends one email to all original recipients again.
/// Simple, but carries a double-send risk: any recipient whose SMTP delivery
/// succeeded in a prior partial attempt will receive the message twice.
///
/// `Individual` — on retry each recipient is re-processed independently,
/// using the same individual-send path as `SendMode::Individual`.  The
/// service inserts an `notification_log` row per recipient at the time of the
/// **first** group send attempt, so re-delivery can skip addresses that
/// already have a `SENT` row and only re-send to those still `PENDING` or
/// `FAILED`.
///
/// Use `Individual` when:
/// - recipients partially overlap (some may already be on a suppression list),
/// - SMTP is unreliable and partial deliveries are common,
/// - auditing per-address success/failure matters more than the shared `To:`
///   header semantics of the original group email.
///
/// Trade-off: recipients who are retried individually will receive a separate
/// email — the `To:` header on retry shows only their own address.  If the
/// shared-`To:` visibility is a product requirement for retries too, use
/// `Whole` and accept the double-send risk instead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupRetryMode {
    /// Retry the whole group email as a single unit (default).
    #[default]
    Whole,
    /// On retry, fall back to per-recipient individual sends, skipping
    /// addresses that already have a `SENT` row.
    Individual,
}

/// Controls whether a transient send failure is retried.
///
/// Applies to both `SendMode::Individual` and `SendMode::Group`.
///
/// `Retry` (default) — the runner retries up to the configured `max_retries`
/// limit using exponential back-off, consistent with current behaviour.
///
/// `NoRetry` — any failure (transient or permanent) is immediately marked
/// `FAILED` with `exhausted = true`.  The row is visible in status queries
/// and remains eligible for manual operator retry via the retry API, but the
/// consumer will not attempt another delivery on its own.
///
/// Use `NoRetry` when:
/// - the event is time-sensitive and a delayed retry would be worse than a
///   visible failure (e.g. one-time passcodes, time-locked invitations),
/// - the publisher owns re-delivery and prefers to re-publish rather than
///   rely on in-process retry,
/// - you want deterministic failure visibility without waiting for
///   `max_retries` attempts to exhaust.
///
/// Rate-limit failures are treated the same as transient failures under
/// `NoRetry` — the send is abandoned immediately rather than waiting for a
/// token.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryPolicy {
    /// Retry on transient failures up to `max_retries` (default).
    #[default]
    Retry,
    /// Fail immediately on any send error; do not retry automatically.
    NoRetry,
}

impl SendMode {
    /// Returns the lowercase string representation used in the database
    /// `send_mode` column and in AMQP payloads.
    pub fn as_str(&self) -> &'static str {
        match self {
            SendMode::Individual => "individual",
            SendMode::Group => "group",
        }
    }
}

impl GroupRetryMode {
    /// Returns the lowercase string representation used in the database
    /// `group_retry_mode` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupRetryMode::Whole => "whole",
            GroupRetryMode::Individual => "individual",
        }
    }
}

impl TryFrom<&str> for GroupRetryMode {
    type Error = crate::AppError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "whole" => Ok(GroupRetryMode::Whole),
            "individual" => Ok(GroupRetryMode::Individual),
            other => Err(crate::AppError::UnknownStatus(format!(
                "unknown group_retry_mode: {other}"
            ))),
        }
    }
}

/// All email-specific options for a single event.
///
/// One event can carry **multiple recipients** — the notification service
/// processes each independently so every delivery has its own `notification_log`
/// row, retry counter, and status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailOptions {
    /// Controls whether multiple recipients each get a separate email
    /// (`Individual`, default) or share a single email with all addresses
    /// visible in the `To:` header (`Group`).
    #[serde(default)]
    pub send_mode: SendMode,

    /// Controls how a group send is retried after a transient failure.
    ///
    /// Only meaningful when `send_mode` is `Group`; ignored for `Individual`.
    ///
    /// `Whole` (default) — retry the whole group email as a unit.
    /// `Individual` — fall back to per-recipient sends on retry, skipping
    /// addresses that already have a `SENT` row in `notification_log`.
    ///
    /// See [`GroupRetryMode`] for the full trade-off discussion.
    #[serde(default)]
    pub group_retry_mode: GroupRetryMode,

    /// Controls whether the consumer retries automatically after a transient
    /// send failure.
    ///
    /// `Retry` (default) — retry with exponential back-off up to
    /// `max_retries`.  `NoRetry` — mark `FAILED` immediately without any
    /// automatic retry attempt; the row remains eligible for manual operator
    /// retry via the retry API.
    ///
    /// See [`RetryPolicy`] for the full trade-off discussion.
    #[serde(default)]
    pub retry_policy: RetryPolicy,

    /// One or more TO recipients. Each is processed independently:
    /// a blocked recipient does not prevent others from receiving the email.
    ///
    /// Deserialization is backwards-compatible: a legacy single-recipient
    /// payload `"recipient": {...}` is automatically promoted to a
    /// one-element list via the custom deserializer below.
    ///
    /// The `alias = "recipient"` (singular) handles publishers that embed the
    /// old flat-envelope field name inside `channel_overrides.email` instead
    /// of migrating to the plural form. Without this alias, a singular-key
    /// payload would silently deserialize as an empty `Vec`, dropping the
    /// recipient entirely.
    #[serde(alias = "recipient", deserialize_with = "deserialize_recipients")]
    pub recipients: Vec<Recipient>,

    /// Zero or more CC recipients included in every delivery for this event.
    ///
    /// CC addresses are attached as `Cc:` headers and are visible to all
    /// recipients. They are subject to the same recipient filter (blocklist and
    /// allowlist) as TO recipients.
    ///
    /// A **blocked** CC address is silently excluded from delivery: the
    /// consumer logs the exclusion at `WARN` level and continues sending to the
    /// remaining CC/BCC and all TO recipients. Delivery is never aborted for a
    /// blocked CC address. To audit which CC addresses were stripped, search
    /// structured logs for `message="CC address blocked"`.
    ///
    /// An **invalid** CC address (format check) is still a permanent failure
    /// for the entire delivery — a malformed address can never be delivered
    /// regardless of retry.
    ///
    /// CC addresses do not get their own `notification_log` rows and are not
    /// individually retried. The post-filter (effective) CC list is stored in
    /// the DB so the audit record reflects what was actually delivered.
    #[serde(default)]
    pub cc: Vec<Recipient>,

    /// Zero or more BCC recipients included in every delivery for this event.
    ///
    /// BCC addresses are passed as `Bcc:` headers and are hidden from other
    /// recipients. The same filter, validation, and failure semantics as `cc`
    /// apply: blocked addresses are silently excluded and logged at WARN;
    /// invalid addresses (format check) are a permanent failure for the whole
    /// delivery.
    #[serde(default)]
    pub bcc: Vec<Recipient>,

    /// Optional per-event sender override. When present, the notification
    /// service uses these values as the From address instead of the globally
    /// configured mailer defaults.
    #[serde(default)]
    pub from_override: Option<FromOverride>,

    /// Zero or more file attachments referenced by URL.
    ///
    /// The notification service fetches each URL at send time and attaches
    /// the downloaded bytes to the email.
    ///
    /// See [`AttachmentRef`] for the full contract and URL guidance.
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,

    /// Named sender account to use for SMTP authentication and the From address.
    ///
    /// Must match a key under `[sender_accounts]` in the service config.
    /// When absent (or the name is not configured), the service falls back to
    /// the global `[mailer]` settings.
    #[serde(default)]
    pub sender_account: Option<String>,
}

// ── Backwards-compatibility shim ─────────────────────────────────────────────

/// Legacy flat envelope. Business systems that still publish `EmailEvent`
/// (the old shape) can be transparently promoted to `NotificationEvent` via
/// [`EmailEvent::into_notification_event`].
///
/// **Deprecated**: new publishers should use [`NotificationEvent`] directly.
#[deprecated(
    since = "0.2.0",
    note = "Use NotificationEvent with channel_overrides.email instead"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailEvent {
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "event_type")]
    pub event_type: String,
    /// Accepts both `"recipients"` (new plural form) and the legacy singular
    /// `"recipient"` key so old publishers don't silently lose their recipient.
    /// The `deserialize_recipients` visitor handles both a single object and an
    /// array at the value level; the alias handles the key-name difference.
    #[serde(alias = "recipient", deserialize_with = "deserialize_recipients")]
    pub recipients: Vec<Recipient>,
    pub payload: Value,
    #[serde(default)]
    pub from_override: Option<FromOverride>,
    #[serde(default)]
    pub metadata: Metadata,
    #[serde(default)]
    pub cc: Vec<Recipient>,
    #[serde(default)]
    pub bcc: Vec<Recipient>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
    #[serde(default)]
    pub sender_account: Option<String>,
}

#[allow(deprecated)]
impl EmailEvent {
    /// Promote a legacy `EmailEvent` into the new `NotificationEvent` shape,
    /// moving all email-specific fields into `channel_overrides.email`.
    pub fn into_notification_event(self) -> NotificationEvent {
        NotificationEvent {
            event_id: self.event_id,
            timestamp: self.timestamp,
            event_type: self.event_type,
            payload: self.payload,
            metadata: self.metadata,
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: self.recipients,
                    cc: self.cc,
                    bcc: self.bcc,
                    from_override: self.from_override,
                    attachments: self.attachments,
                    sender_account: self.sender_account,
                    send_mode: SendMode::Individual,
                    group_retry_mode: GroupRetryMode::Whole,
                    retry_policy: RetryPolicy::default(),
                }),
            },
        }
    }
}

// ── Attachment reference ──────────────────────────────────────────────────────

/// A reference to a file attachment, resolved by the notification service
/// at send time via an HTTP GET.
///
/// # Interface contract for business systems
///
/// ```json
/// {
///   "attachments": [
///     {
///       "url":          "https://storage.example.com/invoices/inv-1234.pdf?token=...",
///       "filename":     "invoice-1234.pdf",
///       "content_type": "application/pdf"
///     }
///   ]
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub url: String,
    pub filename: String,
    pub content_type: String,
    #[serde(default)]
    pub fetch_token: Option<String>,
    #[serde(default)]
    pub max_age_secs: Option<u64>,
}

impl AttachmentRef {
    /// Validate metadata fields that can be checked without a network call.
    ///
    /// `check_time` is the wall-clock instant at which expiry is evaluated.
    /// Callers should pass `Utc::now()` in production; tests may pass a fixed
    /// instant to exercise expiry logic deterministically without sleeping.
    pub fn validate(
        &self,
        event_timestamp: &DateTime<Utc>,
        check_time: DateTime<Utc>,
    ) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("permanent: attachment url must not be empty".into());
        }
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            return Err(format!(
                "permanent: attachment url '{}' must start with http:// or https://",
                self.url
            ));
        }
        if self.filename.is_empty() {
            return Err("permanent: attachment filename must not be empty".into());
        }
        if self.filename.contains('/') || self.filename.contains('\\') {
            return Err(format!(
                "permanent: attachment filename '{}' must not contain path separators",
                self.filename
            ));
        }
        if !self.content_type.contains('/') {
            return Err(format!(
                "permanent: attachment content_type '{}' is not a valid MIME type",
                self.content_type
            ));
        }
        if let Some(max_age) = self.max_age_secs {
            let age = check_time
                .signed_duration_since(*event_timestamp)
                .num_seconds()
                .max(0) as u64;
            if age > max_age {
                return Err(format!(
                    "permanent: attachment '{}' URL has expired \
                     (max_age_secs={max_age}, age={age}s)",
                    self.filename
                ));
            }
        }
        Ok(())
    }
}

// ── Deserializer ──────────────────────────────────────────────────────────────

fn deserialize_recipients<'de, D>(de: D) -> Result<Vec<Recipient>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, MapAccess, Visitor};
    use std::fmt;

    struct RecipientsVisitor;

    impl<'de> Visitor<'de> for RecipientsVisitor {
        type Value = Vec<Recipient>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a recipients array or a single recipient object")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();
            while let Some(r) = seq.next_element::<Recipient>()? {
                v.push(r);
            }
            Ok(v)
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let r = Recipient::deserialize(de::value::MapAccessDeserializer::new(map))?;
            Ok(vec![r])
        }
    }

    de.deserialize_any(RecipientsVisitor)
}

// ── Supporting types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromOverride {
    pub email: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipient {
    pub email: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default)]
    pub source: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── deserialize_recipients edge cases ────────────────────────────────────

    /// A normal array of recipient objects should deserialize correctly.
    #[test]
    fn deserialize_recipients_accepts_array() {
        let json = r#"{"recipients": [{"email": "a@example.com"}, {"email": "b@example.com"}]}"#;
        let opts: EmailOptions = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(opts.recipients.len(), 2);
    }

    /// A singular `recipient` key with a single object (legacy shape) should
    /// be promoted to a one-element Vec.
    #[test]
    fn deserialize_recipients_accepts_singular_object() {
        let json = r#"{"recipient": {"email": "a@example.com"}}"#;
        let opts: EmailOptions = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(opts.recipients.len(), 1);
        assert_eq!(opts.recipients[0].email, "a@example.com");
    }

    /// `"recipient": null` is an invalid payload — the deserializer must return
    /// an error rather than silently producing an empty Vec or panicking.
    /// Documents the current behaviour so any future change is intentional.
    #[test]
    fn deserialize_recipients_rejects_null_value() {
        let json = r#"{"recipient": null}"#;
        let result = serde_json::from_str::<EmailOptions>(json);
        assert!(
            result.is_err(),
            "null recipient should produce a deserialization error, got: {:?}",
            result
        );
    }

    /// An empty array is technically valid JSON but produces zero recipients;
    /// the consumer will reject it at the filter step, but deserialization
    /// itself should succeed so the error surface is consistent.
    #[test]
    fn deserialize_recipients_accepts_empty_array() {
        let json = r#"{"recipients": []}"#;
        let opts: EmailOptions = serde_json::from_str(json).expect("should deserialize");
        assert!(opts.recipients.is_empty());
    }
}
