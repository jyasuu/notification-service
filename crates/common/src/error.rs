use thiserror::Error;

/// Whether a [`AppError::Mailer`] failure should be retried.
///
/// Using an explicit field instead of a string prefix makes the intent
/// self-documenting and removes the risk of a typo silently turning a
/// permanent error into a retryable one (or vice versa).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailerKind {
    /// Transient failure (network hiccup, 5xx, …) — will be retried.
    Transient,
    /// Permanent failure (bad address, invalid template, …) — goes to DLQ.
    Permanent,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Mailer failure with explicit retry semantics.
    ///
    /// Use [`AppError::permanent_mailer`] / [`AppError::transient_mailer`]
    /// rather than constructing this variant directly.  Check permanence with
    /// [`AppError::is_permanent_mailer`] rather than inspecting `kind`.
    #[error("mailer error: {message}")]
    Mailer { message: String, kind: MailerKind },

    /// Permanent failure: bad template, unknown event type, etc.
    /// Never retried — goes straight to DLQ.
    #[error("template error: {0}")]
    Template(String),

    /// Transient failure: mail server / webhook returned 429 Too Many Requests.
    /// Retried with a longer, jittered backoff — NOT the normal retry schedule.
    #[error("rate limited by mail provider: {0}")]
    RateLimited(String),

    /// Permanent skip: recipient is on the block/allow-list.
    /// The message is ACK'd (not DLQ'd) and the row is marked BLOCKED.
    #[error("recipient blocked: {0}")]
    Blocked(String),

    #[error("queue error: {0}")]
    Queue(String),

    #[error("deserialization error: {0}")]
    Deserialize(#[from] serde_json::Error),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("duplicate event (already processed): {0}")]
    Duplicate(String),

    /// A status value was read from the database that has no corresponding
    /// `EmailStatus` variant.  This indicates either a schema migration that
    /// added a new status without a matching code update, or data corruption.
    #[error("unknown email status value in database: '{0}'")]
    UnknownStatus(String),
}

impl AppError {
    /// Construct a permanent (non-retryable) `Mailer` error.
    pub fn permanent_mailer(msg: impl Into<String>) -> Self {
        AppError::Mailer {
            message: msg.into(),
            kind: MailerKind::Permanent,
        }
    }

    /// Construct a transient (retryable) `Mailer` error.
    pub fn transient_mailer(msg: impl Into<String>) -> Self {
        AppError::Mailer {
            message: msg.into(),
            kind: MailerKind::Transient,
        }
    }

    /// Returns `true` when this is a permanent (non-retryable) `Mailer` error.
    pub fn is_permanent_mailer(&self) -> bool {
        matches!(
            self,
            AppError::Mailer {
                kind: MailerKind::Permanent,
                ..
            }
        )
    }
}
