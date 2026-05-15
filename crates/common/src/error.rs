use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("mailer error: {0}")]
    Mailer(String),

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
