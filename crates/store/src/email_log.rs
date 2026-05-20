use common::{AppError, EmailLog, EmailStatus};
use sqlx::PgPool;
use tracing::instrument;
use uuid::Uuid;

/// Result of an `insert_pending` call.
#[derive(Debug)]
pub enum InsertResult {
    /// A new row was inserted (first attempt for this recipient).
    Inserted,
    /// The row already existed; carries the current `retry_count` and `status`
    /// so the caller can make terminal-state decisions without a second query.
    Duplicate { retry_count: i32, status: String },
}

/// Arguments for [`EmailLogStore::insert_pending`].
///
/// Using a named struct instead of 10 positional parameters makes call sites
/// self-documenting and makes future field additions (e.g. a new stored column)
/// a one-line struct change rather than a signature change at every call site.
pub struct InsertPendingArgs<'a> {
    pub event_id: Uuid,
    pub event_type: &'a str,
    pub recipient_email: &'a str,
    pub recipient_name: Option<&'a str>,
    pub payload: &'a serde_json::Value,
    pub from_override: Option<&'a serde_json::Value>,
    pub attachments: Option<&'a serde_json::Value>,
    pub sender_account: Option<&'a str>,
    pub cc: Option<&'a serde_json::Value>,
    pub bcc: Option<&'a serde_json::Value>,
    /// Delivery mode of the original event (`"individual"` or `"group"`).
    /// Stored so `republish_event()` can faithfully replay the original mode
    /// rather than defaulting every retry to `Individual`.
    pub send_mode: &'a str,
    /// The original `NotificationEvent.timestamp` from the business service.
    /// Stored separately from `created_at` so attachment expiry checks use
    /// the publication time rather than the consumer processing time.
    pub event_timestamp: chrono::DateTime<chrono::Utc>,
}

/// All database operations for the `email_log` table.
///
/// Keyed by `(event_id, recipient_email)` — one row per recipient per event.
#[derive(Clone)]
pub struct EmailLogStore {
    pool: PgPool,
}

impl EmailLogStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a PENDING row for one recipient.
    ///
    /// Returns `InsertResult::Inserted` for a fresh row, or
    /// `InsertResult::Duplicate { retry_count }` when `(event_id,
    /// recipient_email)` already exists.  The `retry_count` on a duplicate is
    /// returned inline so the caller can seed its in-memory retry counter
    /// without a second round-trip to the database.
    ///
    /// `payload`, `from_override`, `attachments`, `cc`, and `bcc` are stored for
    /// retry reconstruction so the full original event can be re-published on manual
    /// retry. `recipient_name` is stored so the display name survives retries.
    ///
    /// **Idempotency note**: on conflict the stored `payload`, `from_override`,
    /// `attachments`, `cc`, `bcc`, and `sender_account` are intentionally **not** updated.
    /// The first write wins. If a re-published event carries different values
    /// for these fields (e.g. refreshed attachment URLs), the stored row keeps
    /// the original values. This is by design — the retry API reconstructs the
    /// event from the stored columns, so updating them mid-flight would change
    /// what gets re-sent. To deliver a corrected event, cancel the existing row
    /// and publish a new event with a different `event_id`.
    #[instrument(skip(self, args))]
    pub async fn insert_pending(
        &self,
        args: InsertPendingArgs<'_>,
    ) -> Result<InsertResult, AppError> {
        // Use DO UPDATE to return the existing retry_count and status on conflict
        // so the caller can make terminal-state decisions (SENT/BLOCKED skip) in
        // a single round-trip.  The UPDATE expression is a no-op (status stays
        // unchanged); we only need the RETURNING clause to fire on conflict rows.
        //
        // Note: `xmax <> 0` is a PostgreSQL-specific trick — xmax holds the
        // transaction ID of the last deleting or locking transaction; on a plain
        // INSERT it is always 0, so a non-zero value reliably signals a conflict.
        let row = sqlx::query!(
            r#"
            INSERT INTO email_log
                (event_id, event_type, recipient_email, recipient_name, status, payload, from_override, attachments, sender_account, cc, bcc, send_mode, event_timestamp)
            VALUES ($1, $2, $3, $4, 'PENDING', $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (event_id, recipient_email) DO UPDATE
                SET updated_at = email_log.updated_at  -- no-op; fires RETURNING on conflict
            RETURNING id, retry_count, status,
                      (xmax <> 0) AS "was_conflict!: bool"
            "#,
            args.event_id,
            args.event_type,
            args.recipient_email,
            args.recipient_name,
            args.payload,
            args.from_override,
            args.attachments,
            args.sender_account,
            args.cc,
            args.bcc,
            args.send_mode,
            args.event_timestamp,
        )
        .fetch_one(&self.pool)
        .await?;

        if row.was_conflict {
            Ok(InsertResult::Duplicate {
                retry_count: row.retry_count,
                status: row.status,
            })
        } else {
            Ok(InsertResult::Inserted)
        }
    }

    #[instrument(skip(self))]
    pub async fn mark_sent(&self, event_id: Uuid, recipient_email: &str) -> Result<(), AppError> {
        sqlx::query!(
            "UPDATE email_log SET status='SENT', total_attempts=total_attempts+1, updated_at=now() WHERE event_id=$1 AND recipient_email=$2",
            event_id, recipient_email,
        ).execute(&self.pool).await?;
        Ok(())
    }

    #[instrument(skip(self, error_msg))]
    pub async fn mark_failed(
        &self,
        event_id: Uuid,
        recipient_email: &str,
        error_msg: &str,
        exhausted: bool,
    ) -> Result<(), AppError> {
        let status = if exhausted { "FAILED" } else { "PENDING" };
        sqlx::query!(
            r#"UPDATE email_log
               SET retry_count=retry_count+1, total_attempts=total_attempts+1,
                   last_error=$3, status=$4, updated_at=now()
               WHERE event_id=$1 AND recipient_email=$2"#,
            event_id,
            recipient_email,
            error_msg,
            status,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[instrument(skip(self, reason))]
    pub async fn mark_blocked(
        &self,
        event_id: Uuid,
        recipient_email: &str,
        reason: &str,
    ) -> Result<(), AppError> {
        sqlx::query!(
            "UPDATE email_log SET status='BLOCKED', last_error=$3, updated_at=now() WHERE event_id=$1 AND recipient_email=$2",
            event_id, recipient_email, reason,
        ).execute(&self.pool).await?;
        Ok(())
    }

    /// Fetch all delivery rows for an event (one per recipient).
    #[instrument(skip(self))]
    pub async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<EmailLog>, AppError> {
        let rows = sqlx::query!(
            r#"SELECT id, event_id, event_type, recipient_email, recipient_name, status, retry_count,
                      total_attempts, last_error, payload, from_override, attachments, sender_account, cc, bcc,
                      send_mode, event_timestamp, created_at, updated_at
               FROM email_log WHERE event_id=$1 ORDER BY created_at"#,
            event_id,
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(AppError::NotFound(event_id.to_string()));
        }

        rows.into_iter()
            .map(|r| {
                Ok(EmailLog {
                    id: r.id,
                    event_id: r.event_id,
                    recipient_email: r.recipient_email,
                    recipient_name: r.recipient_name,
                    event_type: r.event_type,
                    status: EmailStatus::try_from(r.status.as_str())?,
                    retry_count: r.retry_count,
                    total_attempts: r.total_attempts,
                    last_error: r.last_error,
                    payload: r.payload,
                    from_override: r.from_override,
                    attachments: r.attachments,
                    sender_account: r.sender_account,
                    cc: r.cc,
                    bcc: r.bcc,
                    send_mode: r.send_mode,
                    event_timestamp: r.event_timestamp,
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()
    }

    /// Fetch the row for a single recipient within an event.
    #[instrument(skip(self))]
    pub async fn get_by_event_and_recipient(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<EmailLog, AppError> {
        let r = sqlx::query!(
            r#"SELECT id, event_id, event_type, recipient_email, recipient_name, status, retry_count,
                      total_attempts, last_error, payload, from_override, attachments, sender_account, cc, bcc,
                      send_mode, event_timestamp, created_at, updated_at
               FROM email_log WHERE event_id=$1 AND recipient_email=$2"#,
            event_id,
            recipient_email,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_email}")))?;

        Ok(EmailLog {
            id: r.id,
            event_id: r.event_id,
            recipient_email: r.recipient_email,
            recipient_name: r.recipient_name,
            event_type: r.event_type,
            status: EmailStatus::try_from(r.status.as_str())?,
            retry_count: r.retry_count,
            total_attempts: r.total_attempts,
            last_error: r.last_error,
            payload: r.payload,
            from_override: r.from_override,
            attachments: r.attachments,
            sender_account: r.sender_account,
            cc: r.cc,
            bcc: r.bcc,
            send_mode: r.send_mode,
            event_timestamp: r.event_timestamp,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }

    /// Reset a single FAILED recipient row to PENDING for manual replay.
    /// Uses an atomic UPDATE … WHERE status='FAILED' to avoid the TOCTOU race
    /// that existed in the old fetch-then-update approach.
    ///
    /// `retry_count` is reset to 0 so the recipient gets a full fresh set of
    /// automatic retries.  `total_attempts` is intentionally NOT reset — it is
    /// a lifetime counter used for auditing and detecting persistently failing
    /// addresses.
    #[instrument(skip(self))]
    pub async fn reset_for_retry(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<(), AppError> {
        let result = sqlx::query!(
            r#"UPDATE email_log SET status='PENDING', retry_count=0, last_error=NULL, updated_at=now()
               WHERE event_id=$1 AND recipient_email=$2 AND status='FAILED'"#,
            event_id, recipient_email,
        ).execute(&self.pool).await?;

        if result.rows_affected() == 0 {
            return Err(AppError::NotFound(format!(
                "No FAILED record for {event_id}/{recipient_email}"
            )));
        }
        Ok(())
    }

    /// Atomically reset ALL FAILED rows for an event to PENDING in a single
    /// UPDATE, returning the email addresses that were actually reset.
    ///
    /// `retry_count` is reset to 0 so each recipient gets a fresh set of
    /// automatic retries.  `total_attempts` is intentionally NOT reset — it
    /// is a lifetime counter preserved across manual retries for auditing.
    #[instrument(skip(self))]
    pub async fn reset_all_failed_for_event(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<String>, AppError> {
        let rows = sqlx::query!(
            r#"UPDATE email_log
               SET    status='PENDING', retry_count=0, last_error=NULL, updated_at=now()
               WHERE  event_id=$1 AND status='FAILED'
               RETURNING recipient_email"#,
            event_id,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.recipient_email).collect())
    }

    /// Return the current status of a single recipient row.
    /// Used by the processor to distinguish terminal (SENT/BLOCKED) from
    /// retryable (PENDING/FAILED) states on re-entry after restart.
    #[instrument(skip(self))]
    pub async fn get_status(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<EmailStatus, AppError> {
        let row = sqlx::query!(
            "SELECT status FROM email_log WHERE event_id=$1 AND recipient_email=$2",
            event_id,
            recipient_email,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_email}")))?;

        EmailStatus::try_from(row.status.as_str())
    }

    /// Return the retry_count for a specific recipient row (seeds in-memory counter after restart).
    #[instrument(skip(self))]
    pub async fn get_retry_count(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<i32, AppError> {
        let row = sqlx::query!(
            "SELECT retry_count FROM email_log WHERE event_id=$1 AND recipient_email=$2",
            event_id,
            recipient_email,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_email}")))?;
        Ok(row.retry_count)
    }

    /// Expose the underlying pool for health checks.
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}
