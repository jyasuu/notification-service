use chrono::{DateTime, Utc};
use common::{AppError, NotificationLog, NotificationStatus};
use sqlx::PgPool;
use tracing::instrument;
use uuid::Uuid;

// ── Re-export so callers don't need to import two modules ─────────────────────

/// Result of an `insert_pending` call.
#[derive(Debug)]
pub enum InsertResult {
    /// A new row was inserted (first attempt for this recipient).
    Inserted,
    /// The row already existed; carries the current `retry_count` and `status`
    /// so the caller can make terminal-state decisions without a second query.
    Duplicate { retry_count: i32, status: String },
}

/// Email-specific arguments for [`EmailNotificationStore::insert_pending`].
///
/// Contains all the channel-agnostic fields plus every field that is specific
/// to email delivery. Future channels define their own `SmsInsertArgs`,
/// `PushInsertArgs`, etc. alongside this — the `NotificationStore` trait never
/// grows email-only fields.
pub struct EmailInsertPendingArgs<'a> {
    // ── Channel-agnostic core ─────────────────────────────────────────────────
    pub event_id: Uuid,
    pub event_type: &'a str,
    pub recipient_email: &'a str,
    pub payload: &'a serde_json::Value,
    pub event_timestamp: DateTime<Utc>,

    // ── Email-specific ────────────────────────────────────────────────────────
    /// Optional display name for the recipient (e.g. "Alice Smith").
    /// Stored so templates using `{{ name }}` render correctly on retry.
    pub recipient_name: Option<&'a str>,
    /// Per-event From address override as JSONB: `{"email": "...", "name": "..."}`.
    pub from_override: Option<&'a serde_json::Value>,
    /// Attachment URL references as JSONB array.
    pub attachments: Option<&'a serde_json::Value>,
    /// Named SMTP sender account key. `None` → use global [mailer] defaults.
    pub sender_account: Option<&'a str>,
    /// CC recipients as JSONB array of `{"email": "...", "name": "..."}`.
    pub cc: Option<&'a serde_json::Value>,
    /// BCC recipients as JSONB array of `{"email": "...", "name": "..."}`.
    pub bcc: Option<&'a serde_json::Value>,
    /// Delivery mode: `"individual"` or `"group"`.
    /// Stored so `republish_event()` faithfully replays the original mode.
    pub send_mode: &'a str,
    /// Retry strategy for group-mode events: `"whole"` or `"individual"`.
    /// Stored so `republish_event()` faithfully replays the original retry strategy.
    /// `None` means the field was absent (pre-0028 rows); treated as `"whole"` on retry.
    pub group_retry_mode: Option<&'a str>,
}

// ── Channel constants ─────────────────────────────────────────────────────────

pub const CHANNEL_EMAIL: &str = "email";

// ── EventDeliveryDetail ───────────────────────────────────────────────────────

/// Event-level email replay data returned by [`NotificationStore::get_event_delivery_detail`].
///
/// All recipient rows for the same `event_id` carry identical values for these
/// fields — they are event-level properties, not per-recipient ones.  This
/// type surfaces them as a single authoritative record so `republish_event()`
/// can reconstruct the event without fragile `find_map` scanning.
#[derive(Debug, Clone)]
pub struct EventDeliveryDetail {
    pub event_type: String,
    pub payload: serde_json::Value,
    pub event_timestamp: DateTime<Utc>,
    pub earliest_created_at: DateTime<Utc>,
    pub from_override: Option<serde_json::Value>,
    pub sender_account: Option<String>,
    pub send_mode: Option<String>,
    pub group_retry_mode: Option<String>,
    pub attachments: Option<serde_json::Value>,
    pub cc: Option<serde_json::Value>,
    pub bcc: Option<serde_json::Value>,
}

// ── NotificationStore trait ───────────────────────────────────────────────────

/// Channel-agnostic interface that every channel store must implement.
///
/// The consumer processor (`processor.rs`) calls these methods; it never
/// touches the underlying tables directly.  This makes adding a new channel
/// a matter of writing a new implementor — the processor is unchanged.
#[async_trait::async_trait]
pub trait NotificationStore: Send + Sync + 'static {
    /// Insert a PENDING delivery row.
    ///
    /// Returns `InsertResult::Inserted` for a new row, or
    /// `InsertResult::Duplicate { retry_count, status }` when the idempotency
    /// key `(event_id, channel, recipient_id)` already exists.
    async fn insert_pending(
        &self,
        args: &EmailInsertPendingArgs<'_>,
    ) -> Result<InsertResult, AppError>;

    /// Mark a delivery as successfully sent.
    async fn mark_sent(&self, event_id: Uuid, recipient_id: &str) -> Result<(), AppError>;

    /// Record a delivery failure.
    ///
    /// When `exhausted` is true the status transitions to `FAILED` (terminal
    /// until an operator triggers a manual retry).  When false the status
    /// stays `PENDING` and the retry counter is incremented.
    async fn mark_failed(
        &self,
        event_id: Uuid,
        recipient_id: &str,
        error_msg: &str,
        exhausted: bool,
    ) -> Result<(), AppError>;

    /// Mark a delivery as blocked by the recipient filter.
    async fn mark_blocked(
        &self,
        event_id: Uuid,
        recipient_id: &str,
        reason: &str,
    ) -> Result<(), AppError>;

    /// Fetch all delivery rows for an event (one per recipient).
    async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<NotificationLog>, AppError>;

    /// Fetch the row for a single recipient within an event.
    async fn get_by_event_and_recipient(
        &self,
        event_id: Uuid,
        recipient_id: &str,
    ) -> Result<NotificationLog, AppError>;

    /// Reset a single FAILED or BLOCKED row to PENDING for manual replay.
    ///
    /// Accepts both `FAILED` and `BLOCKED` terminal states so operators can
    /// retry a recipient after removing them from the blocklist — previously
    /// a `BLOCKED` row had no API path to retry and required manual SQL.
    async fn reset_for_retry(&self, event_id: Uuid, recipient_id: &str) -> Result<(), AppError>;

    /// Reset ALL FAILED rows for an event to PENDING, returning the
    /// recipient IDs that were actually reset.
    async fn reset_all_failed_for_event(&self, event_id: Uuid) -> Result<Vec<String>, AppError>;

    /// Return the authoritative event-level email replay data for `event_id`.
    ///
    /// All recipient rows for the same event carry identical values for the
    /// event-level fields (payload, from_override, attachments, cc, bcc,
    /// send_mode, group_retry_mode, sender_account).  This method reads them
    /// from the first row and performs a consistency check in debug builds,
    /// surfacing data corruption rather than silently picking an arbitrary
    /// winner with `find_map`.
    ///
    /// Returns `AppError::NotFound` when no rows exist for `event_id`.
    async fn get_event_delivery_detail(
        &self,
        event_id: Uuid,
    ) -> Result<EventDeliveryDetail, AppError>;

    /// Expose the pool for health checks.
    fn pool(&self) -> &PgPool;
}

// ── EmailNotificationStore ────────────────────────────────────────────────────

/// Implements `NotificationStore` for the email channel.
///
/// Writes to both `notification_log` (channel-agnostic) and
/// `email_notification_log` (email-specific detail) inside a single
/// database transaction so the two tables are always consistent.
///
/// The idempotency key is `(event_id, 'email', recipient_email)`.
#[derive(Clone)]
pub struct EmailNotificationStore {
    pool: PgPool,
}

impl EmailNotificationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl NotificationStore for EmailNotificationStore {
    #[instrument(skip(self, args))]
    async fn insert_pending(
        &self,
        args: &EmailInsertPendingArgs<'_>,
    ) -> Result<InsertResult, AppError> {
        let mut tx = self.pool.begin().await?;

        // ── 1. Upsert into notification_log (channel-agnostic) ────────────────
        //
        // The DO UPDATE is a no-op (touches updated_at with its own value) but
        // causes PostgreSQL to return the row via RETURNING even on conflict.
        // xmax <> 0 distinguishes a fresh insert from a conflict row.
        let row = sqlx::query!(
            r#"
            INSERT INTO notification_log
                (event_id, event_type, channel, recipient_id, payload, event_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id, channel, recipient_id) DO UPDATE
                SET updated_at = notification_log.updated_at
            RETURNING id,
                      retry_count,
                      status,
                      (xmax <> 0) AS "was_conflict!: bool"
            "#,
            args.event_id,
            args.event_type,
            CHANNEL_EMAIL,
            args.recipient_email,
            args.payload,
            args.event_timestamp,
        )
        .fetch_one(&mut *tx)
        .await?;

        if row.was_conflict {
            tx.commit().await?;
            return Ok(InsertResult::Duplicate {
                retry_count: row.retry_count,
                status: row.status,
            });
        }

        // ── 2. Insert email-specific detail row (only on fresh insert) ────────
        sqlx::query!(
            r#"
            INSERT INTO email_notification_log
                (notification_id, recipient_email, recipient_name,
                 from_override, sender_account, send_mode, group_retry_mode,
                 cc, bcc, attachments)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            row.id,
            args.recipient_email,
            args.recipient_name,
            args.from_override,
            args.sender_account,
            args.send_mode,
            args.group_retry_mode,
            args.cc,
            args.bcc,
            args.attachments,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(InsertResult::Inserted)
    }

    #[instrument(skip(self))]
    async fn mark_sent(&self, event_id: Uuid, recipient_id: &str) -> Result<(), AppError> {
        let result = sqlx::query!(
            r#"
            UPDATE notification_log
               SET status = 'SENT',
                   total_attempts = total_attempts + 1,
                   updated_at = now()
             WHERE event_id = $1
               AND channel = $2
               AND recipient_id = $3
            "#,
            event_id,
            CHANNEL_EMAIL,
            recipient_id,
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            tracing::warn!(
                %event_id,
                recipient_id,
                "mark_sent matched no rows — row may have been deleted or recipient_id is wrong"
            );
        }
        Ok(())
    }

    #[instrument(skip(self, error_msg))]
    async fn mark_failed(
        &self,
        event_id: Uuid,
        recipient_id: &str,
        error_msg: &str,
        exhausted: bool,
    ) -> Result<(), AppError> {
        let status = if exhausted { "FAILED" } else { "PENDING" };
        let result = sqlx::query!(
            r#"
            UPDATE notification_log
               SET retry_count    = retry_count + 1,
                   total_attempts = total_attempts + 1,
                   last_error     = $4,
                   status         = $5,
                   updated_at     = now()
             WHERE event_id     = $1
               AND channel      = $2
               AND recipient_id = $3
            "#,
            event_id,
            CHANNEL_EMAIL,
            recipient_id,
            error_msg,
            status,
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            tracing::warn!(
                %event_id,
                recipient_id,
                status,
                "mark_failed matched no rows — row may have been deleted or recipient_id is wrong"
            );
        }
        Ok(())
    }

    #[instrument(skip(self, reason))]
    async fn mark_blocked(
        &self,
        event_id: Uuid,
        recipient_id: &str,
        reason: &str,
    ) -> Result<(), AppError> {
        let result = sqlx::query!(
            r#"
            UPDATE notification_log
               SET status     = 'BLOCKED',
                   last_error = $4,
                   updated_at = now()
             WHERE event_id     = $1
               AND channel      = $2
               AND recipient_id = $3
            "#,
            event_id,
            CHANNEL_EMAIL,
            recipient_id,
            reason,
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            tracing::warn!(
                %event_id,
                recipient_id,
                "mark_blocked matched no rows — row may have been deleted or recipient_id is wrong"
            );
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<NotificationLog>, AppError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                n.id,
                n.event_id,
                n.event_type,
                n.status,
                n.retry_count,
                n.total_attempts,
                n.last_error,
                n.payload,
                n.event_timestamp,
                n.created_at,
                n.updated_at,
                e.recipient_email,
                e.recipient_name,
                e.from_override,
                e.sender_account,
                e.send_mode,
                e.group_retry_mode,
                e.cc,
                e.bcc,
                e.attachments
            FROM notification_log n
            JOIN email_notification_log e ON e.notification_id = n.id
            WHERE n.event_id = $1
              AND n.channel  = $2
            ORDER BY n.created_at
            -- Safety cap: prevents a single pathological event with thousands of
            -- recipients from loading the full result set into the API pod's memory.
            -- 500 rows is well above any legitimate multi-recipient event; a bulk
            -- campaign accidentally routed here would otherwise cause an OOM.
            LIMIT 500
            "#,
            event_id,
            CHANNEL_EMAIL,
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(AppError::NotFound(event_id.to_string()));
        }

        rows.into_iter()
            .map(|r| {
                Ok(NotificationLog {
                    id: r.id,
                    event_id: r.event_id,
                    event_type: r.event_type,
                    channel: CHANNEL_EMAIL.to_owned(),
                    recipient_email: r.recipient_email,
                    recipient_name: r.recipient_name,
                    status: NotificationStatus::try_from(r.status.as_str())?,
                    retry_count: r.retry_count,
                    total_attempts: r.total_attempts,
                    last_error: r.last_error,
                    payload: Some(r.payload),
                    from_override: r.from_override,
                    attachments: r.attachments,
                    sender_account: r.sender_account,
                    cc: r.cc,
                    bcc: r.bcc,
                    send_mode: r.send_mode,
                    group_retry_mode: r.group_retry_mode,
                    event_timestamp: Some(r.event_timestamp),
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                })
            })
            .collect()
    }

    #[instrument(skip(self))]
    async fn get_by_event_and_recipient(
        &self,
        event_id: Uuid,
        recipient_id: &str,
    ) -> Result<NotificationLog, AppError> {
        let r = sqlx::query!(
            r#"
            SELECT
                n.id,
                n.event_id,
                n.event_type,
                n.status,
                n.retry_count,
                n.total_attempts,
                n.last_error,
                n.payload,
                n.event_timestamp,
                n.created_at,
                n.updated_at,
                e.recipient_email,
                e.recipient_name,
                e.from_override,
                e.sender_account,
                e.send_mode,
                e.group_retry_mode,
                e.cc,
                e.bcc,
                e.attachments
            FROM notification_log n
            JOIN email_notification_log e ON e.notification_id = n.id
            WHERE n.event_id    = $1
              AND n.channel     = $2
              AND n.recipient_id = $3
            "#,
            event_id,
            CHANNEL_EMAIL,
            recipient_id,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_id}")))?;

        Ok(NotificationLog {
            id: r.id,
            event_id: r.event_id,
            event_type: r.event_type,
            channel: CHANNEL_EMAIL.to_owned(),
            recipient_email: r.recipient_email,
            recipient_name: r.recipient_name,
            status: NotificationStatus::try_from(r.status.as_str())?,
            retry_count: r.retry_count,
            total_attempts: r.total_attempts,
            last_error: r.last_error,
            payload: Some(r.payload),
            from_override: r.from_override,
            attachments: r.attachments,
            sender_account: r.sender_account,
            cc: r.cc,
            bcc: r.bcc,
            send_mode: r.send_mode,
            group_retry_mode: r.group_retry_mode,
            event_timestamp: Some(r.event_timestamp),
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }

    #[instrument(skip(self))]
    async fn reset_for_retry(&self, event_id: Uuid, recipient_id: &str) -> Result<(), AppError> {
        let result = sqlx::query!(
            r#"
            UPDATE notification_log
               SET status     = 'PENDING',
                   retry_count = 0,
                   last_error  = NULL,
                   updated_at  = now()
             WHERE event_id     = $1
               AND channel      = $2
               AND recipient_id = $3
               AND status       IN ('FAILED', 'BLOCKED')
            "#,
            event_id,
            CHANNEL_EMAIL,
            recipient_id,
        )
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(AppError::NotFound(format!(
                "No FAILED or BLOCKED record for {event_id}/{recipient_id}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn reset_all_failed_for_event(&self, event_id: Uuid) -> Result<Vec<String>, AppError> {
        let rows = sqlx::query!(
            r#"
            UPDATE notification_log
               SET status      = 'PENDING',
                   retry_count = 0,
                   last_error  = NULL,
                   updated_at  = now()
             WHERE event_id = $1
               AND channel  = $2
               AND status   = 'FAILED'
            RETURNING recipient_id
            "#,
            event_id,
            CHANNEL_EMAIL,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.recipient_id).collect())
    }

    #[instrument(skip(self))]
    async fn get_event_delivery_detail(
        &self,
        event_id: Uuid,
    ) -> Result<EventDeliveryDetail, AppError> {
        // Fetch every row so we can assert event-level field consistency.
        // In the normal case this is identical to get_by_event_id but returns
        // only the fields needed for replay, without per-recipient state.
        let rows = sqlx::query!(
            r#"
            SELECT
                n.event_type,
                n.payload,
                n.event_timestamp,
                n.created_at,
                e.from_override,
                e.sender_account,
                e.send_mode,
                e.group_retry_mode,
                e.attachments,
                e.cc,
                e.bcc
            FROM notification_log n
            JOIN email_notification_log e ON e.notification_id = n.id
            WHERE n.event_id = $1
              AND n.channel  = $2
            ORDER BY n.created_at
            "#,
            event_id,
            CHANNEL_EMAIL,
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(AppError::NotFound(event_id.to_string()));
        }

        // The first row is authoritative for all event-level fields.
        let first = &rows[0];

        // ── Consistency assertion (debug builds) ─────────────────────────────
        // Event-level fields must be identical across all rows for the same
        // event_id.  A mismatch indicates data corruption (e.g. manual edits or
        // a bug that wrote different values per row).  We assert in debug mode
        // to surface this during development/testing without adding runtime
        // overhead in production.
        #[cfg(debug_assertions)]
        {
            for row in rows.iter().skip(1) {
                debug_assert_eq!(
                    row.event_type, first.event_type,
                    "event_type mismatch for event_id {event_id}"
                );
                debug_assert_eq!(
                    row.from_override, first.from_override,
                    "from_override mismatch for event_id {event_id}"
                );
                debug_assert_eq!(
                    row.attachments, first.attachments,
                    "attachments mismatch for event_id {event_id}"
                );
                debug_assert_eq!(row.cc, first.cc, "cc mismatch for event_id {event_id}");
                debug_assert_eq!(row.bcc, first.bcc, "bcc mismatch for event_id {event_id}");
                debug_assert_eq!(
                    row.send_mode, first.send_mode,
                    "send_mode mismatch for event_id {event_id}"
                );
                debug_assert_eq!(
                    row.group_retry_mode, first.group_retry_mode,
                    "group_retry_mode mismatch for event_id {event_id}"
                );
                debug_assert_eq!(
                    row.sender_account, first.sender_account,
                    "sender_account mismatch for event_id {event_id}"
                );
            }
        }

        let earliest_created_at = rows
            .iter()
            .map(|r| r.created_at)
            .min()
            .unwrap_or(first.created_at);

        Ok(EventDeliveryDetail {
            event_type: first.event_type.clone(),
            payload: first.payload.clone(),
            event_timestamp: first.event_timestamp,
            earliest_created_at,
            from_override: first.from_override.clone(),
            sender_account: first.sender_account.clone(),
            send_mode: first.send_mode.clone(),
            group_retry_mode: first.group_retry_mode.clone(),
            attachments: first.attachments.clone(),
            cc: first.cc.clone(),
            bcc: first.bcc.clone(),
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}
