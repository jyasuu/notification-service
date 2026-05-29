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
    /// Full To: recipient list for group sends with `group_retry_mode = "whole"`.
    ///
    /// Non-NULL only when `send_mode = "group"` AND `group_retry_mode = "whole"`:
    /// that is the only case where a single row tracks multiple recipients.
    /// Individual sends and group/Individual sends each write one row per
    /// recipient, making this column redundant for those modes.
    pub to_recipients: Option<&'a serde_json::Value>,
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

    /// Insert multiple PENDING delivery rows atomically.
    ///
    /// Used by the group-send path (`GroupRetryMode::Individual`) to write all
    /// per-recipient rows in one operation so a mid-batch crash cannot leave
    /// the set partially written.  If any insert fails the entire batch is
    /// rolled back and the caller receives the error.
    ///
    /// The default implementation falls back to sequential `insert_pending`
    /// calls (no atomicity guarantee).  `EmailNotificationStore` overrides
    /// this with a real database transaction.
    async fn insert_pending_batch(
        &self,
        args: &[EmailInsertPendingArgs<'_>],
    ) -> Result<Vec<InsertResult>, AppError> {
        let mut results = Vec::with_capacity(args.len());
        for a in args {
            results.push(self.insert_pending(a).await?);
        }
        Ok(results)
    }

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

    /// Reset PENDING rows that are older than `timeout_secs` to FAILED.
    ///
    /// These are "orphaned" rows: status = PENDING with no AMQP message to
    /// drive them forward (e.g. after a broker blip during a retry call).
    /// Returns the number of rows updated.
    async fn reap_stale_pending(&self, timeout_secs: u64) -> Result<u64, AppError>;

    /// Mark a delivery as blocked by the recipient filter.
    async fn mark_blocked(
        &self,
        event_id: Uuid,
        recipient_id: &str,
        reason: &str,
    ) -> Result<(), AppError>;

    /// Record a terminal skip for an event-level validation failure.
    ///
    /// Written when the consumer ACKs a delivery without attempting to send:
    /// no email `channel_overrides`, empty recipient list, or recipient count
    /// exceeding `max_recipients_per_event`.
    ///
    /// `reason` is a short human-readable description stored in `last_error`
    /// so operators can diagnose the skip via the status API.
    ///
    /// Unlike `mark_failed`, `SKIPPED` rows are **not** eligible for the
    /// manual retry API — the publisher must re-publish with corrected data.
    async fn mark_skipped(
        &self,
        event_id: Uuid,
        event_type: &str,
        recipient_id: &str,
        reason: &str,
        event_timestamp: chrono::DateTime<chrono::Utc>,
        payload: &serde_json::Value,
    ) -> Result<(), AppError>;

    /// Fetch all delivery rows for an event (one per recipient).
    async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<NotificationLog>, AppError>;

    /// Fetch delivery rows for an event, optionally filtered to a subset of
    /// recipient addresses.
    ///
    /// When `only_emails` is `Some`, only rows whose `recipient_id` is in the
    /// set are returned — avoids loading every row for the event when only a
    /// single recipient is being retried.  Pass `None` to return all rows
    /// (equivalent to `get_by_event_id`).
    async fn get_recipients_for_event(
        &self,
        event_id: Uuid,
        only_emails: Option<&[String]>,
    ) -> Result<Vec<NotificationLog>, AppError>;

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
                 cc, bcc, attachments, to_recipients)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
            args.to_recipients,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(InsertResult::Inserted)
    }

    /// Transactional override: all rows in one `BEGIN`/`COMMIT`.
    ///
    /// Runs every pair of `(notification_log, email_notification_log)` inserts
    /// inside a single outer transaction so the batch is written atomically.
    /// A crash between any two rows rolls back the entire set; on AMQP
    /// redelivery the `Duplicate` path in `insert_pending` handles individual
    /// rows that were committed in a prior partial attempt.
    #[instrument(skip(self, args), fields(count = args.len()))]
    async fn insert_pending_batch(
        &self,
        args: &[EmailInsertPendingArgs<'_>],
    ) -> Result<Vec<InsertResult>, AppError> {
        let mut tx = self.pool.begin().await?;
        let mut results = Vec::with_capacity(args.len());

        for a in args {
            // ── notification_log ──────────────────────────────────────────────
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
                a.event_id,
                a.event_type,
                CHANNEL_EMAIL,
                a.recipient_email,
                a.payload,
                a.event_timestamp,
            )
            .fetch_one(&mut *tx)
            .await?;

            if row.was_conflict {
                results.push(InsertResult::Duplicate {
                    retry_count: row.retry_count,
                    status: row.status,
                });
                continue;
            }

            // ── email_notification_log ────────────────────────────────────────
            sqlx::query!(
                r#"
                INSERT INTO email_notification_log
                    (notification_id, recipient_email, recipient_name,
                     from_override, sender_account, send_mode, group_retry_mode,
                     cc, bcc, attachments, to_recipients)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                row.id,
                a.recipient_email,
                a.recipient_name,
                a.from_override,
                a.sender_account,
                a.send_mode,
                a.group_retry_mode,
                a.cc,
                a.bcc,
                a.attachments,
                a.to_recipients,
            )
            .execute(&mut *tx)
            .await?;

            results.push(InsertResult::Inserted);
        }

        tx.commit().await?;
        Ok(results)
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

    /// Insert a terminal SKIPPED row into `notification_log`.
    ///
    /// Unlike the other `mark_*` methods this does an INSERT rather than an
    /// UPDATE — SKIPPED rows are written at the moment of the skip decision,
    /// before any PENDING row exists.  We use `ON CONFLICT DO NOTHING` so a
    /// re-delivered duplicate message (broker re-delivery before ACK arrived)
    /// does not fail; the original SKIPPED row is preserved and the consumer
    /// can safely ACK the duplicate.
    ///
    /// `email_notification_log` is intentionally NOT written: SKIPPED events
    /// have no delivery detail worth storing — no recipient was contacted and
    /// no template was rendered.
    #[instrument(skip(self, reason, payload))]
    async fn mark_skipped(
        &self,
        event_id: Uuid,
        event_type: &str,
        recipient_id: &str,
        reason: &str,
        event_timestamp: chrono::DateTime<chrono::Utc>,
        payload: &serde_json::Value,
    ) -> Result<(), AppError> {
        sqlx::query!(
            r#"
            INSERT INTO notification_log
                (event_id, event_type, channel, recipient_id,
                 status, last_error, payload, event_timestamp,
                 retry_count, total_attempts)
            VALUES ($1, $2, $3, $4,
                    'SKIPPED', $5, $6, $7,
                    0, 0)
            ON CONFLICT (event_id, channel, recipient_id) DO NOTHING
            "#,
            event_id,
            event_type,
            CHANNEL_EMAIL,
            recipient_id,
            reason,
            payload,
            event_timestamp,
        )
        .execute(&self.pool)
        .await?;
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
                COALESCE(e.recipient_email, n.recipient_id) AS "recipient_email!",
                e.recipient_name,
                e.from_override,
                e.sender_account,
                e.send_mode,
                e.group_retry_mode,
                e.cc,
                e.bcc,
                e.attachments,
                e.to_recipients
            FROM notification_log n
            LEFT JOIN email_notification_log e ON e.notification_id = n.id
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
                    to_recipients: r.to_recipients,
                    event_timestamp: Some(r.event_timestamp),
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                })
            })
            .collect()
    }

    #[instrument(skip(self, only_emails))]
    async fn get_recipients_for_event(
        &self,
        event_id: Uuid,
        only_emails: Option<&[String]>,
    ) -> Result<Vec<NotificationLog>, AppError> {
        // When a filter is provided, do a targeted query so we don't load
        // every recipient row for the event (high-cardinality events can have
        // hundreds of rows, but a single-recipient retry only needs one).
        // The ANY($3) clause is safe: sqlx encodes &[String] as a Postgres text[].
        match only_emails {
            Some(emails) if !emails.is_empty() => {
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
                        COALESCE(e.recipient_email, n.recipient_id) AS "recipient_email!",
                        e.recipient_name,
                        e.from_override,
                        e.sender_account,
                        e.send_mode,
                        e.group_retry_mode,
                        e.cc,
                        e.bcc,
                        e.attachments,
                        e.to_recipients
                    FROM notification_log n
                    LEFT JOIN email_notification_log e ON e.notification_id = n.id
                    WHERE n.event_id = $1
                      AND n.channel  = $2
                      AND COALESCE(e.recipient_email, n.recipient_id) = ANY($3)
                    ORDER BY n.created_at
                    LIMIT 500
                    "#,
                    event_id,
                    CHANNEL_EMAIL,
                    emails as &[String],
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
                            to_recipients: r.to_recipients,
                            event_timestamp: Some(r.event_timestamp),
                            created_at: r.created_at,
                            updated_at: r.updated_at,
                        })
                    })
                    .collect()
            }
            // No filter — delegate to get_by_event_id to avoid duplicating the query.
            _ => self.get_by_event_id(event_id).await,
        }
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
                COALESCE(e.recipient_email, n.recipient_id) AS "recipient_email!",
                e.recipient_name,
                e.from_override,
                e.sender_account,
                e.send_mode,
                e.group_retry_mode,
                e.cc,
                e.bcc,
                e.attachments,
                e.to_recipients
            FROM notification_log n
            LEFT JOIN email_notification_log e ON e.notification_id = n.id
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
            to_recipients: r.to_recipients,
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
                e.bcc,
                e.to_recipients
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

    async fn reap_stale_pending(&self, timeout_secs: u64) -> Result<u64, AppError> {
        // Mark PENDING rows that are older than `timeout_secs` as FAILED.
        // These are orphaned rows: the AMQP message that should drive them
        // was lost (e.g. a broker blip mid-retry-API call).
        // We use a conservative last_error message so operators understand
        // this is an infrastructure issue, not a delivery failure.
        let result = sqlx::query!(
            r#"
            UPDATE notification_log
               SET status     = 'FAILED',
                   last_error = 'Orphaned by stale-PENDING reaper: PENDING row exceeded timeout with no AMQP message to drive it. Trigger a manual retry when the broker is healthy.',
                   updated_at = now()
             WHERE status     = 'PENDING'
               AND channel    = $1
               AND updated_at < now() - ($2 || ' seconds')::interval
            "#,
            CHANNEL_EMAIL,
            timeout_secs.to_string(),
        )
        .execute(&self.pool)
        .await
        .map_err(AppError::Database)?;
        Ok(result.rows_affected())
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EmailInsertPendingArgs, InsertResult, NotificationStore};
    use chrono::Utc;
    use sqlx::PgPool;
    use uuid::Uuid;

    // `#[sqlx::test]` spins up a temporary Postgres database for each test,
    // runs the crate migrations from `../../migrations` (workspace root),
    // injects a `PgPool`, and tears the DB down afterwards.
    //
    // Requires DATABASE_URL pointing to a Postgres instance with CREATE DATABASE
    // privileges.  In CI this comes from docker-compose; locally:
    //   docker compose up -d db
    //   export DATABASE_URL=postgres://user:pass@localhost/notify
    //   cargo test -p store

    /// Verify the `($2 || ' seconds')::interval` pattern in `reap_stale_pending`
    /// is valid Postgres SQL and that the function returns the correct row count.
    ///
    /// Four rows are inserted:
    ///   - stale PENDING (10 min old)   → should be reaped → FAILED
    ///   - fresh PENDING (just now)     → should NOT be touched
    ///   - pre-existing FAILED (old)    → should NOT be overwritten
    ///   - SENT (old)                   → should NOT be touched
    ///
    /// After `reap_stale_pending(5)` exactly one row should have flipped to
    /// FAILED with the reaper's signature string in `last_error`.
    #[sqlx::test(migrations = "../../migrations")]
    async fn reap_stale_pending_counts_and_leaves_other_rows_alone(pool: PgPool) {
        let store = EmailNotificationStore::new(pool.clone());
        let payload = serde_json::json!({"k": "v"});
        let ts = Utc::now();

        // ── Insert stale PENDING row ──────────────────────────────────────────
        let eid_stale = Uuid::new_v4();
        assert!(matches!(
            store
                .insert_pending(&EmailInsertPendingArgs {
                    event_id: eid_stale,
                    event_type: "test.reap",
                    recipient_email: "stale@example.com",
                    recipient_name: None,
                    payload: &payload,
                    from_override: None,
                    attachments: None,
                    sender_account: None,
                    cc: None,
                    bcc: None,
                    send_mode: "individual",
                    group_retry_mode: None,
                    to_recipients: None,
                    event_timestamp: ts,
                })
                .await
                .unwrap(),
            InsertResult::Inserted
        ));
        // Back-date to simulate the row being stuck for 10 minutes.
        sqlx::query!(
            "UPDATE notification_log SET updated_at = now() - interval '10 minutes' \
             WHERE event_id = $1 AND recipient_id = $2",
            eid_stale,
            "stale@example.com",
        )
        .execute(&pool)
        .await
        .unwrap();

        // ── Insert fresh PENDING row (should not be reaped) ───────────────────
        let eid_fresh = Uuid::new_v4();
        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: eid_fresh,
                event_type: "test.reap",
                recipient_email: "fresh@example.com",
                recipient_name: None,
                payload: &payload,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
                event_timestamp: ts,
            })
            .await
            .unwrap();

        // ── Insert a row, back-date it, then transition to pre-existing FAILED ─
        let eid_failed = Uuid::new_v4();
        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: eid_failed,
                event_type: "test.reap",
                recipient_email: "failed@example.com",
                recipient_name: None,
                payload: &payload,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
                event_timestamp: ts,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE notification_log SET updated_at = now() - interval '10 minutes' \
             WHERE event_id = $1",
            eid_failed,
        )
        .execute(&pool)
        .await
        .unwrap();
        store
            .mark_failed(
                eid_failed,
                "failed@example.com",
                "pre-existing failure",
                true,
            )
            .await
            .unwrap();

        // ── Insert a row, back-date it, then mark SENT ────────────────────────
        let eid_sent = Uuid::new_v4();
        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: eid_sent,
                event_type: "test.reap",
                recipient_email: "sent@example.com",
                recipient_name: None,
                payload: &payload,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
                event_timestamp: ts,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE notification_log SET updated_at = now() - interval '10 minutes' \
             WHERE event_id = $1",
            eid_sent,
        )
        .execute(&pool)
        .await
        .unwrap();
        store.mark_sent(eid_sent, "sent@example.com").await.unwrap();

        // ── Run the reaper with a 5-second timeout ────────────────────────────
        let reaped = store.reap_stale_pending(5).await.unwrap();
        assert_eq!(
            reaped, 1,
            "Expected exactly 1 stale row reaped, got {reaped}"
        );

        // Stale row must now be FAILED with the reaper's signature message.
        let row = sqlx::query!(
            "SELECT status, last_error FROM notification_log \
             WHERE event_id = $1 AND recipient_id = $2",
            eid_stale,
            "stale@example.com",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.status, "FAILED");
        assert!(
            row.last_error
                .as_deref()
                .unwrap_or("")
                .contains("stale-PENDING reaper"),
            "Expected reaper signature in last_error; got: {:?}",
            row.last_error,
        );

        // Fresh row must still be PENDING.
        let row = sqlx::query!(
            "SELECT status FROM notification_log WHERE event_id = $1",
            eid_fresh,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row.status, "PENDING",
            "Fresh row should not have been reaped"
        );

        // Pre-existing FAILED row must not have been overwritten.
        let row = sqlx::query!(
            "SELECT status, last_error FROM notification_log WHERE event_id = $1",
            eid_failed,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.status, "FAILED");
        assert!(
            !row.last_error
                .as_deref()
                .unwrap_or("")
                .contains("stale-PENDING reaper"),
            "Pre-existing FAILED last_error should not have been overwritten",
        );

        // SENT row must be untouched.
        let row = sqlx::query!(
            "SELECT status FROM notification_log WHERE event_id = $1",
            eid_sent,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.status, "SENT");
    }

    /// `reap_stale_pending(0)` with a zero-second timeout should not panic
    /// and should produce valid SQL (tests the `|| ' seconds'` interpolation
    /// with 0 as input).
    #[sqlx::test(migrations = "../../migrations")]
    async fn reap_stale_pending_zero_timeout_is_valid_sql(pool: PgPool) {
        let store = EmailNotificationStore::new(pool.clone());
        let payload = serde_json::json!({});
        let ts = Utc::now();
        let eid = Uuid::new_v4();

        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: eid,
                event_type: "test.zero",
                recipient_email: "z@example.com",
                recipient_name: None,
                payload: &payload,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
                event_timestamp: ts,
            })
            .await
            .unwrap();

        // With timeout=0 the interval is `'0 seconds'` = zero, so
        // `updated_at < now()` is true for the row we just inserted.
        // This primarily exercises "is the SQL valid" rather than behaviour.
        let reaped = store.reap_stale_pending(0).await.unwrap();
        assert_eq!(
            reaped, 1,
            "With 0-second timeout, freshly-inserted PENDING row should be reaped"
        );
    }
}
