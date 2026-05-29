//! Read-only query helpers used exclusively by the `anctl` binary.
//!
//! These functions encapsulate the `sqlx::query!` call sites that previously
//! lived in `anctl/src/cmd/{logs,status,template}.rs`.  Moving them here
//! means `cargo sqlx prepare` only needs to run in one place (the `store`
//! crate) and `anctl` no longer carries a direct `sqlx` dependency.
//!
//! All functions accept a `&PgPool` so callers remain in control of how the
//! pool is created and torn down.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

// ── Shared result types ───────────────────────────────────────────────────────

/// One row returned by [`list_notification_logs`].
#[derive(Debug)]
pub struct NotificationLogRow {
    pub event_id: String,
    pub event_type: String,
    pub recipient_email: String,
    pub status: String,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// One row returned by [`get_status_for_event`] / [`get_status_for_recipient`].
#[derive(Debug)]
pub struct RecipientStatusRow {
    pub recipient_email: String,
    pub status: String,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// One row returned by [`list_templates`].
#[derive(Debug)]
pub struct TemplateListRow {
    pub event_type: String,
    pub channel: String,
    pub subject: String,
    pub version: i32,
    pub active: bool,
    pub updated_at: DateTime<Utc>,
}

/// One row returned by [`show_template`].
#[derive(Debug)]
pub struct TemplateDetailRow {
    pub event_type: String,
    pub channel: String,
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
    pub version: i32,
    pub active: bool,
    pub updated_at: DateTime<Utc>,
}

/// One row returned by [`list_outbox_rows`].
#[derive(Debug)]
pub struct OutboxRow {
    pub event_id: String,
    pub event_type: String,
    pub status: String,
    pub fail_count: i32,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

// ── notification_log queries ──────────────────────────────────────────────────

/// Return recent `notification_log` rows for the email channel.
///
/// When `status` is `Some`, the query uses an equality filter on the status
/// column (Postgres can use an index).  When `None` the status predicate is
/// omitted entirely rather than using `ILIKE '%'`, which would suppress index
/// use on large tables.
///
/// `event_type_filter` and `email_filter` are ILIKE patterns; pass `%<value>%`
/// from the caller (or `%` for "match all").
pub async fn list_notification_logs(
    pool: &PgPool,
    status: Option<&str>,
    event_type_filter: &str,
    email_filter: &str,
    limit: i64,
) -> Result<Vec<NotificationLogRow>, sqlx::Error> {
    // sqlx::query! generates a unique anonymous struct per call site, so the
    // two branches each map directly to Vec<NotificationLogRow>.
    if let Some(status) = status {
        let rows = sqlx::query!(
            r#"SELECT n.event_id, n.event_type, n.recipient_id AS recipient_email,
                      n.status, n.retry_count, n.last_error, n.updated_at
               FROM   notification_log n
               WHERE  n.channel        = 'email'
                 AND  n.status         = $1
                 AND  n.event_type     ILIKE $2
                 AND  n.recipient_id   ILIKE $3
               ORDER  BY n.updated_at DESC
               LIMIT  $4"#,
            status,
            event_type_filter,
            email_filter,
            limit,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| NotificationLogRow {
                event_id: r.event_id.to_string(),
                event_type: r.event_type,
                recipient_email: r.recipient_email,
                status: r.status,
                retry_count: r.retry_count,
                last_error: r.last_error,
                updated_at: r.updated_at,
            })
            .collect())
    } else {
        let rows = sqlx::query!(
            r#"SELECT n.event_id, n.event_type, n.recipient_id AS recipient_email,
                      n.status, n.retry_count, n.last_error, n.updated_at
               FROM   notification_log n
               WHERE  n.channel        = 'email'
                 AND  n.event_type     ILIKE $1
                 AND  n.recipient_id   ILIKE $2
               ORDER  BY n.updated_at DESC
               LIMIT  $3"#,
            event_type_filter,
            email_filter,
            limit,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| NotificationLogRow {
                event_id: r.event_id.to_string(),
                event_type: r.event_type,
                recipient_email: r.recipient_email,
                status: r.status,
                retry_count: r.retry_count,
                last_error: r.last_error,
                updated_at: r.updated_at,
            })
            .collect())
    }
}

/// Return all email delivery rows for a given event, ordered by creation time.
pub async fn get_status_for_event(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<Vec<RecipientStatusRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT n.recipient_id AS recipient_email, n.status,
                  n.retry_count, n.last_error, n.updated_at
           FROM notification_log n
           WHERE n.event_id = $1
             AND n.channel  = 'email'
           ORDER BY n.created_at"#,
        event_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| RecipientStatusRow {
            recipient_email: r.recipient_email,
            status: r.status,
            retry_count: r.retry_count,
            last_error: r.last_error,
            updated_at: r.updated_at,
        })
        .collect())
}

/// Return the email delivery row for a single recipient within an event.
///
/// Returns `Ok(None)` when no matching row exists.
pub async fn get_status_for_recipient(
    pool: &PgPool,
    event_id: Uuid,
    email: &str,
) -> Result<Option<RecipientStatusRow>, sqlx::Error> {
    let row = sqlx::query!(
        r#"SELECT n.recipient_id AS recipient_email, n.status,
                  n.retry_count, n.last_error, n.updated_at
           FROM notification_log n
           WHERE n.event_id    = $1
             AND n.channel     = 'email'
             AND n.recipient_id = $2"#,
        event_id,
        email,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| RecipientStatusRow {
        recipient_email: r.recipient_email,
        status: r.status,
        retry_count: r.retry_count,
        last_error: r.last_error,
        updated_at: r.updated_at,
    }))
}

// ── notification_template queries ─────────────────────────────────────────────

/// List all templates ordered by type then channel.
pub async fn list_templates(pool: &PgPool) -> Result<Vec<TemplateListRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT type, channel, subject, version, active, updated_at
           FROM   notification_template
           ORDER  BY type, channel"#
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TemplateListRow {
            event_type: r.r#type,
            channel: r.channel,
            subject: r.subject,
            version: r.version,
            active: r.active,
            updated_at: r.updated_at,
        })
        .collect())
}

/// Return all channel variants for a single event type.
pub async fn show_template(
    pool: &PgPool,
    event_type: &str,
) -> Result<Vec<TemplateDetailRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT type, channel, subject, body_html, body_text, version, active, updated_at
           FROM   notification_template
           WHERE  type = $1
           ORDER  BY channel"#,
        event_type,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TemplateDetailRow {
            event_type: r.r#type,
            channel: r.channel,
            subject: r.subject,
            body_html: r.body_html,
            body_text: r.body_text,
            version: r.version,
            active: r.active,
            updated_at: r.updated_at,
        })
        .collect())
}

// ── outbox queries ────────────────────────────────────────────────────────────

/// Return recent outbox rows filtered by status.
///
/// This query runs against a *separate* database (the business-service outbox
/// DB), so the caller must provide a pool connected to that URL rather than
/// the main notification DB.
pub async fn list_outbox_rows(
    pool: &PgPool,
    status: &str,
    limit: i64,
) -> Result<Vec<OutboxRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT event_id, event_type, status, fail_count, payload, created_at, published_at
           FROM   outbox
           WHERE  status = $1
           ORDER  BY created_at DESC
           LIMIT  $2"#,
        status,
        limit,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| OutboxRow {
            event_id: r.event_id.to_string(),
            event_type: r.event_type,
            status: r.status,
            fail_count: r.fail_count,
            payload: r.payload,
            created_at: r.created_at,
            published_at: r.published_at,
        })
        .collect())
}
