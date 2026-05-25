//! `ns logs` — list recent notification_log rows with optional filters.
//!
//! Queries the notification DB directly; does not require the HTTP API.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use crate::{
    cli::{LogsArgs, OutputFormat},
    config::CliConfig,
    output,
};

#[derive(Debug, Serialize, Tabled)]
struct LogRow {
    #[tabled(rename = "Event ID")]
    event_id: String,
    #[tabled(rename = "Type")]
    event_type: String,
    #[tabled(rename = "Recipient")]
    recipient: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Retries")]
    retry_count: i32,
    #[tabled(rename = "Last Error")]
    last_error: String,
    #[tabled(rename = "Updated")]
    updated_at: String,
}

pub async fn run(args: LogsArgs, cfg: CliConfig, fmt: OutputFormat) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&cfg.database.url)
        .await?;

    // Build partial-match filters for the free-text fields.
    let type_filter = format!("%{}%", args.event_type.as_deref().unwrap_or(""));
    let email_filter = format!("%{}%", args.email.as_deref().unwrap_or(""));
    let max_err = if args.full_error { usize::MAX } else { 60 };

    // sqlx::query! generates a unique anonymous struct per call site, so the
    // two branches can't share a single `rows` binding.  Map each branch
    // directly to Vec<LogRow> so the types unify at the outer let.
    //
    // When a status filter is provided, use equality (= $1) so Postgres can
    // use an index on the status column.  When none is provided, omit the
    // WHERE clause on status entirely — ILIKE '%' looks equivalent but
    // prevents index use on large notification_log tables.
    let display: Vec<LogRow> = if let Some(ref status) = args.status {
        sqlx::query!(
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
            type_filter,
            email_filter,
            args.limit,
        )
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|r| {
            log_row(
                r.event_id.to_string(),
                r.event_type,
                r.recipient_email,
                r.status,
                r.retry_count,
                r.last_error,
                r.updated_at,
                max_err,
            )
        })
        .collect()
    } else {
        sqlx::query!(
            r#"SELECT n.event_id, n.event_type, n.recipient_id AS recipient_email,
                      n.status, n.retry_count, n.last_error, n.updated_at
               FROM   notification_log n
               WHERE  n.channel        = 'email'
                 AND  n.event_type     ILIKE $1
                 AND  n.recipient_id   ILIKE $2
               ORDER  BY n.updated_at DESC
               LIMIT  $3"#,
            type_filter,
            email_filter,
            args.limit,
        )
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|r| {
            log_row(
                r.event_id.to_string(),
                r.event_type,
                r.recipient_email,
                r.status,
                r.retry_count,
                r.last_error,
                r.updated_at,
                max_err,
            )
        })
        .collect()
    };

    if display.is_empty() {
        println!("(no results)");
        return Ok(());
    }

    match fmt {
        OutputFormat::Json => output::print_json(&display),
        OutputFormat::Table => output::print_table(&display),
    }
    Ok(())
}

/// Map raw query columns into a [`LogRow`] for display.
fn log_row(
    event_id: String,
    event_type: String,
    recipient_email: String,
    status: String,
    retry_count: i32,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
    max_err: usize,
) -> LogRow {
    LogRow {
        event_id,
        event_type,
        recipient: recipient_email,
        status,
        retry_count,
        last_error: last_error
            .as_deref()
            .map(|e| output::truncate(e, max_err))
            .unwrap_or_else(|| "—".into()),
        updated_at: updated_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    }
}
