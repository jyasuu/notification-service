//! `ns logs` — list recent email_log rows with optional filters.
//!
//! Queries the notification DB directly; does not require the HTTP API.

use anyhow::Result;
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

    // Build a flexible query using ILIKE for partial matches.
    let status_filter = args.status.as_deref().unwrap_or("%");
    let type_filter = format!("%{}%", args.event_type.as_deref().unwrap_or(""));
    let email_filter = format!("%{}%", args.email.as_deref().unwrap_or(""));

    let rows = sqlx::query!(
        r#"SELECT event_id, event_type, recipient_email, status,
                  retry_count, last_error, updated_at
           FROM   email_log
           WHERE  status           ILIKE $1
             AND  event_type       ILIKE $2
             AND  recipient_email  ILIKE $3
           ORDER  BY updated_at DESC
           LIMIT  $4"#,
        status_filter,
        type_filter,
        email_filter,
        args.limit,
    )
    .fetch_all(&pool)
    .await?;

    if rows.is_empty() {
        println!("(no results)");
        return Ok(());
    }

    let max_err = if args.full_error { usize::MAX } else { 60 };

    let display: Vec<LogRow> = rows
        .into_iter()
        .map(|r| LogRow {
            event_id: r.event_id.to_string(),
            event_type: r.event_type,
            recipient: r.recipient_email,
            status: r.status,
            retry_count: r.retry_count,
            last_error: r
                .last_error
                .as_deref()
                .map(|e| output::truncate(e, max_err))
                .unwrap_or_else(|| "—".into()),
            updated_at: r.updated_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        })
        .collect();

    match fmt {
        OutputFormat::Json => output::print_json(&display),
        OutputFormat::Table => output::print_table(&display),
    }
    Ok(())
}
