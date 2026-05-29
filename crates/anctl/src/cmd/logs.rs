//! `anctl logs` — list recent notification_log rows with optional filters.
//!
//! Queries the notification DB directly via the `store` crate; does not
//! require the HTTP API to be running.

use anyhow::Result;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use store::cli_queries;

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

    let rows = cli_queries::list_notification_logs(
        &pool,
        args.status.as_deref(),
        &type_filter,
        &email_filter,
        args.limit,
    )
    .await?;

    if rows.is_empty() {
        println!("(no results)");
        return Ok(());
    }

    let display: Vec<LogRow> = rows
        .into_iter()
        .map(|r| LogRow {
            event_id: r.event_id,
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
