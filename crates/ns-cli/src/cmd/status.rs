//! `ns status` — show delivery status for an event from the notification DB.
//!
//! Queries email_log directly; does not require the HTTP API to be running.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use crate::{
    cli::{OutputFormat, StatusArgs},
    config::CliConfig,
    output,
};

#[derive(Debug, Serialize, Tabled)]
struct RecipientRow {
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Retries")]
    retry_count: i32,
    #[tabled(rename = "Last Error")]
    last_error: String,
    #[tabled(rename = "Updated")]
    updated_at: String,
}

pub async fn run(args: StatusArgs, cfg: CliConfig, fmt: OutputFormat) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&cfg.database.url)
        .await?;

    if let Some(email) = &args.email {
        // Single recipient
        let row = sqlx::query!(
            r#"SELECT recipient_email, status, retry_count, last_error, updated_at
               FROM email_log
               WHERE event_id = $1 AND recipient_email = $2"#,
            args.event_id,
            email,
        )
        .fetch_optional(&pool)
        .await?;

        match row {
            None => println!("No record found for {email} in event {}", args.event_id),
            Some(r) => {
                let rows = vec![RecipientRow {
                    email: r.recipient_email,
                    status: r.status,
                    retry_count: r.retry_count,
                    last_error: output::opt(&r.last_error),
                    updated_at: fmt_ts(Some(r.updated_at)),
                }];
                match fmt {
                    OutputFormat::Json => output::print_json(&rows),
                    OutputFormat::Table => output::print_table(&rows),
                }
            }
        }
    } else {
        // All recipients
        let rows = sqlx::query!(
            r#"SELECT recipient_email, status, retry_count, last_error, updated_at
               FROM email_log
               WHERE event_id = $1
               ORDER BY created_at"#,
            args.event_id,
        )
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            println!("No records found for event {}", args.event_id);
            return Ok(());
        }

        // Summary (table mode only — JSON consumers can compute their own)
        if matches!(fmt, OutputFormat::Table) {
            let total = rows.len();
            let sent = rows.iter().filter(|r| r.status == "SENT").count();
            let failed = rows.iter().filter(|r| r.status == "FAILED").count();
            let blocked = rows.iter().filter(|r| r.status == "BLOCKED").count();
            let pending = rows.iter().filter(|r| r.status == "PENDING").count();
            println!("Event: {}", args.event_id);
            println!("Total: {total}  Sent: {sent}  Pending: {pending}  Failed: {failed}  Blocked: {blocked}");
            println!();
        }

        let display: Vec<RecipientRow> = rows
            .into_iter()
            .map(|r| RecipientRow {
                email: r.recipient_email,
                status: r.status,
                retry_count: r.retry_count,
                last_error: output::opt(&r.last_error),
                updated_at: fmt_ts(Some(r.updated_at)),
            })
            .collect();

        match fmt {
            OutputFormat::Json => output::print_json(&display),
            OutputFormat::Table => output::print_table(&display),
        }
    }

    Ok(())
}

fn fmt_ts(ts: Option<DateTime<Utc>>) -> String {
    ts.map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "—".into())
}
