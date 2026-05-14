//! `ns outbox` — inspect the business-service outbox table.
//!
//! Requires `outbox_database_url` to be configured (same setting that
//! enables the built-in outbox worker).

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use crate::{cli::OutboxArgs, config::CliConfig, output};

#[derive(Debug, Serialize, Tabled)]
struct OutboxRow {
    #[tabled(rename = "Event ID")]
    event_id: String,
    #[tabled(rename = "Type")]
    event_type: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Fails")]
    fail_count: i32,
    #[tabled(rename = "Payload (preview)")]
    payload_preview: String,
    #[tabled(rename = "Created")]
    created_at: String,
    #[tabled(rename = "Published")]
    published_at: String,
}

pub async fn run(args: OutboxArgs, cfg: CliConfig) -> Result<()> {
    let db_url = match cfg.outbox_database_url {
        Some(u) => u,
        None => bail!(
            "outbox_database_url is not configured.\n\
             Set NS__OUTBOX_DATABASE_URL or add outbox_database_url to config/local.toml."
        ),
    };

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await?;

    let rows = sqlx::query!(
        r#"SELECT event_id, event_type, status, fail_count, payload, created_at, published_at
           FROM   outbox
           WHERE  status = $1
           ORDER  BY created_at DESC
           LIMIT  $2"#,
        args.status,
        args.limit,
    )
    .fetch_all(&pool)
    .await?;

    if rows.is_empty() {
        println!("(no {} outbox rows)", args.status);
        return Ok(());
    }

    let max_payload = if args.full_payload { usize::MAX } else { 80 };

    let display: Vec<OutboxRow> = rows
        .into_iter()
        .map(|r| {
            let payload_str = r.payload.to_string();
            OutboxRow {
                event_id: r.event_id.to_string(),
                event_type: r.event_type,
                status: r.status,
                fail_count: r.fail_count,
                payload_preview: output::truncate(&payload_str, max_payload),
                created_at: r.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                published_at: r
                    .published_at
                    .map(|t: DateTime<Utc>| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| "—".into()),
            }
        })
        .collect();

    output::print_table(&display);
    Ok(())
}
