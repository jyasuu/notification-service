//! `ns template` — list, show, and flush notification templates.
//!
//! * `list` and `show` query `notification_template` via the `store` crate —
//!   they do not require the HTTP API to be running.
//! * `flush` calls the HTTP API's DELETE cache endpoints, which do require a
//!   running service.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use store::cli_queries;

use crate::{
    cli::{OutputFormat, TemplateAction, TemplateArgs},
    config::CliConfig,
    output,
};

#[derive(Debug, Serialize, Tabled)]
struct TemplateRow {
    #[tabled(rename = "Type")]
    event_type: String,
    #[tabled(rename = "Channel")]
    channel: String,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Version")]
    version: i32,
    #[tabled(rename = "Active")]
    active: bool,
    #[tabled(rename = "Updated")]
    updated_at: String,
}

pub async fn run(args: TemplateArgs, cfg: CliConfig, fmt: OutputFormat) -> Result<()> {
    match args.action {
        // ── list: read notification_template directly from the DB ─────────────
        TemplateAction::List => {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&cfg.database.url)
                .await
                .context("Failed to connect to database")?;

            let rows = cli_queries::list_templates(&pool).await?;

            if rows.is_empty() {
                println!("(no templates in database)");
                return Ok(());
            }

            let display: Vec<TemplateRow> = rows
                .into_iter()
                .map(|r| TemplateRow {
                    event_type: r.event_type,
                    channel: r.channel,
                    subject: output::truncate(&r.subject, 50),
                    version: r.version,
                    active: r.active,
                    updated_at: r.updated_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                })
                .collect();

            match fmt {
                OutputFormat::Json => output::print_json(&display),
                OutputFormat::Table => output::print_table(&display),
            }
        }

        // ── show: read one template row from the DB ───────────────────────────
        TemplateAction::Show { event_type } => {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&cfg.database.url)
                .await
                .context("Failed to connect to database")?;

            let rows = cli_queries::show_template(&pool, &event_type).await?;

            if rows.is_empty() {
                bail!("No template found for event type '{event_type}'");
            }

            for r in rows {
                println!("Type    : {}", r.event_type);
                println!("Channel : {}", r.channel);
                println!("Version : {}  Active: {}", r.version, r.active);
                println!("Updated : {}", r.updated_at.format("%Y-%m-%d %H:%M:%S UTC"));
                println!();
                println!("Subject :\n{}\n", r.subject);
                println!("HTML body:\n{}\n", r.body_html);
                println!("Text body:\n{}", r.body_text);
                println!("{}", "─".repeat(60));
            }
        }

        // ── flush: call the HTTP API's DELETE cache endpoint ──────────────────
        TemplateAction::Flush { event_type } => {
            let base_url = cfg.api_base_url();
            let client = Client::new();

            let url = match event_type {
                Some(ref et) => format!("{base_url}/templates/{et}/cache"),
                None => format!("{base_url}/templates/cache"),
            };

            let mut req = client.delete(&url);
            if let Some(key) = &cfg.http.api_key {
                req = req.bearer_auth(key);
            }

            let resp = req.send().await.context("HTTP request failed")?;
            let status = resp.status();
            if status.is_success() {
                println!("✓ Template cache flushed (HTTP {status})");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Flush failed (HTTP {status}): {body}");
            }
        }
    }

    Ok(())
}
