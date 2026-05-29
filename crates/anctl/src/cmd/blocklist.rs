//! `anctl blocklist` — manage the runtime block/allow-list via the HTTP API.
//!
//! All sub-commands call the `/admin/blocklist` endpoints, so the service
//! must be running and the CLI must be configured with a valid `http.api_key`.
//!
//! ```
//! anctl blocklist list
//! anctl blocklist add  --kind blocked_email  --value spam@example.com  [--reason "opt-out"]
//! anctl blocklist add  --kind blocked_domain --value example.com
//! anctl blocklist add  --kind allowed_email  --value vip@partner.com
//! anctl blocklist add  --kind allowed_domain --value trusted.org
//! anctl blocklist remove <id>
//! anctl blocklist flush          # evict cache (lazy reload on next check)
//! anctl blocklist reload         # evict + eagerly reload cache from DB
//! ```

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Serialize;
use tabled::Tabled;

use crate::{
    cli::{BlocklistAction, BlocklistArgs},
    config::CliConfig,
    output,
};

#[derive(Debug, Serialize, Tabled)]
struct BlocklistRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Kind")]
    kind: String,
    #[tabled(rename = "Value")]
    value: String,
    #[tabled(rename = "Reason")]
    reason: String,
    #[tabled(rename = "Created")]
    created_at: String,
}

pub async fn run(args: BlocklistArgs, cfg: CliConfig, fmt: crate::cli::OutputFormat) -> Result<()> {
    let base = cfg.api_base_url();
    let client = Client::new();

    // Attach bearer auth to every request if an API key is configured.
    let auth = cfg.http.api_key.clone();
    let authed = |req: reqwest::RequestBuilder| -> reqwest::RequestBuilder {
        match &auth {
            Some(key) => req.bearer_auth(key),
            None => req,
        }
    };

    match args.action {
        // ── list ──────────────────────────────────────────────────────────────
        BlocklistAction::List => {
            let resp = authed(client.get(format!("{base}/admin/blocklist")))
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("List failed (HTTP {status}): {body}");
            }
            let entries: Vec<serde_json::Value> =
                resp.json().await.context("Failed to parse response")?;
            if entries.is_empty() {
                println!("(no active block/allow-list entries)");
                return Ok(());
            }
            let rows: Vec<BlocklistRow> = entries
                .iter()
                .map(|e| BlocklistRow {
                    id: e["id"].as_i64().unwrap_or(0),
                    kind: e["kind"].as_str().unwrap_or("").to_string(),
                    value: e["value"].as_str().unwrap_or("").to_string(),
                    reason: e["reason"].as_str().unwrap_or("").to_string(),
                    created_at: e["created_at"].as_str().unwrap_or("").to_string(),
                })
                .collect();
            match fmt {
                crate::cli::OutputFormat::Json => output::print_json(&entries),
                crate::cli::OutputFormat::Table => output::print_table(&rows),
            }
        }

        // ── add ───────────────────────────────────────────────────────────────
        BlocklistAction::Add {
            kind,
            value,
            reason,
        } => {
            let body = serde_json::json!({
                "kind":   kind,
                "value":  value,
                "reason": reason,
            });
            let resp = authed(client.post(format!("{base}/admin/blocklist")))
                .json(&body)
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            let json: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            if status.is_success() {
                let id = json["id"].as_i64().unwrap_or(0);
                println!("✓ Entry added (id={id}): {kind} '{value}'");
            } else {
                bail!(
                    "Add failed (HTTP {status}): {}",
                    json["error"].as_str().unwrap_or("unknown error")
                );
            }
        }

        // ── remove ────────────────────────────────────────────────────────────
        BlocklistAction::Remove { id } => {
            let resp = authed(client.delete(format!("{base}/admin/blocklist/{id}")))
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            if status.is_success() {
                println!("✓ Entry {id} removed (soft-deleted).");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Remove failed (HTTP {status}): {body}");
            }
        }

        // ── flush (lazy evict) ────────────────────────────────────────────────
        BlocklistAction::Flush => {
            let resp = authed(client.delete(format!("{base}/admin/blocklist/cache")))
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            if status.is_success() {
                println!("✓ Block-list cache evicted. Next check will reload from DB.");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Flush failed (HTTP {status}): {body}");
            }
        }

        // ── reload (evict + eager pre-warm) ───────────────────────────────────
        BlocklistAction::Reload => {
            let resp = authed(client.post(format!("{base}/admin/blocklist/cache")))
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            let json: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            if status.is_success() {
                let count = json["entry_count"].as_u64().unwrap_or(0);
                println!("✓ Block-list cache reloaded ({count} active entries).");
            } else {
                bail!(
                    "Reload failed (HTTP {status}): {}",
                    json["error"].as_str().unwrap_or("unknown error")
                );
            }
        }
    }

    Ok(())
}
