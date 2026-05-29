//! `anctl retry` — reset FAILED recipients and re-enqueue via the HTTP API.
//!
//! Uses the service's HTTP retry endpoints so the event is faithfully
//! reconstructed from stored DB data (payload, from_override, attachments).

use anyhow::{bail, Context, Result};
use dialoguer::Confirm;
use reqwest::Client;
use serde_json::Value;

use crate::{cli::RetryArgs, config::CliConfig};

pub async fn run(args: RetryArgs, cfg: CliConfig) -> Result<()> {
    let base = format!("http://localhost:{}", cfg.http.port);
    let client = Client::new();

    // Confirm before resetting
    if !args.yes {
        let target = args.email.as_deref().unwrap_or("ALL failed recipients");
        let ok = Confirm::new()
            .with_prompt(format!(
                "Reset and re-enqueue {target} for event {}?",
                args.event_id
            ))
            .default(true)
            .interact()
            .context("Prompt failed")?;
        if !ok {
            println!("Aborted.");
            return Ok(());
        }
    }

    let url = match &args.email {
        Some(email) => format!("{base}/emails/{}/recipients/{email}/retry", args.event_id),
        None => format!("{base}/emails/{}/retry", args.event_id),
    };

    let mut req = client.post(&url);

    if let Some(key) = &cfg.http.api_key {
        req = req.bearer_auth(key);
    }

    let resp = req.send().await.context("HTTP request failed")?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);

    if status.is_success() {
        println!("✓ {}", body["message"].as_str().unwrap_or("OK"));
        if let Some(n) = body["recipientsReset"].as_u64() {
            println!("  Recipients reset: {n}");
        }
        if let Some(id) = body["eventId"].as_str() {
            println!("  Event ID        : {id}");
        }
    } else {
        bail!(
            "Retry failed (HTTP {status}): {}",
            body["error"]
                .as_str()
                .or_else(|| body.as_str())
                .unwrap_or("unknown error")
        );
    }

    Ok(())
}
