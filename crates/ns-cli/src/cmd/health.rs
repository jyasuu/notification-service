//! `ns health` — check service health and readiness via the HTTP API.

use anyhow::Result;

use crate::{cli::HealthArgs, config::CliConfig};

pub async fn run(args: HealthArgs, cfg: CliConfig) -> Result<()> {
    let base_url = match args.api_url {
        Some(u) => u,
        None => format!("http://localhost:{}", cfg.http.port),
    };

    let url = format!("{}/health", base_url.trim_end_matches('/'));
    let resp = reqwest::get(&url).await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("OK  {url}");
        if !body.is_empty() {
            println!("{body}");
        }
    } else {
        println!("UNHEALTHY  {url}  (HTTP {status})");
        if !body.is_empty() {
            println!("{body}");
        }
        std::process::exit(1);
    }

    Ok(())
}
