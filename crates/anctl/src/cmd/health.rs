//! `anctl health` — check service health and readiness via the HTTP API.

use anyhow::Result;

use crate::{cli::HealthArgs, config::CliConfig};

pub async fn run(args: HealthArgs, cfg: CliConfig) -> Result<()> {
    let base_url = match args.api_url {
        Some(u) => u,
        None => format!("http://localhost:{}", cfg.http.port),
    };
    let base_url = base_url.trim_end_matches('/');

    let mut all_ok = true;

    all_ok &= check_endpoint(&format!("{base_url}/health")).await?;

    if args.ready {
        all_ok &= check_endpoint(&format!("{base_url}/ready")).await?;
    }

    if !all_ok {
        std::process::exit(1);
    }

    Ok(())
}

/// GET a single endpoint, print the result, and return whether it was healthy.
async fn check_endpoint(url: &str) -> Result<bool> {
    let resp = reqwest::get(url).await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("OK  {url}");
        if !body.is_empty() {
            println!("{body}");
        }
        Ok(true)
    } else {
        println!("UNHEALTHY  {url}  (HTTP {status})");
        if !body.is_empty() {
            println!("{body}");
        }
        Ok(false)
    }
}
