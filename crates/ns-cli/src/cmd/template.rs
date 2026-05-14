//! `ns template` — list, show, and flush email templates via the HTTP API.

use anyhow::Result;

use crate::{
    cli::{TemplateAction, TemplateArgs},
    config::CliConfig,
};

pub async fn run(args: TemplateArgs, cfg: CliConfig) -> Result<()> {
    let base_url = format!("http://localhost:{}", cfg.http.port);
    let client = reqwest::Client::new();

    match args.action {
        TemplateAction::List => {
            let url = format!("{base_url}/templates");
            let resp = client.get(&url).send().await?;
            println!("{}", resp.text().await?);
        }

        TemplateAction::Show { event_type } => {
            let url = format!("{base_url}/templates/{event_type}");
            let resp = client.get(&url).send().await?;
            println!("{}", resp.text().await?);
        }

        TemplateAction::Flush { event_type } => {
            let url = match event_type {
                Some(ref et) => format!("{base_url}/templates/{et}/cache"),
                None => format!("{base_url}/templates/cache"),
            };
            let resp = client.delete(&url).send().await?;
            let status = resp.status();
            println!("{status}  {}", resp.text().await.unwrap_or_default());
        }
    }

    Ok(())
}
