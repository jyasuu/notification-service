//! ns — notification-service CLI
//!
//! A full operations tool for the notification-service.
//! Connects directly to RabbitMQ and PostgreSQL (same config as the service)
//! or to the running HTTP API, depending on the subcommand.
//!
//! # Quick reference
//!
//! ```
//! ns send    --type ORDER_CONFIRMATION --to alice@example.com [OPTIONS]
//! ns status  <event-id>
//! ns retry   <event-id> [--email alice@example.com]
//! ns logs    [--status FAILED] [--limit 50]
//! ns outbox  [--status PENDING] [--limit 50]
//! ns template list
//! ns template show  <event-type>
//! ns template flush [<event-type>]
//! ns health
//! ```

mod cli;
mod cmd;
mod config;
mod output;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    // Quiet by default — only warnings unless RUST_LOG is set.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let cfg = config::load(cli.config.as_deref())?;
    let fmt = cli.output;

    match cli.command {
        Command::Send(args) => cmd::send::run(args, cfg).await,
        Command::Status(args) => cmd::status::run(args, cfg, fmt).await,
        Command::Retry(args) => cmd::retry::run(args, cfg).await,
        Command::Logs(args) => cmd::logs::run(args, cfg, fmt).await,
        Command::Outbox(args) => cmd::outbox::run(args, cfg, fmt).await,
        Command::Template(args) => cmd::template::run(args, cfg, fmt).await,
        Command::Health(args) => cmd::health::run(args, cfg).await,
    }
}
