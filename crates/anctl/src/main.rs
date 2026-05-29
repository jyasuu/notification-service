//! anctl — anvil-notify CLI
//!
//! A full operations tool for the anvil-notify.
//! Connects directly to RabbitMQ and PostgreSQL (same config as the service)
//! or to the running HTTP API, depending on the subcommand.
//!
//! # Quick reference
//!
//! ```
//! anctl send    --type ORDER_CONFIRMATION --to alice@example.com [OPTIONS]
//! anctl status  <event-id>
//! anctl retry   <event-id> [--email alice@example.com]
//! anctl logs    [--status FAILED] [--limit 50]
//! anctl outbox  [--status PENDING] [--limit 50]
//! anctl blocklist list
//! anctl blocklist add  --kind blocked_email --value spam@example.com [--reason "opt-out"]
//! anctl blocklist remove <id>
//! anctl blocklist flush
//! anctl blocklist reload
//! anctl template list
//! anctl template show  <event-type>
//! anctl template flush [<event-type>]
//! anctl health
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
        Command::Send(args) => cmd::send::run(*args, cfg).await,
        Command::Status(args) => cmd::status::run(args, cfg, fmt).await,
        Command::Retry(args) => cmd::retry::run(args, cfg).await,
        Command::Logs(args) => cmd::logs::run(*args, cfg, fmt).await,
        Command::Outbox(args) => cmd::outbox::run(args, cfg, fmt).await,
        Command::Blocklist(args) => cmd::blocklist::run(args, cfg, fmt).await,
        Command::Template(args) => cmd::template::run(args, cfg, fmt).await,
        Command::Health(args) => cmd::health::run(args, cfg).await,
    }
}
