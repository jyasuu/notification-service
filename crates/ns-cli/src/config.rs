//! Config loader for the CLI.
//!
//! Reads the same config/default.toml + config/local.toml + NS__ env vars
//! as the service so the CLI works without extra setup.

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct CliConfig {
    pub database: DatabaseConfig,
    pub amqp: AmqpConfig,
    pub http: HttpConfig,
    /// Outbox DB URL — only needed for `ns outbox`.
    pub outbox_database_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
    pub api_key: Option<String>,
}

/// Load config from files + environment variables.
///
/// If `path` is supplied it is loaded instead of the default file pair.
pub fn load(path: Option<&str>) -> Result<CliConfig> {
    let mut builder = config::Config::builder()
        .set_default("http.port", 8080)?
        .set_default("amqp.exchange", "notifications")?
        .set_default("amqp.routing_key", "email.requested")?;

    if let Some(p) = path {
        builder = builder.add_source(config::File::with_name(p).required(true));
    } else {
        builder = builder
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false));
    }

    let cfg = builder
        .add_source(config::Environment::with_prefix("NS").separator("__"))
        .build()
        .context("Failed to load config")?
        .try_deserialize::<CliConfig>()
        .context("Failed to parse config")?;

    Ok(cfg)
}
