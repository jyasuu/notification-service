//! Config loader for the CLI.
//!
//! Reads the same config/default.toml + config/local.toml + AN__ env vars
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
    /// Base URL of the running anvil-notify HTTP API.
    /// Defaults to `http://localhost:<http.port>`.
    /// Override with `AN__API_URL` or `api_url` in config/local.toml when
    /// the service is not on the same host as the CLI (e.g. staging, k8s).
    pub api_url: Option<String>,
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

impl CliConfig {
    /// Base URL for the anvil-notify HTTP API.
    ///
    /// Returns `api_url` from config when set, otherwise falls back to
    /// `http://localhost:<http.port>`.  All HTTP-calling commands (`retry`,
    /// `template flush`, `health`) should use this instead of constructing
    /// the URL themselves so the target host is consistent and configurable.
    pub fn api_base_url(&self) -> String {
        self.api_url
            .clone()
            .unwrap_or_else(|| format!("http://localhost:{}", self.http.port))
    }
}

/// Load config from files + environment variables.
///
/// If `path` is supplied it is loaded instead of the default file pair.
pub fn load(path: Option<&str>) -> Result<CliConfig> {
    let mut builder = config::Config::builder()
        .set_default("http.port", 8080)?
        .set_default("amqp.exchange", "anvil-notify")?
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
