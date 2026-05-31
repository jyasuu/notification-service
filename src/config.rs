use rate_limiter::RateLimitConfig;
use recipient_filter::FilterConfig;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub amqp: AmqpConfig,
    pub mailer: MailerConfig,
    pub http: HttpConfig,
    /// Port for the Prometheus /metrics endpoint (default: 9091).
    /// Keep this separate from the public API port.
    pub metrics_port: Option<u16>,
    /// Outbound rate-limiting to the mail server.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Recipient block/allow-list.
    #[serde(default)]
    pub filter: FilterConfig,
    /// How long resolved templates are cached in memory (seconds).
    #[serde(default = "default_template_cache_ttl_secs")]
    pub template_cache_ttl_secs: u64,
    /// How long the DB-backed block/allow-list snapshot is cached in memory (seconds).
    /// Set to 0 to disable caching (every check hits the DB).
    #[serde(default = "default_block_list_cache_ttl_secs")]
    pub block_list_cache_ttl_secs: u64,
    /// PENDING rows older than this (seconds) are considered orphaned and
    /// reset to FAILED by the stale-PENDING reaper task.
    #[serde(default = "default_stale_pending_timeout_secs")]
    pub stale_pending_timeout_secs: u64,
    /// How often (seconds) the stale-PENDING reaper runs.
    #[serde(default = "default_stale_pending_reaper_interval_secs")]
    pub stale_pending_reaper_interval_secs: u64,
    /// Maximum size of a single fetched email attachment in bytes.
    #[serde(default = "default_max_attachment_bytes")]
    pub max_attachment_bytes: usize,
    /// How long (in seconds) to wait for in-flight tasks to finish during graceful shutdown.
    /// Default: 30 s. Increase if your SMTP server is slow to respond under load,
    /// or if you run with a high max_concurrency. Un-ACK'd AMQP messages are
    /// re-queued by the broker when the connection closes after this timeout.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    /// Named SMTP sender accounts for multi-tenant / multi-brand deployments.
    ///
    /// Each entry gives a business system its own SMTP credentials and From
    /// address. The publisher selects an account by setting `sender_account`
    /// in the `EmailEvent`. When the field is absent or the name is not found,
    /// the service falls back to the global `[mailer]` config.
    ///
    /// config/default.toml example:
    /// ```toml
    /// [sender_accounts.system_a]
    /// host       = "smtp.gmail.com"
    /// port       = 587
    /// username   = "A@example.com"
    /// password   = "app-password-a"
    /// from_email = "A@example.com"
    /// from_name  = "System A"
    ///
    /// [sender_accounts.system_b]
    /// host       = "smtp.sendgrid.net"
    /// port       = 587
    /// username   = "apikey"
    /// password   = "SG.xxxx"
    /// from_email = "B@example.com"
    /// from_name  = "System B"
    /// ```
    #[serde(default)]
    pub sender_accounts: HashMap<String, SmtpAccountConfig>,
}

/// SMTP credentials for a named sender account.
/// All required fields must be present — there is no partial fallback to the global mailer.
///
/// Optional tuning fields (`tls_mode`, `*_timeout_ms`, `pool_size`) default to the
/// same values as the global `[mailer]` SMTP block.
///
/// The `password` field is redacted in `Debug` output so it never appears in
/// log lines even when someone prints the full config for diagnostics.
#[derive(Deserialize, Clone)]
pub struct SmtpAccountConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    pub from_email: String,
    pub from_name: String,
    /// Explicit TLS mode override. Omit to infer from port.
    pub tls_mode: Option<mailer::smtp::SmtpTlsMode>,
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    #[serde(default = "default_smtp_pool_size")]
    pub pool_size: u32,
}

impl std::fmt::Debug for SmtpAccountConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpAccountConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("from_email", &self.from_email)
            .field("from_name", &self.from_name)
            .field("tls_mode", &self.tls_mode)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .field("read_timeout_ms", &self.read_timeout_ms)
            .field("write_timeout_ms", &self.write_timeout_ms)
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}
fn default_block_list_cache_ttl_secs() -> u64 {
    30
}
fn default_stale_pending_timeout_secs() -> u64 {
    // 10 minutes: generous enough to avoid false-positives during slow brokers,
    // tight enough to surface genuine orphans before they age out of operator attention.
    600
}
fn default_stale_pending_reaper_interval_secs() -> u64 {
    300 // Run every 5 minutes.
}

fn default_max_rl_waits() -> u32 {
    5
}

fn default_max_recipients_per_event() -> usize {
    500
}

fn default_template_cache_ttl_secs() -> u64 {
    300
}

fn default_max_attachment_bytes() -> usize {
    10 * 1024 * 1024
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    /// Maximum number of connections in the PostgreSQL connection pool.
    /// Default: 10. Tune based on your Postgres `max_connections` setting
    /// and the number of anvil-notify replicas.
    #[serde(default = "default_db_pool_size")]
    pub pool_size: u32,
}

fn default_db_pool_size() -> u32 {
    10
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    /// Cap on concurrent in-flight message handlers (default: 10).
    pub max_concurrency: usize,
    /// Maximum consecutive rate-limit backoff cycles per recipient (default: 5).
    #[serde(default = "default_max_rl_waits")]
    pub max_rl_waits: u32,
    /// Hard cap on the number of recipients allowed per AMQP event message.
    /// Events exceeding this are NACKed to the DLQ. Default: 500.
    #[serde(default = "default_max_recipients_per_event")]
    pub max_recipients_per_event: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
    /// When set, all `/emails/*` endpoints require `Authorization: Bearer <api_key>`.
    /// Leave unset only when the API is isolated behind a private network.
    /// Override via `AN__HTTP__API_KEY` environment variable.
    pub api_key: Option<String>,
    /// Set to `true` to explicitly acknowledge that the HTTP API runs without
    /// authentication.  When `api_key` is absent and this flag is `false`
    /// (the default), the service refuses to start.
    ///
    /// Override via `AN__HTTP__ALLOW_UNAUTHENTICATED=true` or set
    /// `allow_unauthenticated = true` in `config/local.toml`.  Never set this
    /// in production — use it only for isolated dev/test environments.
    #[serde(default)]
    pub allow_unauthenticated: bool,
}

/// Which email backend to use.
///
/// SMTP fields mirror Spring Boot's `spring.mail.*` namespace.
/// The `backend` discriminant selects the variant.
#[derive(Deserialize, Clone)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum MailerConfig {
    Smtp {
        host: String,
        port: u16,
        #[serde(default)]
        username: String,
        #[serde(default)]
        password: String,
        from_email: String,
        from_name: String,
        /// Explicit TLS mode. Omit to infer from port (Spring Boot default behaviour).
        tls_mode: Option<mailer::smtp::SmtpTlsMode>,
        /// TCP connect timeout in milliseconds. Default: 5000.
        /// `spring.mail.properties.mail.smtp.connectiontimeout`
        #[serde(default = "default_connection_timeout_ms")]
        connection_timeout_ms: u64,
        /// Socket read timeout in milliseconds. Default: 10000.
        /// `spring.mail.properties.mail.smtp.timeout`
        #[serde(default = "default_read_timeout_ms")]
        read_timeout_ms: u64,
        /// Socket write timeout in milliseconds. Default: 10000.
        /// `spring.mail.properties.mail.smtp.writetimeout`
        #[serde(default = "default_write_timeout_ms")]
        write_timeout_ms: u64,
        /// SMTP connection pool size. Default: 5.
        #[serde(default = "default_smtp_pool_size")]
        pool_size: u32,
    },
    Webhook {
        url: String,
        auth_token: Option<String>,
    },
}

fn default_connection_timeout_ms() -> u64 {
    5_000
}
fn default_read_timeout_ms() -> u64 {
    10_000
}
fn default_write_timeout_ms() -> u64 {
    10_000
}
fn default_smtp_pool_size() -> u32 {
    5
}

impl std::fmt::Debug for MailerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailerConfig::Smtp {
                host,
                port,
                username,
                from_email,
                from_name,
                tls_mode,
                connection_timeout_ms,
                read_timeout_ms,
                write_timeout_ms,
                pool_size,
                ..
            } => f
                .debug_struct("MailerConfig::Smtp")
                .field("host", host)
                .field("port", port)
                .field("username", username)
                .field("password", &"[REDACTED]")
                .field("from_email", from_email)
                .field("from_name", from_name)
                .field("tls_mode", tls_mode)
                .field("connection_timeout_ms", connection_timeout_ms)
                .field("read_timeout_ms", read_timeout_ms)
                .field("write_timeout_ms", write_timeout_ms)
                .field("pool_size", pool_size)
                .finish(),
            MailerConfig::Webhook { url, auth_token } => f
                .debug_struct("MailerConfig::Webhook")
                .field("url", url)
                .field("auth_token", &auth_token.as_deref().map(|_| "[REDACTED]"))
                .finish(),
        }
    }
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        // Canonical default values live in config/default.toml — that is the
        // single source of truth.  Do not add set_default() calls here for
        // values that are already present in that file; duplicating them
        // creates two places to update and makes the effective default unclear.
        //
        // The load order (lowest to highest priority):
        //   1. config/default.toml  — checked-in defaults for every field
        //   2. config/local.toml    — developer overrides (gitignored)
        //   3. AN__* env vars       — production / container overrides
        //
        // `serde(default = "...")` fns on struct fields handle defaults for
        // fields that are not present in the TOML files at all (e.g. optional
        // tuning knobs added in later versions).
        let cfg = config::Config::builder()
            // File-based config (optional)
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false))
            // Environment overrides: AN__DATABASE__URL, AN__AMQP__URL, etc.
            .add_source(config::Environment::with_prefix("AN").separator("__"))
            .build()?;

        let app: Self = cfg.try_deserialize()?;
        app.validate()?;
        Ok(app)
    }

    fn validate(&self) -> anyhow::Result<()> {
        use anyhow::bail;

        if self.database.url.is_empty() {
            bail!("database.url must not be empty");
        }
        if self.amqp.url.is_empty() {
            bail!("amqp.url must not be empty");
        }

        match &self.mailer {
            MailerConfig::Smtp {
                host, from_email, ..
            } => {
                if host.is_empty() {
                    bail!("mailer.host must not be empty");
                }
                // username may be empty — that disables credential handshake,
                // which is required for dev catch-all servers (Mailpit, MailHog)
                // that advertise no authentication mechanisms.
                if from_email.is_empty() {
                    bail!("mailer.from_email must not be empty");
                }
            }
            MailerConfig::Webhook { url, .. } => {
                if url.is_empty() {
                    bail!("mailer.url must not be empty");
                }
                if !url.starts_with("http://") && !url.starts_with("https://") {
                    bail!("mailer.url must start with http:// or https://");
                }
            }
        }

        if self.amqp.max_concurrency == 0 || self.amqp.max_concurrency > 1_000 {
            bail!(
                "amqp.max_concurrency must be between 1 and 1000, got {}",
                self.amqp.max_concurrency
            );
        }
        if self.amqp.max_recipients_per_event == 0 {
            bail!("amqp.max_recipients_per_event must be at least 1");
        }

        // Warn when the configured retry_base_ms is so large that even the
        // *first* retry hits the 30-minute in-process cap.  In that case the
        // AMQP consumer-timeout deadline becomes the binding constraint, the
        // un-ACK'd message may be re-queued by the broker, and max_retries
        // has no practical effect.  The check uses the same cap constant as
        // delivery.rs (30 min = 1_800_000 ms) so they stay in sync.
        const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1_000;
        // First retry delay = retry_base_ms * 2^1
        let first_retry_ms = self
            .amqp
            .retry_base_ms
            .saturating_mul(2)
            .min(MAX_RETRY_DELAY_MS);
        if first_retry_ms >= MAX_RETRY_DELAY_MS {
            bail!(
                "amqp.retry_base_ms ({}) is so large that the first retry delay \
                 ({} ms) already hits the 30-minute cap; max_retries has no \
                 practical effect. Reduce retry_base_ms or keep it ≤ 900_000 ms.",
                self.amqp.retry_base_ms,
                first_retry_ms,
            );
        }
        // Also warn (but don't fail) when max_retries * retry_base_ms would
        // require more than 30 minutes total, since that bounds how long an
        // un-ACK'd AMQP message can be held.  Operators who need longer hold
        // times should use an external scheduler rather than in-process retry.
        let max_total_delay_ms: u64 = (1..=self.amqp.max_retries)
            .map(|i| {
                self.amqp
                    .retry_base_ms
                    .saturating_mul(1u64 << (i as u64).min(10))
                    .min(MAX_RETRY_DELAY_MS)
            })
            .sum();
        if max_total_delay_ms > MAX_RETRY_DELAY_MS {
            // anyhow::bail! would abort startup; this is just a heads-up.
            tracing::warn!(
                retry_base_ms = self.amqp.retry_base_ms,
                max_retries = self.amqp.max_retries,
                max_total_delay_secs = max_total_delay_ms / 1_000,
                "amqp.retry_base_ms * 2^max_retries exceeds 30 minutes; \
                 un-ACK'd AMQP messages will be held for up to 30 min per attempt. \
                 Consider reducing retry_base_ms or max_retries."
            );
        }

        // Validate every named sender account at startup so a typo in the
        // config fails fast rather than causing a runtime panic later.
        for (name, acct) in &self.sender_accounts {
            if acct.host.is_empty() {
                bail!("sender_accounts.{name}.host must not be empty");
            }
            // username may be empty for no-auth SMTP servers (see above).
            if acct.from_email.is_empty() {
                bail!("sender_accounts.{name}.from_email must not be empty");
            }
        }

        Ok(())
    }
}
