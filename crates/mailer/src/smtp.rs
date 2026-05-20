use std::time::Duration;

use async_trait::async_trait;
use common::AppError;
use lettre::{
    message::{
        header::ContentType, Attachment as LettreAttachment, Mailbox, MultiPart, SinglePart,
    },
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use crate::{EmailMessage, EmailSender};

// ── TLS mode ──────────────────────────────────────────────────────────────────

/// Controls how the SMTP connection is secured.
///
/// Mirrors the Spring Boot `spring.mail.properties.mail.smtp.ssl.*` / starttls
/// knobs but expressed as a single enum so misconfiguration is a compile-time
/// (or config parse-time) error rather than a silent no-op.
///
/// **Auto-detection (default):** omit `tls_mode` and AnvilNotify will infer
/// the right mode from the port number, matching the Spring Boot defaults:
///
/// | Port | Inferred mode |
/// |------|---------------|
/// | 465  | `smtps` (implicit TLS)  |
/// | 587 / 25 | `starttls` |
/// | other | `none` — plain, no encryption (dev only) |
///
/// Explicitly setting `tls_mode` overrides port-based inference, which is
/// useful when your provider uses a non-standard port.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SmtpTlsMode {
    /// Implicit TLS from the first byte (port 465 / SMTPS).
    /// Equivalent to `spring.mail.properties.mail.smtp.ssl.enable=true`.
    Smtps,
    /// Upgrade a plain connection to TLS via the STARTTLS command (port 587/25).
    /// Equivalent to `spring.mail.properties.mail.smtp.starttls.enable=true`.
    Starttls,
    /// No encryption. Dev catch-all servers only (Mailpit, MailHog).
    /// Never use in production — credentials are transmitted in the clear.
    None,
}

// ── SmtpConfig ────────────────────────────────────────────────────────────────

/// Full SMTP configuration, modelled after Spring Boot's `spring.mail.*`
/// namespace so the field names are familiar to Java/Kotlin developers.
///
/// TOML equivalent of Spring Boot:
/// ```toml
/// [mailer]
/// backend    = "smtp"
/// host       = "smtp.gmail.com"         # spring.mail.host
/// port       = 587                      # spring.mail.port
/// username   = "user@gmail.com"         # spring.mail.username
/// password   = "app-password"           # spring.mail.password
/// from_email = "no-reply@example.com"   # spring.mail.from  (custom)
/// from_name  = "My App"                 # spring.mail.from_name  (custom)
///
/// # Optional tuning — all have sensible defaults:
/// # tls_mode           = "starttls"     # explicit override; inferred from port by default
/// # connection_timeout_ms = 5000        # spring.mail.properties.mail.smtp.connectiontimeout
/// # read_timeout_ms       = 5000        # spring.mail.properties.mail.smtp.timeout
/// # write_timeout_ms      = 5000        # spring.mail.properties.mail.smtp.writetimeout
/// # pool_size             = 5           # lettre connection-pool size (default: 5)
/// ```
#[derive(Debug, Clone)]
pub struct SmtpConfig {
    // ── Required ──────────────────────────────────────────────────────────────
    /// SMTP server hostname. `spring.mail.host`.
    pub host: String,
    /// SMTP server port. `spring.mail.port`.
    /// Drives TLS-mode auto-detection when `tls_mode` is `None`.
    pub port: u16,
    /// SMTP authentication username. `spring.mail.username`.
    /// Leave empty to skip credential handshake (dev catch-all servers).
    pub username: String,
    /// SMTP authentication password. `spring.mail.password`.
    pub password: String,
    /// Envelope / display From address. Equivalent to `spring.mail.from`.
    pub from_email: String,
    /// Display name shown in the From header alongside `from_email`.
    pub from_name: String,

    // ── Optional / tuning ────────────────────────────────────────────────────
    /// Explicit TLS mode. When `None`, inferred automatically from `port`.
    /// `spring.mail.properties.mail.smtp.ssl.enable` /
    /// `spring.mail.properties.mail.smtp.starttls.enable`.
    pub tls_mode: Option<SmtpTlsMode>,

    /// TCP connection establishment timeout.
    /// `spring.mail.properties.mail.smtp.connectiontimeout`. Default: 5 s.
    pub connection_timeout: Duration,

    /// Socket read timeout (time to wait for server response after sending data).
    /// `spring.mail.properties.mail.smtp.timeout`. Default: 10 s.
    pub read_timeout: Duration,

    /// Socket write timeout.
    /// `spring.mail.properties.mail.smtp.writetimeout`. Default: 10 s.
    pub write_timeout: Duration,

    /// Maximum number of open SMTP connections kept in lettre's connection pool.
    /// Increase for high-throughput deployments; leave at the default (5) for
    /// most services. `spring.mail` has no direct equivalent — this is a
    /// transport-layer pool, not a thread pool.
    pub pool_size: u32,
}

impl SmtpConfig {
    /// Resolve the effective TLS mode: use the explicit override if set,
    /// otherwise infer from the port number (Spring Boot–compatible defaults).
    fn effective_tls_mode(&self) -> SmtpTlsMode {
        if let Some(ref m) = self.tls_mode {
            return m.clone();
        }
        match self.port {
            465 => SmtpTlsMode::Smtps,
            587 | 25 => SmtpTlsMode::Starttls,
            _ => SmtpTlsMode::None,
        }
    }
}

/// Deserializable form of `SmtpConfig`, used by `config.rs` / TOML.
/// All optional fields default to Spring Boot–compatible values.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SmtpConfigRaw {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    pub from_email: String,
    pub from_name: String,
    /// Explicit TLS mode override. Omit to infer from port.
    pub tls_mode: Option<SmtpTlsMode>,
    /// TCP connect timeout in milliseconds. Default: 5000.
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    /// Socket read timeout in milliseconds. Default: 10000.
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    /// Socket write timeout in milliseconds. Default: 10000.
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    /// SMTP connection pool size. Default: 5.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
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
fn default_pool_size() -> u32 {
    5
}

impl From<SmtpConfigRaw> for SmtpConfig {
    fn from(r: SmtpConfigRaw) -> Self {
        Self {
            host: r.host,
            port: r.port,
            username: r.username,
            password: r.password,
            from_email: r.from_email,
            from_name: r.from_name,
            tls_mode: r.tls_mode,
            connection_timeout: Duration::from_millis(r.connection_timeout_ms),
            read_timeout: Duration::from_millis(r.read_timeout_ms),
            write_timeout: Duration::from_millis(r.write_timeout_ms),
            pool_size: r.pool_size,
        }
    }
}

// ── SmtpSender ────────────────────────────────────────────────────────────────

pub struct SmtpSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from_email: String,
    from_name: String,
}

impl SmtpSender {
    pub fn new(cfg: SmtpConfig) -> Result<Self, AppError> {
        let with_creds = !cfg.username.is_empty();
        let mode = cfg.effective_tls_mode();

        // Macro: attach port + optional credentials then build the transport.
        // Used for the two Result-returning builder variants (relay / starttls_relay).
        macro_rules! build_transport {
            ($builder:expr) => {{
                let b = $builder
                    .map_err(|e: lettre::transport::smtp::Error| {
                        AppError::transient_mailer(e.to_string())
                    })?
                    .port(cfg.port)
                    .timeout(Some(cfg.connection_timeout))
                    .pool_config(
                        lettre::transport::smtp::PoolConfig::new().max_size(cfg.pool_size),
                    );
                if with_creds {
                    b.credentials(Credentials::new(cfg.username.clone(), cfg.password.clone()))
                        .build()
                } else {
                    b.build()
                }
            }};
        }

        let transport = match mode {
            SmtpTlsMode::Smtps => {
                // Credentials are only attached when a username is configured.
                // Dev catch-all servers (Mailpit, MailHog) advertise no auth mechanisms
                // and return "No compatible authentication mechanism was found" if the
                // client attempts a credential handshake even with valid credentials.
                build_transport!(AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host))
            }
            SmtpTlsMode::Starttls => {
                build_transport!(AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(
                    &cfg.host
                ))
            }
            SmtpTlsMode::None => {
                // No TLS. Intended for local dev catch-all servers
                // (Mailpit on 1025, MailHog on 1025/2525). If this fires in production
                // you likely have the wrong port configured — 587 (STARTTLS) or 465
                // (implicit TLS) are the correct choices for real SMTP providers.
                warn!(
                    port = cfg.port,
                    host = %cfg.host,
                    "SMTP: using plain (no-TLS) transport on non-standard port. \
                     Intended for local dev only — verify port is correct for production."
                );
                // builder_dangerous does not return a Result, so handle separately.
                let b = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.host)
                    .port(cfg.port)
                    .timeout(Some(cfg.connection_timeout))
                    .pool_config(
                        lettre::transport::smtp::PoolConfig::new().max_size(cfg.pool_size),
                    );
                if with_creds {
                    b.credentials(Credentials::new(cfg.username, cfg.password))
                        .build()
                } else {
                    b.build()
                }
            }
        };

        Ok(Self {
            transport,
            from_email: cfg.from_email,
            from_name: cfg.from_name,
        })
    }
}

#[async_trait]
impl EmailSender for SmtpSender {
    #[instrument(skip(self, msg), fields(event_id = %msg.event_id, to = %msg.to_email, to_extra = msg.to_extra.len()))]
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError> {
        let from_email = msg
            .from_email_override
            .as_deref()
            .unwrap_or(&self.from_email);
        let from_name = msg.from_name_override.as_deref().unwrap_or(&self.from_name);
        let from = format_mailbox(from_email, Some(from_name))
            .map_err(|e| AppError::transient_mailer(e.to_string()))?;

        let to = format_mailbox(&msg.to_email, msg.to_name.as_deref())
            .map_err(|e| AppError::transient_mailer(e.to_string()))?;

        // Parse cc / bcc address lists before touching the body builder so a
        // bad address fails early with a clear permanent error.
        let cc_addrs = msg
            .cc
            .iter()
            .map(|r| format_mailbox(&r.email, r.name.as_deref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::permanent_mailer(format!("invalid cc address: {e}")))?;

        let bcc_addrs = msg
            .bcc
            .iter()
            .map(|r| format_mailbox(&r.email, r.name.as_deref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::permanent_mailer(format!("invalid bcc address: {e}")))?;

        // ── Build text+html alternative ───────────────────────────────────────
        let alternative = MultiPart::alternative()
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(msg.body_text.clone()),
            )
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_HTML)
                    .body(msg.body_html.clone()),
            );

        // ── Wrap in multipart/mixed when attachments are present ──────────────
        //
        // MIME structure:
        //   multipart/mixed          (only when attachments present)
        //     multipart/alternative  (always: text + html)
        //     <attachment parts>…
        //
        // Without attachments we keep the simpler multipart/alternative so
        // mail clients don't show a spurious empty attachments section.
        let body = if msg.attachments.is_empty() {
            alternative
        } else {
            let mut mixed = MultiPart::mixed().multipart(alternative);
            for att in &msg.attachments {
                // Bytes are already fetched and validated by AttachmentFetcher.
                let content_type = att.content_type.parse::<ContentType>().map_err(|e| {
                    AppError::permanent_mailer(format!(
                        "attachment '{}' has invalid content-type '{}': {e}",
                        att.filename, att.content_type
                    ))
                })?;

                mixed = mixed.singlepart(
                    LettreAttachment::new(att.filename.clone())
                        .body(att.data.clone(), content_type),
                );
            }
            mixed
        };

        // ── Parse extra To: addresses for group sends ─────────────────────
        // In individual mode to_extra is empty and this is a no-op.
        let to_extra_addrs = msg
            .to_extra
            .iter()
            .map(|r| format_mailbox(&r.email, r.name.as_deref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::permanent_mailer(format!("invalid to address: {e}")))?;

        let mut builder = Message::builder().from(from).to(to);

        for addr in to_extra_addrs {
            builder = builder.to(addr);
        }
        for addr in cc_addrs {
            builder = builder.cc(addr);
        }
        for addr in bcc_addrs {
            builder = builder.bcc(addr);
        }

        let email = builder
            .subject(&msg.subject)
            .multipart(body)
            .map_err(|e| AppError::transient_mailer(e.to_string()))?;

        self.transport
            .send(email)
            .await
            .map_err(classify_smtp_error)?;

        info!(
            event_id    = %msg.event_id,
            to_extra    = msg.to_extra.len(),
            attachments = msg.attachments.len(),
            cc          = msg.cc.len(),
            bcc         = msg.bcc.len(),
            "Email sent via SMTP"
        );
        Ok(())
    }
}

// ── Address formatting ────────────────────────────────────────────────────────

/// Format an email address as a lettre `Mailbox`.
///
/// When a display name is supplied the result is `"Name <addr>"`;
/// when absent it is just `"addr"`. Returns an `AddressError` on parse failure
/// so callers can map it to the appropriate `AppError` variant.
fn format_mailbox(
    email: &str,
    name: Option<&str>,
) -> Result<Mailbox, lettre::address::AddressError> {
    match name {
        Some(n) if !n.is_empty() => format!("{n} <{email}>").parse(),
        _ => email.parse(),
    }
}

fn classify_smtp_error(err: lettre::transport::smtp::Error) -> AppError {
    // lettre 0.11 exposes boolean predicate methods on Error; the internal
    // ErrorKind enum and kind() accessor are private.
    if err.is_permanent() {
        // 5xx response: bad recipient, auth failure, policy rejection, etc.
        // Will never succeed on retry — route straight to DLQ.
        AppError::permanent_mailer(format!("SMTP {err}"))
    } else if err.is_transient() {
        // 4xx response: server busy, quota, greylisting — retry later.
        warn!(smtp_error = %err, "SMTP 4xx transient — treating as rate-limited");
        AppError::RateLimited(format!("SMTP transient: {err}"))
    } else if err.is_timeout() || err.is_tls() {
        // Network-level failures — transient, worth retrying.
        AppError::transient_mailer(err.to_string())
    } else if err.is_client() {
        // Client-side error (invalid address format, builder error) — permanent.
        AppError::permanent_mailer(format!("SMTP client error: {err}"))
    } else {
        // Unknown / transport shutdown — treat as transient.
        AppError::transient_mailer(err.to_string())
    }
}

pub fn is_permanent_smtp_error(err: &AppError) -> bool {
    err.is_permanent_mailer()
}
