//! Clap argument definitions for every subcommand.

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

/// Output format for tabular commands.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table (default).
    #[default]
    Table,
    /// Machine-readable JSON array.
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "ns",
    about = "anvil-notify operations CLI",
    version,
    propagate_version = true
)]
pub struct Cli {
    /// Path to a config file (TOML).
    /// Defaults to config/default.toml then config/local.toml, same as the service.
    #[arg(long, short, global = true, env = "AN_CLI_CONFIG")]
    pub config: Option<String>,

    /// Output format (`table` or `json`).
    #[arg(long, short, global = true, default_value = "table")]
    pub output: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Publish a new email event directly to RabbitMQ.
    Send(Box<SendArgs>),

    /// Show delivery status for an event (all recipients or one).
    Status(StatusArgs),

    /// Reset FAILED recipient(s) to PENDING and re-enqueue.
    Retry(RetryArgs),

    /// List recent notification_log rows with optional filters.
    Logs(Box<LogsArgs>),

    /// Inspect the business-service outbox table.
    Outbox(OutboxArgs),

    /// Manage email templates (list / show / flush cache).
    Template(TemplateArgs),

    /// Check service health and readiness.
    Health(HealthArgs),
}

// ── send ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct SendArgs {
    /// Event type, e.g. ORDER_CONFIRMATION.
    ///
    /// When --subject / --body-html / --body-text are supplied, the CLI
    /// automatically uses the GENERIC_HTML template regardless of this value.
    /// When those flags are absent, a matching template must exist in the DB.
    #[arg(long, short = 't')]
    pub event_type: String,

    /// Recipient email address. Repeat for multiple recipients.
    ///   --to alice@example.com --to bob@example.com
    #[arg(long, required = true)]
    pub to: Vec<String>,

    /// Display name for the recipient(s). Repeat in the same order as --to.
    #[arg(long)]
    pub name: Vec<String>,

    /// CC recipient email address(es). Visible to all recipients as `Cc:` headers.
    /// Repeat for multiple addresses:
    ///   --cc manager@example.com --cc auditor@example.com
    ///
    /// CC addresses are not independently tracked, filtered, or retried.
    #[arg(long)]
    pub cc: Vec<String>,

    /// BCC recipient email address(es). Hidden from other recipients.
    /// Repeat for multiple addresses.
    ///
    /// BCC addresses are not independently tracked, filtered, or retried.
    #[arg(long)]
    pub bcc: Vec<String>,

    /// Template payload as a JSON string or a path to a JSON file (prefix with @).
    ///   --payload '{"orderId":"123"}'
    ///   --payload @/path/to/payload.json
    ///
    /// Ignored when --subject / --body-html / --body-text are provided.
    #[arg(long, short, default_value = "{}")]
    pub payload: String,

    /// From address override (optional), e.g. billing@acme.com
    #[arg(long)]
    pub from_email: Option<String>,

    /// From display name override (optional), e.g. "Acme Billing"
    #[arg(long)]
    pub from_name: Option<String>,

    /// Attachment reference as JSON, e.g.:
    ///   '{"url":"https://...","filename":"inv.pdf","content_type":"application/pdf"}'
    /// Repeat for multiple attachments.
    #[arg(long)]
    pub attachment: Vec<String>,

    /// Source metadata tag (stored as metadata.source in the event).
    #[arg(long, default_value = "ns-cli")]
    pub source: String,

    /// Provide a specific event UUID (for idempotency). Auto-generated when omitted.
    #[arg(long)]
    pub event_id: Option<Uuid>,

    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,

    // ── Generic HTML shorthand (optional) ───────────────────────────────────
    // When all three of --subject, --body-html, --body-text are given, the CLI
    // sets event_type to GENERIC_HTML and populates the payload accordingly.
    // A --payload value is ignored when these flags are present.
    /// Email subject line (plain text).
    ///
    /// Must be provided together with --body-html and --body-text.
    /// When all three are present, sends via the built-in GENERIC_HTML template.
    #[arg(long, requires = "body_html", requires = "body_text")]
    pub subject: Option<String>,

    /// Full HTML body of the email.
    ///
    /// Must be provided together with --subject and --body-text.
    #[arg(long, requires = "subject", requires = "body_text")]
    pub body_html: Option<String>,

    /// Plain-text fallback body of the email.
    ///
    /// Must be provided together with --subject and --body-html.
    #[arg(long, requires = "subject", requires = "body_html")]
    pub body_text: Option<String>,
}

// ── status ────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Event UUID to look up.
    pub event_id: Uuid,

    /// Narrow to a single recipient email.
    #[arg(long, short)]
    pub email: Option<String>,
}

// ── retry ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct RetryArgs {
    /// Event UUID whose FAILED recipients should be reset and re-enqueued.
    pub event_id: Uuid,

    /// Retry only this recipient instead of all FAILED ones.
    #[arg(long, short)]
    pub email: Option<String>,

    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

// ── logs ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Filter by status (PENDING, SENT, FAILED, BLOCKED).
    #[arg(long, short)]
    pub status: Option<String>,

    /// Filter by event type (partial match), e.g. ORDER.
    #[arg(long, short = 't')]
    pub event_type: Option<String>,

    /// Filter by recipient email (partial match).
    #[arg(long, short)]
    pub email: Option<String>,

    /// Maximum rows to return.
    #[arg(long, short, default_value = "25")]
    pub limit: i64,

    /// Show full last_error text (truncated by default).
    #[arg(long)]
    pub full_error: bool,
}

// ── outbox ────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct OutboxArgs {
    /// Filter by status (PENDING, PUBLISHED, FAILED).
    #[arg(long, short, default_value = "PENDING")]
    pub status: String,

    /// Maximum rows to return.
    #[arg(long, short, default_value = "25")]
    pub limit: i64,

    /// Show full payload JSON (truncated by default).
    #[arg(long)]
    pub full_payload: bool,
}

// ── template ──────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct TemplateArgs {
    #[command(subcommand)]
    pub action: TemplateAction,
}

#[derive(Debug, Subcommand)]
pub enum TemplateAction {
    /// List all active templates in the database.
    List,

    /// Show subject + body for one event type.
    Show {
        /// Event type, e.g. ORDER_CONFIRMATION
        event_type: String,
    },

    /// Evict one or all templates from the in-memory cache.
    Flush {
        /// Specific event type to evict. Omit to flush all.
        event_type: Option<String>,
    },
}

// ── health ────────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct HealthArgs {
    /// URL of the running anvil-notify HTTP API.
    /// Falls back to http://localhost:<http.port> from config.
    #[arg(long)]
    pub api_url: Option<String>,
}
