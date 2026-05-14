//! Clap argument definitions for every subcommand.

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "ns",
    about = "notification-service operations CLI",
    version,
    propagate_version = true
)]
pub struct Cli {
    /// Path to a config file (TOML).
    /// Defaults to config/default.toml then config/local.toml, same as the service.
    #[arg(long, short, global = true, env = "NS_CLI_CONFIG")]
    pub config: Option<String>,

    /// Output format.
    #[arg(long, short, global = true, default_value = "table", value_parser = ["table", "json"])]
    pub output: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Publish a new email event directly to RabbitMQ.
    Send(SendArgs),

    /// Show delivery status for an event (all recipients or one).
    Status(StatusArgs),

    /// Reset FAILED recipient(s) to PENDING and re-enqueue.
    Retry(RetryArgs),

    /// List recent email_log rows with optional filters.
    Logs(LogsArgs),

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
    /// Event type, e.g. ORDER_CONFIRMATION (must have a matching template).
    #[arg(long, short = 't')]
    pub event_type: String,

    /// Recipient email address. Repeat for multiple recipients.
    ///   --to alice@example.com --to bob@example.com
    #[arg(long, required = true)]
    pub to: Vec<String>,

    /// Display name for the recipient(s). Repeat in the same order as --to.
    #[arg(long)]
    pub name: Vec<String>,

    /// Template payload as a JSON string or a path to a JSON file (prefix with @).
    ///   --payload '{"orderId":"123"}'
    ///   --payload @/path/to/payload.json
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
    /// URL of the running notification-service HTTP API.
    /// Falls back to http://localhost:<http.port> from config.
    #[arg(long)]
    pub api_url: Option<String>,
}
