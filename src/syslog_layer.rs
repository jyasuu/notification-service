//! Syslog tracing-subscriber layer.
//!
//! Supports three transports, matching nginx's `error_log syslog:` syntax:
//!
//! * **Unix socket** (default) — `LOG_SYSLOG_SERVER` unset or empty.
//!   Writes to `/dev/log` (Linux) or `/var/run/syslog` (macOS).
//!   Zero network overhead; works on any host with a local syslogd.
//!
//! * **Remote UDP** — `LOG_SYSLOG_SERVER=host:port` (e.g. `10.0.0.1:514`).
//!   Sends RFC 3164 datagrams to a remote syslog receiver.
//!   Compatible with rsyslog, syslog-ng, Graylog, and Datadog Log Agent.
//!
//! * **Remote TCP** — `LOG_SYSLOG_SERVER=tcp://host:port`.
//!   Sends RFC 3164 framed messages over TCP.
//!   Use for reliable delivery when UDP loss is unacceptable.
//!
//! # Environment variables
//!
//! | Variable              | Default          | Description                           |
//! |-----------------------|------------------|---------------------------------------|
//! | `LOG_SYSLOG_SERVER`   | (local socket)   | `host:port`, `tcp://host:port`, empty |
//! | `LOG_SYSLOG_FACILITY` | `user`           | Syslog facility name                  |
//! | `LOG_SYSLOG_IDENT`    | `anvil-notify`   | Program identifier in syslog header   |
//!
//! # nginx equivalence
//!
//! nginx's `error_log syslog:server=host:port,facility=local7,tag=nginx` maps to:
//!
//! ```env
//! LOG_FORMAT=syslog
//! LOG_SYSLOG_SERVER=host:port
//! LOG_SYSLOG_FACILITY=local7
//! LOG_SYSLOG_IDENT=nginx
//! ```
//!
//! # Message format
//!
//! Each log line is a single-line JSON object embedded in the syslog message
//! body — the same structured fields as `LOG_FORMAT=json` but delivered via
//! syslog instead of stdout. Plain syslogd stores it as-is; log aggregators
//! (Graylog, Splunk, Datadog) can parse the embedded JSON for field access.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::sync::Mutex;

use syslog::{Facility, Formatter3164, LogFormat, Logger, LoggerBackend, Severity};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

// ── Facility parsing ──────────────────────────────────────────────────────────

/// Parse a syslog facility name (case-insensitive).
///
/// Accepted names mirror those accepted by nginx's `facility=` option and
/// by the syslog(3) POSIX manual: `kern`, `user`, `mail`, `daemon`, `auth`,
/// `syslog`, `lpr`, `news`, `uucp`, `cron`, `local0`–`local7`.
pub fn parse_facility(s: &str) -> Facility {
    match s.to_lowercase().as_str() {
        "kern" => Facility::LOG_KERN,
        "user" => Facility::LOG_USER,
        "mail" => Facility::LOG_MAIL,
        "daemon" => Facility::LOG_DAEMON,
        "auth" => Facility::LOG_AUTH,
        "syslog" => Facility::LOG_SYSLOG,
        "lpr" => Facility::LOG_LPR,
        "news" => Facility::LOG_NEWS,
        "uucp" => Facility::LOG_UUCP,
        "cron" => Facility::LOG_CRON,
        "local0" => Facility::LOG_LOCAL0,
        "local1" => Facility::LOG_LOCAL1,
        "local2" => Facility::LOG_LOCAL2,
        "local3" => Facility::LOG_LOCAL3,
        "local4" => Facility::LOG_LOCAL4,
        "local5" => Facility::LOG_LOCAL5,
        "local6" => Facility::LOG_LOCAL6,
        "local7" => Facility::LOG_LOCAL7,
        other => {
            eprintln!("anvil-notify: unknown syslog facility '{other}', defaulting to 'user'");
            Facility::LOG_USER
        }
    }
}

// ── Severity mapping ──────────────────────────────────────────────────────────

fn to_severity(level: &Level) -> Severity {
    match *level {
        Level::ERROR => Severity::LOG_ERR,
        Level::WARN => Severity::LOG_WARNING,
        Level::INFO => Severity::LOG_INFO,
        Level::DEBUG | Level::TRACE => Severity::LOG_DEBUG,
    }
}

// ── Layer ─────────────────────────────────────────────────────────────────────

/// A [`tracing_subscriber::Layer`] that emits events to syslog.
///
/// Construct via [`SyslogLayer::from_env`]; install with `.with()` on a
/// `tracing_subscriber::Registry`.
pub struct SyslogLayer {
    /// Shared logger — `Logger<LoggerBackend, Formatter3164>` implements `Write`
    /// so we can lock, format, and write in one step.
    logger: Mutex<Logger<LoggerBackend, Formatter3164>>,
}

impl SyslogLayer {
    /// Build a `SyslogLayer` from environment variables.
    ///
    /// Reads `LOG_SYSLOG_SERVER`, `LOG_SYSLOG_FACILITY`, and
    /// `LOG_SYSLOG_IDENT`.  Falls back to sensible defaults when variables
    /// are absent or empty.
    pub fn from_env() -> anyhow::Result<Self> {
        let server = std::env::var("LOG_SYSLOG_SERVER").unwrap_or_default();
        let facility_str = std::env::var("LOG_SYSLOG_FACILITY").unwrap_or_else(|_| "user".into());
        let ident = std::env::var("LOG_SYSLOG_IDENT").unwrap_or_else(|_| "anvil-notify".into());
        let facility = parse_facility(&facility_str);

        Self::new(&server, facility, &ident)
    }

    /// Build a `SyslogLayer` with explicit parameters.
    ///
    /// * `server`   — empty → local Unix socket; `"host:port"` → remote UDP;
    ///   `"tcp://host:port"` → remote TCP.
    /// * `facility` — syslog facility.
    /// * `ident`    — program identifier string in syslog headers.
    pub fn new(server: &str, facility: Facility, ident: &str) -> anyhow::Result<Self> {
        let formatter = Formatter3164 {
            facility,
            hostname: None, // syslog crate fills this in from the host
            process: ident.to_owned(),
            pid: std::process::id(),
        };

        let logger: Logger<LoggerBackend, Formatter3164> = if server.is_empty() {
            // Local Unix socket (/dev/log or /var/run/syslog).
            syslog::unix(formatter)
                .map_err(|e| anyhow::anyhow!("Failed to connect to local syslog socket: {e}"))?
        } else if let Some(addr_str) = server.strip_prefix("tcp://") {
            // Remote TCP.
            syslog::tcp(formatter, addr_str).map_err(|e| {
                anyhow::anyhow!("Failed to connect to syslog TCP server '{addr_str}': {e}")
            })?
        } else {
            // Remote UDP — bind an ephemeral local port.
            syslog::udp(formatter, "0.0.0.0:0", server).map_err(|e| {
                anyhow::anyhow!("Failed to open UDP syslog socket to '{server}': {e}")
            })?
        };

        Ok(Self {
            logger: Mutex::new(logger),
        })
    }

    /// Format a tracing event into a syslog message body.
    ///
    /// Produces a single-line JSON object with `level`, `target`, and all
    /// structured fields recorded on the event and its parent spans.
    fn format_message<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> String
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut fields: BTreeMap<String, String> = BTreeMap::new();

        // Walk parent spans first (root → leaf) so closer spans win on key
        // collisions — same behaviour as the built-in JSON formatter.
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let exts = span.extensions();
                if let Some(recorded) = exts.get::<SpanFields>() {
                    for (k, v) in &recorded.0 {
                        fields.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Event-level fields overwrite span fields.
        let mut visitor = StringVisitor(&mut fields);
        event.record(&mut visitor);

        // Serialise to a compact JSON object.
        let mut out = String::with_capacity(128);
        let _ = write!(
            out,
            "{{\"level\":\"{}\",\"target\":\"{}\"",
            event.metadata().level(),
            event.metadata().target(),
        );
        for (k, v) in &fields {
            // Escape the value as a JSON string.
            let escaped = v
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r");
            let _ = write!(out, ",\"{k}\":\"{escaped}\"");
        }
        out.push('}');
        out
    }

    /// Dispatch `msg` to the syslog backend at the given severity level.
    fn send(&self, severity: Severity, msg: &str) {
        if let Ok(mut logger) = self.logger.lock() {
            // `LogFormat::format` writes directly into the `Write` backend.
            let _ = logger
                .formatter
                .clone()
                .format(&mut logger.backend, severity, msg);
        }
    }
}

// ── Layer implementation ──────────────────────────────────────────────────────

impl<S> Layer<S> for SyslogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// Record span fields into an extension so `format_message` can walk them.
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut fields = SpanFields(BTreeMap::new());
            let mut visitor = StringVisitor(&mut fields.0);
            attrs.record(&mut visitor);
            span.extensions_mut().insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let severity = to_severity(event.metadata().level());
        let msg = Self::format_message(event, &ctx);
        self.send(severity, &msg);
    }
}

// ── Span field storage ────────────────────────────────────────────────────────

/// Span fields stored as a plain `BTreeMap<String, String>` in the span
/// extension registry.  Using strings (not `serde_json::Value`) keeps the
/// syslog layer free of any JSON dep beyond what is already in the workspace.
struct SpanFields(BTreeMap<String, String>);

// ── String field visitor ──────────────────────────────────────────────────────

struct StringVisitor<'a>(&'a mut BTreeMap<String, String>);

impl<'a> tracing::field::Visit for StringVisitor<'a> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}
