use async_trait::async_trait;
use common::AppError;
use lettre::{
    message::{header::ContentType, Attachment as LettreAttachment, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use tracing::{info, instrument, warn};

use crate::{EmailMessage, EmailSender};

pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub from_email: String,
    pub from_name: String,
}

pub struct SmtpSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from_email: String,
    from_name: String,
}

impl SmtpSender {
    pub fn new(cfg: SmtpConfig) -> Result<Self, AppError> {
        let creds = Credentials::new(cfg.username, cfg.password);
        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.host)
            .map_err(|e| AppError::Mailer(e.to_string()))?
            .port(cfg.port)
            .credentials(creds)
            .build();
        Ok(Self {
            transport,
            from_email: cfg.from_email,
            from_name: cfg.from_name,
        })
    }
}

#[async_trait]
impl EmailSender for SmtpSender {
    #[instrument(skip(self, msg), fields(event_id = %msg.event_id, to = %msg.to_email))]
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError> {
        let from_email = msg
            .from_email_override
            .as_deref()
            .unwrap_or(&self.from_email);
        let from_name = msg.from_name_override.as_deref().unwrap_or(&self.from_name);
        let from = format!("{from_name} <{from_email}>")
            .parse()
            .map_err(|e: lettre::address::AddressError| AppError::Mailer(e.to_string()))?;

        let to = match &msg.to_name {
            Some(name) => format!("{name} <{}>", msg.to_email),
            None => msg.to_email.clone(),
        }
        .parse()
        .map_err(|e: lettre::address::AddressError| AppError::Mailer(e.to_string()))?;

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
                    AppError::Mailer(format!(
                        "permanent: attachment '{}' has invalid content-type '{}': {e}",
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

        let email = Message::builder()
            .from(from)
            .to(to)
            .subject(&msg.subject)
            .multipart(body)
            .map_err(|e| AppError::Mailer(e.to_string()))?;

        self.transport
            .send(email)
            .await
            .map_err(classify_smtp_error)?;

        info!(
            event_id    = %msg.event_id,
            attachments = msg.attachments.len(),
            "Email sent via SMTP"
        );
        Ok(())
    }
}

fn classify_smtp_error(err: lettre::transport::smtp::Error) -> AppError {
    let msg = err.to_string();
    if msg.contains("421") || msg.contains("450") || msg.contains("451") || msg.contains("452") {
        warn!(smtp_error = %msg, "SMTP transient 4xx — treating as rate-limited");
        return AppError::RateLimited(format!("SMTP 4xx: {msg}"));
    }
    if msg.contains("535") || msg.contains("534") {
        return AppError::Mailer(format!("permanent: SMTP auth failure: {msg}"));
    }
    if msg.contains(" 55") || msg.contains(" 54") || msg.contains(" 53") {
        return AppError::Mailer(format!("permanent: SMTP 5xx: {msg}"));
    }
    AppError::Mailer(msg)
}

pub fn is_permanent_smtp_error(err: &AppError) -> bool {
    matches!(err, AppError::Mailer(m) if m.starts_with("permanent:"))
}
