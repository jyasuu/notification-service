use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use common::AppError;
use reqwest::{Client, StatusCode};
use serde_json::json;
use tracing::{info, instrument, warn};

use crate::{EmailMessage, EmailSender};

pub struct WebhookConfig {
    pub url: String,
    pub auth_token: Option<String>,
}

pub struct WebhookSender {
    client: Client,
    url: String,
    auth_token: Option<String>,
}

impl WebhookSender {
    pub fn new(cfg: WebhookConfig) -> Self {
        Self {
            client: Client::new(),
            url: cfg.url,
            auth_token: cfg.auth_token,
        }
    }
}

/// The JSON body POSTed to the webhook endpoint.
///
/// Attachments are re-encoded as base64 in the JSON body — the receiver
/// gets a self-contained payload and never needs to fetch from a URL.
/// This is safe because the bytes are already in memory from the fetch step.
#[async_trait]
impl EmailSender for WebhookSender {
    #[instrument(skip(self, msg), fields(event_id = %msg.event_id, to = %msg.to_email))]
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError> {
        let attachments: Vec<_> = msg
            .attachments
            .iter()
            .map(|a| {
                json!({
                    "filename":     a.filename,
                    "content_type": a.content_type,
                    // Re-encode fetched bytes as base64 for JSON transport
                    "data":         STANDARD.encode(&a.data),
                })
            })
            .collect();

        let body = json!({
            "event_id":            msg.event_id,
            "to_email":            msg.to_email,
            "to_name":             msg.to_name,
            "subject":             msg.subject,
            "body_html":           msg.body_html,
            "body_text":           msg.body_text,
            "from_email_override": msg.from_email_override,
            "from_name_override":  msg.from_name_override,
            "attachments":         attachments,
        });

        let mut req = self.client.post(&self.url).json(&body);
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AppError::Mailer(e.to_string()))?;

        let status = resp.status();
        if status.is_success() {
            info!(event_id = %msg.event_id, attachments = msg.attachments.len(), "Email dispatched via webhook");
            return Ok(());
        }

        let text = resp.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS {
            warn!(http_status = 429, body = %text, "Webhook rate-limited");
            return Err(AppError::RateLimited(format!("webhook HTTP 429: {text}")));
        }
        if status.is_client_error() {
            return Err(AppError::Mailer(format!(
                "permanent: webhook HTTP {status}: {text}"
            )));
        }
        Err(AppError::Mailer(format!("webhook HTTP {status}: {text}")))
    }
}
