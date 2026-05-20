use std::sync::Arc;

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
    /// Shared HTTP client — callers inject this so the sender participates in
    /// the same connection pool used for attachment fetching rather than
    /// maintaining its own isolated pool.
    client: Arc<Client>,
    url: String,
    auth_token: Option<String>,
}

impl WebhookSender {
    /// Construct a `WebhookSender` with a caller-supplied HTTP client.
    ///
    /// Accepting `Arc<Client>` lets the process share a single connection pool
    /// across attachment fetching and webhook delivery, which avoids opening a
    /// second set of OS-level TCP connections to the same host.
    pub fn new(cfg: WebhookConfig, client: Arc<Client>) -> Self {
        Self {
            client,
            url: cfg.url,
            auth_token: cfg.auth_token,
        }
    }

    /// Convenience constructor that builds a default `reqwest::Client`.
    ///
    /// Use this when the caller doesn't have a shared client to inject (e.g.
    /// in `main.rs`, which doesn't take `reqwest` as a direct dependency).
    /// Prefer [`WebhookSender::new`] when a shared client pool already exists.
    pub fn with_default_client(cfg: WebhookConfig) -> Result<Self, reqwest::Error> {
        let client = Arc::new(
            Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
        );
        Ok(Self::new(cfg, client))
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

        let cc: Vec<_> = msg
            .cc
            .iter()
            .map(|r| json!({ "email": r.email, "name": r.name }))
            .collect();

        let bcc: Vec<_> = msg
            .bcc
            .iter()
            .map(|r| json!({ "email": r.email, "name": r.name }))
            .collect();

        let to_extra: Vec<_> = msg
            .to_extra
            .iter()
            .map(|r| json!({ "email": r.email, "name": r.name }))
            .collect();

        let body = json!({
            "event_id":            msg.event_id,
            "to_email":            msg.to_email,
            "to_name":             msg.to_name,
            "to_extra":            to_extra,
            "subject":             msg.subject,
            "body_html":           msg.body_html,
            "body_text":           msg.body_text,
            "from_email_override": msg.from_email_override,
            "from_name_override":  msg.from_name_override,
            "attachments":         attachments,
            "cc":                  cc,
            "bcc":                 bcc,
        });

        let mut req = self.client.post(&self.url).json(&body);
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AppError::transient_mailer(e.to_string()))?;

        let status = resp.status();
        if status.is_success() {
            info!(event_id = %msg.event_id, attachments = msg.attachments.len(), cc = msg.cc.len(), bcc = msg.bcc.len(), "Email dispatched via webhook");
            return Ok(());
        }

        let text = resp.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS {
            warn!(http_status = 429, body = %text, "Webhook rate-limited");
            return Err(AppError::RateLimited(format!("webhook HTTP 429: {text}")));
        }
        if status.is_client_error() {
            return Err(AppError::permanent_mailer(format!(
                "webhook HTTP {status}: {text}"
            )));
        }
        Err(AppError::transient_mailer(format!(
            "webhook HTTP {status}: {text}"
        )))
    }
}
