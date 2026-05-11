use async_trait::async_trait;
use common::AppError;

use crate::EmailMessage;

/// Abstraction over email transport backends.
///
/// Both `SmtpSender` and `WebhookSender` implement this trait,
/// so the consumer is backend-agnostic.
#[async_trait]
pub trait EmailSender: Send + Sync {
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError>;
}
