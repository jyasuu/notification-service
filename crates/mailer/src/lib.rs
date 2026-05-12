pub mod fetcher;
pub mod message;
pub mod sender;
pub mod smtp;
pub mod template;
pub mod webhook;

pub use fetcher::fetch_attachments;
pub use message::{EmailMessage, ResolvedAttachment};
pub use sender::EmailSender;
pub use smtp::SmtpSender;
pub use template::{render_html_template, render_template, templates_for, TemplateStore};
pub use webhook::WebhookSender;
