pub mod email_validation;
pub mod error;
pub mod event;
pub mod log;

pub use email_validation::is_valid_email;
pub use error::{AppError, MailerKind};
#[allow(deprecated)]
pub use event::{
    AttachmentRef, ChannelOverrides, EmailEvent, EmailOptions, FromOverride, GroupRetryMode,
    Metadata, NotificationEvent, Recipient, RetryPolicy, SendMode,
};
pub use log::{EmailLog, EmailStatus, NotificationLog, NotificationStatus};
