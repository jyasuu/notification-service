pub mod email_validation;
pub mod error;
pub mod event;
pub mod log;

pub use email_validation::is_valid_email;
pub use error::AppError;
pub use event::{AttachmentRef, EmailEvent, FromOverride, Recipient};
pub use log::{EmailLog, EmailStatus};
