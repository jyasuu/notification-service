pub mod error;
pub mod event;
pub mod log;

pub use error::AppError;
pub use event::{AttachmentRef, EmailEvent, FromOverride, Recipient};
pub use log::{EmailLog, EmailStatus};
