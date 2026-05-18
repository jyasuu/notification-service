pub mod email_log;
pub mod template_store;

pub use email_log::{EmailLogStore, InsertResult};
pub use template_store::{EmailTemplate, TemplateStore};
