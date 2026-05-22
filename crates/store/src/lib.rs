pub mod notification_log;
pub mod template_store;

// Core multi-channel store — primary public API.
pub use notification_log::{
    EmailInsertPendingArgs,
    EmailNotificationStore,
    InsertPendingArgs, // back-compat alias for EmailInsertPendingArgs
    InsertResult,
    NotificationStore,
    CHANNEL_EMAIL,
};
pub use template_store::{EmailTemplate, NotificationTemplate, TemplateStore};
