use store::{EmailLogStore, TemplateStore};

use crate::publisher::Publisher;

#[derive(Clone)]
pub struct ApiState {
    pub store: EmailLogStore,
    pub template_store: TemplateStore,
    /// Used by retry endpoints to re-enqueue events after resetting DB rows.
    pub publisher: Publisher,
    /// When `Some`, every request must supply `Authorization: Bearer <token>`.
    /// Set via `AN__HTTP__API_KEY` env var or `http.api_key` in config.
    /// Leave `None` (default) only when the API is network-isolated.
    pub api_key: Option<String>,
}
