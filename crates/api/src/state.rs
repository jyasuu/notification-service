use store::EmailLogStore;

use crate::publisher::Publisher;

#[derive(Clone)]
pub struct ApiState {
    pub store: EmailLogStore,
    /// Used by retry endpoints to re-enqueue events after resetting DB rows.
    pub publisher: Publisher,
}
