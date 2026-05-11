use axum::{
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;

use crate::{
    handlers::{
        get_email_status, get_recipient_status, health, ready, retry_event, retry_recipient,
    },
    state::ApiState,
};

pub fn build_router(state: ApiState) -> Router {
    Router::new()
        // Probes
        .route("/health", get(health))
        .route("/ready", get(ready))
        // Event-level: all recipients
        .route("/emails/:event_id", get(get_email_status))
        .route("/emails/:event_id/retry", post(retry_event))
        // Recipient-level: one recipient within an event
        .route(
            "/emails/:event_id/recipients/:email",
            get(get_recipient_status),
        )
        .route(
            "/emails/:event_id/recipients/:email/retry",
            post(retry_recipient),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
