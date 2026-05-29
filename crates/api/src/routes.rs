use std::num::NonZeroU32;
use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;

use crate::{
    handlers::{
        add_blocklist_entry, get_email_status, get_recipient_status, health,
        invalidate_all_template_cache, invalidate_blocklist_cache, invalidate_template_cache,
        list_blocklist, ready, reload_blocklist_cache, remove_blocklist_entry, retry_event,
        retry_recipient,
    },
    state::ApiState,
};

/// Hard cap on incoming request bodies (64 KiB).
///
/// All current API endpoints either have no body (GET / DELETE) or accept a
/// body-less POST (retry endpoints reconstruct the event from DB, so no body
/// is sent at all). 64 KiB is generous enough to tolerate any future body
/// that might be added while preventing memory exhaustion from oversized uploads.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// Maximum retry-endpoint calls per minute.
///
/// Each retry re-enqueues an event to RabbitMQ and resets DB rows.  A burst
/// of retries from a script or dashboard can flood the AMQP queue and
/// overload the consumer.  60 retries/min is generous for manual operator
/// use while still preventing accidental storms.
const RETRY_RATE_PER_MIN: u32 = 60;

type RetryLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

pub fn build_router(state: ApiState) -> Router {
    // Probe routes are always open — no auth needed for health checks.
    let probes = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready));

    // Retry endpoints get an additional token-bucket rate limiter so that a
    // script or runaway dashboard cannot flood the AMQP queue.  Read endpoints
    // (status, blocklist list) are not rate-limited — they are idempotent and
    // cheap.
    let retry_limiter: Arc<RetryLimiter> = Arc::new(RateLimiter::direct(Quota::per_minute(
        NonZeroU32::new(RETRY_RATE_PER_MIN).expect("RETRY_RATE_PER_MIN > 0"),
    )));

    let retry_routes = Router::new()
        .route("/emails/{event_id}/retry", post(retry_event))
        .route(
            "/emails/{event_id}/recipients/{email}/retry",
            post(retry_recipient),
        )
        .layer(middleware::from_fn_with_state(
            retry_limiter,
            retry_rate_limit,
        ));

    // All email status and retry routes require a bearer token when
    // `api_key` is configured. When `api_key` is `None` (network-isolated
    // deployments), the middleware passes every request through.
    let protected = Router::new()
        .route("/emails/{event_id}", get(get_email_status))
        .route(
            "/emails/{event_id}/recipients/{email}",
            get(get_recipient_status),
        )
        // Template cache invalidation — useful after editing notification_template rows
        // without restarting the service.
        .route("/templates/cache", delete(invalidate_all_template_cache))
        .route(
            "/templates/{event_type}/cache",
            delete(invalidate_template_cache),
        )
        // ── DB-backed blocklist admin ─────────────────────────────────────
        .route("/admin/blocklist", axum::routing::get(list_blocklist))
        .route("/admin/blocklist", post(add_blocklist_entry))
        .route("/admin/blocklist/cache", delete(invalidate_blocklist_cache))
        .route("/admin/blocklist/cache", post(reload_blocklist_cache))
        .route("/admin/blocklist/{id}", delete(remove_blocklist_entry))
        .merge(retry_routes)
        .layer(middleware::from_fn_with_state(state.clone(), bearer_auth));

    Router::new()
        .merge(probes)
        .merge(protected)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Axum middleware that enforces a token-bucket rate limit on retry endpoints.
///
/// Returns 429 Too Many Requests when the bucket is empty, with a
/// `Retry-After: 60` header so clients know when to try again.
async fn retry_rate_limit(
    State(limiter): State<Arc<RetryLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    if limiter.check().is_err() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", "60")],
            Json(json!({
                "error": "retry rate limit exceeded — maximum 60 retries per minute"
            })),
        )
            .into_response();
    }
    next.run(request).await
}

/// Axum middleware that enforces `Authorization: Bearer <token>` on every
/// request when `ApiState::api_key` is `Some`.
///
/// Returns 401 when the header is missing and 403 when the token is wrong,
/// so callers can distinguish "you forgot auth" from "your token is invalid".
async fn bearer_auth(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let Some(expected) = &state.api_key else {
        // Auth disabled — pass through.
        return next.run(request).await;
    };

    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match token {
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authorization header missing or malformed" })),
        )
            .into_response(),
        // Constant-time comparison using HMAC digests of both tokens so that
        // the compared byte slices are always equal length (32 bytes), preventing
        // a timing side-channel that would otherwise allow an attacker to
        // determine the correct token length.
        //
        // Both values are HMAC-SHA256'd under the same key (the expected token
        // itself) before comparison.  The key does not need to be secret for
        // this purpose — we only need the digests to be fixed-length and for
        // the comparison to be constant-time.  Using the expected token as the
        // key ensures the digest depends on both sides, which prevents
        // pre-computation attacks.
        Some(t) => {
            let mac_of = |input: &str| -> [u8; 32] {
                let mut mac = Hmac::<Sha256>::new_from_slice(expected.as_bytes())
                    .expect("HMAC accepts any key length");
                mac.update(input.as_bytes());
                mac.finalize().into_bytes().into()
            };
            if mac_of(t).ct_eq(&mac_of(expected)).unwrap_u8() == 0 {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "Invalid API key" })),
                )
                    .into_response();
            }
            next.run(request).await
        }
    }
}
