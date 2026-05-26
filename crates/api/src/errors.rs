use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use common::{AppError, MailerKind};
use serde_json::json;

pub struct ApiError(pub AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            AppError::Duplicate(msg) => (StatusCode::CONFLICT, msg.clone()),
            AppError::Template(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            // Permanent mailer errors represent a bad request that will never
            // succeed — return 422 so callers know not to retry without fixing
            // the underlying data.
            AppError::Mailer { message: msg, kind: MailerKind::Permanent } => {
                (StatusCode::UNPROCESSABLE_ENTITY, msg.clone())
            }
            AppError::Blocked(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            // Queue errors mean the AMQP broker is unavailable — this is a
            // transient infrastructure problem, not a logic bug.  503 signals
            // to callers (and load balancers) that retrying is appropriate.
            AppError::Queue(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            AppError::UnknownStatus(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("data integrity error: unknown status '{msg}' — check DB schema vs code version"),
            ),
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        };

        let body = Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
