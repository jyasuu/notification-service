use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use common::AppError;
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
            // Permanent mailer errors (prefixed "permanent:") represent a bad
            // request that will never succeed — return 422 so callers know not
            // to retry without fixing the underlying data.
            AppError::Mailer(msg) if msg.starts_with("permanent:") => {
                (StatusCode::UNPROCESSABLE_ENTITY, msg.clone())
            }
            AppError::Blocked(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        };

        let body = Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
