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
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        };

        let body = Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
