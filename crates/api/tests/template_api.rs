//! Integration tests for the template HTTP endpoints.
//!
//! Gated behind `--features integration` — requires a live PostgreSQL instance:
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/anvil_notify \
//!   cargo test -p api --features integration -- --test-threads=1
//! ```
//!
//! Each test uses a UUID-suffixed event type and cleans up on completion.

#![cfg(feature = "integration")]

use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tower::ServiceExt;
use uuid::Uuid;

use api::{build_router, ApiState, Publisher};
use recipient_filter::{FilterConfig, RecipientFilter};
use store::{BlockListStore, EmailNotificationStore, TemplateStore};

// ── helpers ───────────────────────────────────────────────────────────────────

async fn test_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/anvil_notify".into());
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("test DB connection failed")
}

fn test_state(pool: PgPool) -> ApiState {
    ApiState {
        store: Arc::new(EmailNotificationStore::new(pool.clone())),
        template_store: TemplateStore::new(pool.clone()),
        block_list_store: BlockListStore::new(pool.clone(), Duration::ZERO),
        publisher: Publisher::disconnected(),
        api_key: None,
        filter: RecipientFilter::new(FilterConfig::default()),
        max_recipients_per_event: 50,
    }
}

async fn send_request(state: ApiState, req: Request<Body>) -> (StatusCode, Value) {
    let app = build_router(state);
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn post_json(state: ApiState, path: &str, body: Value) -> (StatusCode, Value) {
    send_request(
        state,
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
}

async fn get_json(state: ApiState, path: &str) -> (StatusCode, Value) {
    send_request(
        state,
        Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn patch_json(state: ApiState, path: &str, body: Value) -> (StatusCode, Value) {
    send_request(
        state,
        Request::builder()
            .method("PATCH")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
}

fn unique_event_type() -> String {
    format!("TEST_{}", Uuid::new_v4().simple())
}

async fn cleanup(pool: &PgPool, event_type: &str) {
    sqlx::query!(
        "DELETE FROM notification_template WHERE type = $1",
        event_type
    )
    .execute(pool)
    .await
    .unwrap();
}

// ── POST /templates ───────────────────────────────────────────────────────────

#[tokio::test]
async fn upsert_template_creates_new_row() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    let (status, body) = post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "channel": "email",
            "subject": "Hello {{ name }}", "body_html": "<p>Hi</p>", "body_text": "Hi",
            "active": true,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["version"], 1);
    assert_eq!(body["inserted"], true);
    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn upsert_template_bumps_version_on_content_change() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    let (s1, _) = post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "v1", "body_html": "<p>v1</p>", "body_text": "v1",
        }),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);

    let (s2, body) = post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "v2", "body_html": "<p>v2</p>", "body_text": "v2",
        }),
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "body: {body}");
    assert_eq!(body["version"], 2);
    assert_eq!(body["inserted"], false);
    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn upsert_template_noop_when_unchanged() {
    let pool = test_pool().await;
    let event_type = unique_event_type();
    let payload = json!({
        "event_type": event_type, "subject": "stable",
        "body_html": "<p>stable</p>", "body_text": "stable",
    });

    post_json(test_state(pool.clone()), "/templates", payload.clone()).await;
    let (_, body) = post_json(test_state(pool.clone()), "/templates", payload).await;
    assert_eq!(
        body["version"], 1,
        "version must not bump on identical re-upload"
    );
    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn upsert_template_400_on_missing_event_type() {
    let pool = test_pool().await;
    let (status, _) = post_json(
        test_state(pool),
        "/templates",
        json!({ "subject": "x", "body_html": "x", "body_text": "x" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn upsert_template_inactive_flag() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    let (status, body) = post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "staged",
            "body_html": "<p>staged</p>", "body_text": "staged", "active": false,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["active"], false);
    cleanup(&pool, &event_type).await;
}

// ── GET /templates ────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_templates_contains_seeded_row_without_body_fields() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "list test",
            "body_html": "<p>list</p>", "body_text": "list",
        }),
    )
    .await;

    let (status, body) = get_json(test_state(pool.clone()), "/templates").await;
    assert_eq!(status, StatusCode::OK);

    let templates = body["templates"].as_array().expect("templates array");
    let our = templates
        .iter()
        .find(|t| t["event_type"] == event_type)
        .expect("seeded template not found in list");

    // Body fields must NOT be present in the list response
    assert!(
        our.get("body_html").is_none_or(|v| v.is_null()),
        "body_html should be absent from list response"
    );

    cleanup(&pool, &event_type).await;
}

// ── GET /templates/:event_type ────────────────────────────────────────────────

#[tokio::test]
async fn get_template_returns_full_content() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "Full subject",
            "body_html": "<p>html</p>", "body_text": "text",
        }),
    )
    .await;

    let (status, body) = get_json(
        test_state(pool.clone()),
        &format!("/templates/{event_type}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let t = &body["templates"][0];
    assert_eq!(t["subject"], "Full subject");
    assert_eq!(t["body_html"], "<p>html</p>");
    assert_eq!(t["body_text"], "text");

    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn get_template_404_for_unknown() {
    let pool = test_pool().await;
    let (status, _) = get_json(test_state(pool), "/templates/DOES_NOT_EXIST_XYZ").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── PATCH /templates/:event_type ─────────────────────────────────────────────

#[tokio::test]
async fn patch_template_toggles_active_atomically() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "patch test",
            "body_html": "<p>patch</p>", "body_text": "patch", "active": true,
        }),
    )
    .await;

    let (status, body) = patch_json(
        test_state(pool.clone()),
        &format!("/templates/{event_type}"),
        json!({ "active": false }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "patch failed: {body}");
    assert_eq!(body["active"], false);

    let row = sqlx::query!(
        "SELECT active FROM notification_template WHERE type = $1 AND channel = 'email'",
        event_type
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!row.active, "DB row should be inactive after PATCH");

    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn patch_template_updates_subject_leaves_bodies_unchanged() {
    let pool = test_pool().await;
    let event_type = unique_event_type();

    post_json(
        test_state(pool.clone()),
        "/templates",
        json!({
            "event_type": event_type, "subject": "original",
            "body_html": "<p>original</p>", "body_text": "original",
        }),
    )
    .await;

    patch_json(
        test_state(pool.clone()),
        &format!("/templates/{event_type}"),
        json!({ "subject": "updated" }),
    )
    .await;

    let (_, body) = get_json(
        test_state(pool.clone()),
        &format!("/templates/{event_type}"),
    )
    .await;
    let t = &body["templates"][0];
    assert_eq!(t["subject"], "updated");
    assert_eq!(
        t["body_html"], "<p>original</p>",
        "body_html must be unchanged"
    );

    cleanup(&pool, &event_type).await;
}

#[tokio::test]
async fn patch_template_404_for_unknown() {
    let pool = test_pool().await;
    let (status, _) = patch_json(
        test_state(pool),
        "/templates/DOES_NOT_EXIST_XYZ",
        json!({ "active": true }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
