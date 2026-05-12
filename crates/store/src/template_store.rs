use std::collections::HashMap;
use std::sync::Arc;

use common::AppError;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

/// One template row from the `email_template` table.
#[derive(Debug, Clone)]
pub struct EmailTemplate {
    pub subject:   String,
    pub body_html: String,
    pub body_text: String,
}

/// DB-backed template store with an in-memory read-through cache.
///
/// # Lookup order
///
/// 1. In-memory cache (no DB round-trip on the hot path).
/// 2. Database — on cache miss, the row is fetched, cached, and returned.
/// 3. Compile-time fallback — if the DB has no row for the event type,
///    the service falls back to the static templates in
///    `mailer::template::templates_for()`.  This keeps the built-in
///    ORDER_CONFIRMATION / PASSWORD_RESET / WELCOME templates working even
///    on a fresh database before migration 0010 has been applied.
///
/// # Cache invalidation
///
/// The cache is populated lazily and lives for the lifetime of the process.
/// To pick up template edits: either restart the service, or call
/// [`TemplateStore::invalidate`] / [`TemplateStore::reload_all`] from an
/// admin endpoint (not yet implemented — logged as a future extension).
///
/// For most deployments a restart on template change is acceptable; the
/// service is stateless and restarts in under a second.
#[derive(Clone)]
pub struct TemplateStore {
    pool:  PgPool,
    cache: Arc<RwLock<HashMap<String, EmailTemplate>>>,
}

impl TemplateStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolve the template for `event_type`.
    ///
    /// Returns `AppError::Template` for unknown event types so the message is
    /// immediately routed to DLQ without wasting retry slots (same semantics
    /// as the old `templates_for()` function).
    #[instrument(skip(self), fields(event_type))]
    pub async fn resolve(&self, event_type: &str) -> Result<EmailTemplate, AppError> {
        // ── 1. Cache hit ──────────────────────────────────────────────────────
        {
            let cache = self.cache.read().await;
            if let Some(t) = cache.get(event_type) {
                debug!("Template cache hit");
                return Ok(t.clone());
            }
        }

        // ── 2. DB lookup ──────────────────────────────────────────────────────
        let row = sqlx::query!(
            r#"
            SELECT subject, body_html, body_text
            FROM   email_template
            WHERE  type = $1 AND active = TRUE
            "#,
            event_type,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        if let Some(r) = row {
            let tpl = EmailTemplate {
                subject:   r.subject,
                body_html: r.body_html,
                body_text: r.body_text,
            };
            // Populate cache under write lock.
            self.cache
                .write()
                .await
                .insert(event_type.to_owned(), tpl.clone());
            info!("Template loaded from DB and cached");
            return Ok(tpl);
        }

        // ── 3. Compile-time fallback ──────────────────────────────────────────
        // Keeps built-in templates working without a DB row.
        if let Ok((subject, body_html, body_text)) = mailer::templates_for(event_type) {
            warn!(
                event_type,
                "Template not found in DB — using compile-time fallback"
            );
            let tpl = EmailTemplate {
                subject:   subject.to_owned(),
                body_html: body_html.to_owned(),
                body_text: body_text.to_owned(),
            };
            // Cache the fallback too so the warning only fires once per type per process lifetime.
            self.cache
                .write()
                .await
                .insert(event_type.to_owned(), tpl.clone());
            return Ok(tpl);
        }

        Err(AppError::Template(format!(
            "Unknown event type '{event_type}' — no template in DB or compile-time fallback"
        )))
    }

    /// Remove a single entry from the cache, forcing the next lookup to
    /// re-fetch from the database. Useful after an operator edits a template row.
    pub async fn invalidate(&self, event_type: &str) {
        self.cache.write().await.remove(event_type);
        info!(event_type, "Template cache entry invalidated");
    }

    /// Clear the entire cache, forcing all subsequent lookups to hit the DB.
    pub async fn reload_all(&self) {
        self.cache.write().await.clear();
        info!("Template cache cleared");
    }
}
