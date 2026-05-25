use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::AppError;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

/// One template row from the `notification_template` table.
#[derive(Debug, Clone)]
pub struct NotificationTemplate {
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
}

// Back-compat alias — callers that still use `EmailTemplate` continue to compile.
pub use NotificationTemplate as EmailTemplate;

/// A cached template entry together with the instant it was populated.
#[derive(Debug, Clone)]
struct CacheEntry {
    template: NotificationTemplate,
    inserted_at: Instant,
}

/// DB-backed template store with an in-memory read-through cache.
///
/// # Lookup order
///
/// 1. In-memory cache — returned immediately when the entry is younger than
///    `cache_ttl`.  A TTL of zero disables the cache entirely (always hits DB).
/// 2. Database — on cache miss or expired entry, the row is fetched, cached,
///    and returned.
/// 3. If no active row exists, `AppError::Template` is returned so the consumer
///    immediately marks the delivery FAILED (no retries, routes to DLQ).
///    Add a row to `notification_template` to register a new event type.
///
/// # Cache invalidation
///
/// Entries expire automatically after `cache_ttl` (default 5 minutes).
/// For immediate invalidation without a restart, call
/// [`TemplateStore::invalidate`] / [`TemplateStore::reload_all`] via the
/// `DELETE /templates/cache` or `DELETE /templates/:event_type/cache` endpoints.
///
/// For most deployments the TTL is short enough that operator edits to the
/// `notification_template` table take effect within minutes automatically.
#[derive(Clone)]
pub struct TemplateStore {
    pool: PgPool,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    cache_ttl: Duration,
}

impl TemplateStore {
    /// Construct with an explicit TTL.  Pass `Duration::ZERO` to disable caching.
    pub fn new_with_ttl(pool: PgPool, cache_ttl: Duration) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
        }
    }

    /// Construct with the default TTL (5 minutes).
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_ttl(pool, Duration::from_secs(300))
    }

    /// Resolve the template for `event_type` and `channel`.
    ///
    /// The cache key is `"{channel}:{event_type}"` so templates for the same
    /// event type on different channels are cached independently.
    ///
    /// Returns `AppError::Template` for unknown (event_type, channel) pairs so
    /// the message is immediately routed to DLQ without wasting retry slots.
    #[instrument(skip(self), fields(event_type, channel))]
    pub async fn resolve(
        &self,
        event_type: &str,
        channel: &str,
    ) -> Result<NotificationTemplate, AppError> {
        let cache_key = format!("{channel}:{event_type}");

        // ── 1. Cache hit (only when TTL is non-zero and entry is fresh) ───────
        // Also record whether a stale entry exists so we can warn below without
        // a second read-lock acquisition — the stale check previously opened a
        // new read lock after the DB miss, which was redundant and could race.
        let stale_entry_exists;
        if !self.cache_ttl.is_zero() {
            let cache = self.cache.read().await;
            match cache.get(&cache_key) {
                Some(entry) if entry.inserted_at.elapsed() < self.cache_ttl => {
                    debug!("Template cache hit");
                    return Ok(entry.template.clone());
                }
                Some(_) => {
                    debug!("Template cache expired");
                    stale_entry_exists = true;
                }
                None => {
                    stale_entry_exists = false;
                }
            }
        } else {
            stale_entry_exists = false;
        }

        // ── 2. DB lookup ──────────────────────────────────────────────────────
        let row = sqlx::query!(
            r#"
            SELECT subject, body_html, body_text
            FROM   notification_template
            WHERE  type = $1 AND channel = $2 AND active = TRUE
            "#,
            event_type,
            channel,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        if let Some(r) = row {
            let tpl = NotificationTemplate {
                subject: r.subject,
                body_html: r.body_html,
                body_text: r.body_text,
            };
            self.cache.write().await.insert(
                cache_key.clone(),
                CacheEntry {
                    template: tpl.clone(),
                    inserted_at: Instant::now(),
                },
            );
            info!("Template loaded from DB and cached");
            return Ok(tpl);
        }

        // No active DB row found — warn if a stale cache entry exists.
        // The stale_entry_exists flag was captured from the initial read lock
        // above, avoiding a second lock acquisition here.
        if stale_entry_exists {
            tracing::warn!(
                event_type,
                channel,
                cache_ttl_secs = self.cache_ttl.as_secs(),
                "Template '{event_type}' (channel '{channel}') not found in DB but a stale \
                 cache entry is still active — expires after TTL or call \
                 DELETE /templates/{event_type}/cache"
            );
        }

        Err(AppError::Template(format!(
            "Unknown event type '{event_type}' for channel '{channel}' \
             — add a row to notification_template"
        )))
    }

    /// Remove all cache entries for `event_type` across every channel,
    /// forcing the next lookup to re-fetch from the database.
    ///
    /// Cache keys are stored as `"{channel}:{event_type}"`, so a plain
    /// `remove(event_type)` would silently miss every entry.  This method
    /// scans all keys and drops those whose suffix matches `:{event_type}`.
    pub async fn invalidate(&self, event_type: &str) {
        let suffix = format!(":{event_type}");
        let mut cache = self.cache.write().await;
        let before = cache.len();
        cache.retain(|k, _| !k.ends_with(&suffix));
        let removed = before - cache.len();
        info!(event_type, removed, "Template cache entries invalidated");
    }

    /// Clear the entire cache, forcing all subsequent lookups to hit the DB.
    pub async fn reload_all(&self) {
        self.cache.write().await.clear();
        info!("Template cache cleared");
    }
}
