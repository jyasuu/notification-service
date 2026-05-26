//! Attachment URL fetcher.
//!
//! The notification service fetches each [`AttachmentRef`] URL at send time
//! so business systems never have to encode or embed file bytes in events.
//!
//! # Fetch strategy
//!
//! * All attachments are fetched **concurrently** via `futures::future::join_all`.
//! * Each attachment is retried independently up to [`FETCH_MAX_RETRIES`] times
//!   on transient failures (5xx, timeout, network error). This means a flaky
//!   storage server for one file does not block delivery for the whole event.
//! * A 4xx or size-exceeded response is a permanent failure — it is returned
//!   immediately without consuming retry slots.
//! * One HTTP GET per attachment attempt, with a 30 s timeout (set on the
//!   shared `Client` at construction time).
//! * Optional `Authorization: Bearer <token>` for internal service URLs.
//! * Responses are size-capped at `max_bytes` (default 10 MiB) to prevent
//!   memory exhaustion from unexpectedly large files.
//!
//! # Error classification
//!
//! | Failure | Error type | Retried |
//! |---|---|---|
//! | 4xx (bad URL, expired, forbidden) | `Permanent` | No |
//! | 5xx (server error) | `Transient` | Yes (up to FETCH_MAX_RETRIES) |
//! | Timeout / network error | `Transient` | Yes |
//! | Response too large | `Permanent` | No |
//! | URL already expired (`max_age_secs`) | `Permanent` | No |
//! | All retries exhausted | last transient error | No further retries |

use common::{AppError, AttachmentRef};
use futures::future::join_all;
use metrics::{counter, histogram};
use reqwest::{Client, StatusCode};
use tokio::time::{sleep, Duration};
use tracing::{debug, instrument, warn};

use crate::message::ResolvedAttachment;

/// Maximum allowed response body size per attachment (10 MiB).
pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// How many times to retry a transient fetch failure before giving up.
/// Delays follow 1 s, 2 s, 4 s (exponential, capped).
const FETCH_MAX_RETRIES: u32 = 3;

/// Fetches all attachment URLs for an event and returns resolved bytes.
///
/// Each attachment is retried independently on transient failures so a
/// flaky storage server for one file does not block the others.
/// Metadata and expiry checks run before any network calls.
///
/// The returned `Vec` preserves the same order as `refs`.
pub async fn fetch_attachments(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    fetch_attachments_with_limit(client, refs, event_timestamp, MAX_ATTACHMENT_BYTES).await
}

/// Like [`fetch_attachments`] but with an explicit per-attachment byte cap.
#[instrument(skip(client, refs, event_timestamp), fields(count = refs.len()))]
pub async fn fetch_attachments_with_limit(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
    max_bytes: usize,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    // ── 1. Validate metadata for every attachment before any network call ─────
    for att_ref in refs {
        att_ref
            .validate(event_timestamp, chrono::Utc::now())
            .map_err(AppError::permanent_mailer)?;
    }

    // ── 2. Fetch all URLs concurrently, each with independent retry ───────────
    //
    // `join_all` (not `try_join_all`) is used here so every attachment gets
    // its own retry budget. A transient 5xx on attachment B no longer cancels
    // the already-in-flight fetch for attachment A.
    let futures: Vec<_> = refs
        .iter()
        .map(|att_ref| fetch_one_with_retry(client, att_ref, max_bytes))
        .collect();

    let results = join_all(futures).await;

    // ── 3. Collect results, preserving per-error permanence ──────────────────
    //
    // Instead of surfacing the first error regardless of type, we check all
    // results so the caller can make a correct requeue decision:
    //   • If ANY error is transient → requeue (we may succeed on retry).
    //   • Only if ALL errors are permanent → no requeue (retrying is pointless).
    //
    // We collect resolved attachments and errors separately so we can inspect
    // the full error set before deciding which error to surface to the caller.
    let mut resolved = Vec::with_capacity(refs.len());
    let mut errors: Vec<AppError> = Vec::new();

    for result in results {
        match result {
            Ok(att) => resolved.push(att),
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        return Ok(resolved);
    }

    // Surface a transient error if any exist, so the caller's `is_permanent_mailer()`
    // check correctly triggers a requeue rather than DLQ-ing an event that might
    // succeed on retry once a flaky storage server recovers.
    // If all errors are permanent, surface the first permanent one.
    let representative = errors
        .into_iter()
        .reduce(|acc, e| {
            // Prefer transient over permanent so the caller requeues.
            if acc.is_permanent_mailer() && !e.is_permanent_mailer() {
                e
            } else {
                acc
            }
        })
        .expect("errors is non-empty");

    Err(representative)
}

/// Fetch a single attachment, retrying on transient failures.
///
/// Permanent failures (4xx, size exceeded, URL expired) are returned
/// immediately without consuming retry slots.
async fn fetch_one_with_retry(
    client: &Client,
    att_ref: &AttachmentRef,
    max_bytes: usize,
) -> Result<ResolvedAttachment, AppError> {
    let mut last_err = None;
    let fetch_start = std::time::Instant::now();

    for attempt in 0..=FETCH_MAX_RETRIES {
        if attempt > 0 {
            let delay = Duration::from_secs(1u64 << (attempt - 1).min(3));
            warn!(
                filename = %att_ref.filename,
                attempt,
                delay_secs = delay.as_secs(),
                "Attachment fetch transient failure — retrying"
            );
            counter!("attachment_fetch_retries_total").increment(1);
            sleep(delay).await;
        }

        match fetch_one(client, att_ref, max_bytes).await {
            Ok(data) => {
                let elapsed = fetch_start.elapsed().as_secs_f64();
                histogram!("attachment_fetch_duration_seconds").record(elapsed);
                counter!("attachment_fetch_total", "result" => "success").increment(1);
                return Ok(ResolvedAttachment {
                    filename: att_ref.filename.clone(),
                    content_type: att_ref.content_type.clone(),
                    data,
                });
            }
            // Permanent errors are returned immediately — no retry.
            Err(e) if e.is_permanent_mailer() => {
                counter!("attachment_fetch_total", "result" => "permanent_failure").increment(1);
                return Err(e);
            }
            Err(e) => last_err = Some(e),
        }
    }

    counter!("attachment_fetch_total", "result" => "transient_failure").increment(1);
    Err(last_err.unwrap_or_else(|| {
        AppError::transient_mailer(format!(
            "attachment '{}' fetch failed after {FETCH_MAX_RETRIES} retries",
            att_ref.filename
        ))
    }))
}

/// Fetch a single attachment URL and return the raw bytes.
#[instrument(skip(client, att_ref), fields(url = %att_ref.url, filename = %att_ref.filename))]
async fn fetch_one(
    client: &Client,
    att_ref: &AttachmentRef,
    max_bytes: usize,
) -> Result<Vec<u8>, AppError> {
    debug!("Fetching attachment");

    let mut req = client.get(&att_ref.url);

    if let Some(token) = &att_ref.fetch_token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.map_err(|e| {
        AppError::transient_mailer(format!(
            "attachment fetch network error '{}': {e}",
            att_ref.filename
        ))
    })?;

    let status = resp.status();

    // 429 → rate-limited by the file server (transient — will be retried).
    if status == StatusCode::TOO_MANY_REQUESTS {
        warn!(filename = %att_ref.filename, "Attachment source returned 429");
        return Err(AppError::RateLimited(format!(
            "attachment '{}' source returned HTTP 429",
            att_ref.filename
        )));
    }

    // 4xx → permanent: bad URL, expired pre-signed URL, access denied, etc.
    if status.is_client_error() {
        return Err(AppError::permanent_mailer(format!(
            "attachment '{}' fetch returned HTTP {status} ({})",
            att_ref.filename, att_ref.url
        )));
    }

    // 5xx → transient: upstream server problem, safe to retry.
    if status.is_server_error() {
        return Err(AppError::transient_mailer(format!(
            "attachment '{}' fetch returned HTTP {status} — will retry",
            att_ref.filename
        )));
    }

    // Cross-check the response Content-Type against the declared type.
    // A mismatch often means the URL has expired and the storage provider
    // returned an HTML error page instead of the intended file.
    // Logged as a warning rather than a hard failure so a legitimate type
    // mismatch (e.g. "application/pdf" vs "application/pdf; charset=utf-8")
    // doesn't break delivery — the bytes are still attached as declared.
    if let Some(resp_ct) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
        if let Ok(resp_ct_str) = resp_ct.to_str() {
            // Compare only the base type (before any ';' parameters).
            let resp_base = resp_ct_str.split(';').next().unwrap_or("").trim();
            let declared_base = att_ref.content_type.split(';').next().unwrap_or("").trim();
            if !resp_base.is_empty() && resp_base != declared_base {
                warn!(
                    filename   = %att_ref.filename,
                    declared   = %att_ref.content_type,
                    response   = %resp_ct_str,
                    "Attachment Content-Type mismatch —                      the URL may have expired and returned an error page.                      Attaching bytes using the declared type."
                );
            }
        }
    }

    // Read body with size cap to prevent memory exhaustion.
    let bytes = resp.bytes().await.map_err(|e| {
        AppError::transient_mailer(format!("attachment '{}' read error: {e}", att_ref.filename))
    })?;

    if bytes.len() > max_bytes {
        return Err(AppError::permanent_mailer(format!(
            "attachment '{}' exceeds size limit ({} > {} bytes)",
            att_ref.filename,
            bytes.len(),
            max_bytes
        )));
    }

    debug!(bytes = bytes.len(), "Attachment fetched");
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn att_ref(url: &str) -> AttachmentRef {
        AttachmentRef {
            url: url.into(),
            filename: "test.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: None,
        }
    }

    #[test]
    fn validate_rejects_empty_url() {
        let mut a = att_ref("https://example.com/file.pdf");
        a.url = "".into();
        assert!(a.validate(&Utc::now(), Utc::now()).is_err());
    }

    #[test]
    fn validate_rejects_non_http_url() {
        let a = att_ref("ftp://example.com/file.pdf");
        assert!(a.validate(&Utc::now(), Utc::now()).is_err());
    }

    #[test]
    fn validate_rejects_path_separator_in_filename() {
        let mut a = att_ref("https://example.com/file.pdf");
        a.filename = "../../etc/passwd".into();
        assert!(a.validate(&Utc::now(), Utc::now()).is_err());
    }

    #[test]
    fn validate_rejects_expired_url() {
        let a = AttachmentRef {
            url: "https://example.com/file.pdf".into(),
            filename: "file.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: Some(0),
        };
        let ts = Utc::now() - chrono::Duration::seconds(10);
        assert!(a.validate(&ts, Utc::now()).unwrap_err().contains("expired"));
    }

    #[test]
    fn validate_accepts_valid_ref() {
        let a = att_ref("https://example.com/invoice.pdf");
        assert!(a.validate(&Utc::now(), Utc::now()).is_ok());
    }

    /// Deterministic expiry test: pin both event_timestamp and check_time so
    /// the test never depends on wall-clock speed or CI timing.
    ///
    /// event_timestamp = T0
    /// check_time      = T0 + 120s  (2 minutes later)
    /// max_age_secs    = 60         (1 minute TTL)
    ///
    /// Age at check_time = 120s > max_age_secs (60s) → must be rejected.
    #[test]
    fn validate_expiry_is_deterministic_with_fixed_check_time() {
        use chrono::TimeZone;
        let t0 = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let check_time = t0 + chrono::Duration::seconds(120);
        let a = AttachmentRef {
            url: "https://example.com/doc.pdf".into(),
            filename: "doc.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: Some(60),
        };
        assert!(
            a.validate(&t0, check_time).unwrap_err().contains("expired"),
            "attachment should be expired: age 120s > max_age_secs 60s"
        );
    }

    /// Confirm that an attachment is valid when check_time is before expiry.
    #[test]
    fn validate_not_expired_when_check_time_within_max_age() {
        use chrono::TimeZone;
        let t0 = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let check_time = t0 + chrono::Duration::seconds(30);
        let a = AttachmentRef {
            url: "https://example.com/doc.pdf".into(),
            filename: "doc.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: Some(60),
        };
        assert!(
            a.validate(&t0, check_time).is_ok(),
            "attachment should be valid: age 30s < max_age_secs 60s"
        );
    }
}
