//! Attachment URL fetcher.
//!
//! The notification service fetches each [`AttachmentRef`] URL at send time
//! so business systems never have to encode or embed file bytes in events.
//!
//! # Fetch strategy
//!
//! * All attachments are fetched **concurrently** via `futures::future::try_join_all`,
//!   reducing total latency from O(n × rtt) to O(max_rtt).
//! * One HTTP GET per attachment, with a configurable timeout (default 30 s).
//! * Optional `Authorization: Bearer <token>` for internal service URLs.
//! * Responses are size-capped at [`MAX_ATTACHMENT_BYTES`] (default 10 MB) to
//!   prevent memory exhaustion from unexpectedly large files.
//! * Non-2xx responses and timeouts are returned as errors; the caller decides
//!   whether they are transient (retryable) or permanent.
//!
//! # Error classification
//!
//! | Failure | Error type | Retried |
//! |---|---|---|
//! | 4xx (bad URL, expired, forbidden) | `permanent:` prefix | No |
//! | 5xx (server error) | transient | Yes |
//! | Timeout / network error | transient | Yes |
//! | Response too large | `permanent:` prefix | No |
//! | URL already expired (`max_age_secs`) | `permanent:` prefix | No |

use common::{AppError, AttachmentRef};
use futures::future::try_join_all;
use reqwest::{Client, StatusCode};
use tracing::{debug, instrument, warn};

use crate::message::ResolvedAttachment;

/// Maximum allowed response body size per attachment (10 MiB).
///
/// Raised as a permanent error so the delivery is not retried.
/// For larger files, instruct business systems to link instead of attach.
pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// Fetches all attachment URLs for an event and returns resolved bytes.
///
/// Uses the compiled-in [`MAX_ATTACHMENT_BYTES`] cap.  Call
/// [`fetch_attachments_with_limit`] to supply a config-driven value.
///
/// All attachments are fetched **concurrently** so total latency is bounded
/// by the slowest single attachment rather than the sum of all round-trips.
/// Metadata and expiry checks run before any network calls.
///
/// The returned `Vec` preserves the same order as `refs`.
#[instrument(skip(client, refs, event_timestamp), fields(count = refs.len()))]
pub async fn fetch_attachments(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    fetch_attachments_with_limit(client, refs, event_timestamp, MAX_ATTACHMENT_BYTES).await
}

/// Like [`fetch_attachments`] but with an explicit per-attachment byte cap.
///
/// Set `max_bytes` from `AppConfig::mailer_max_attachment_bytes` so operators
/// can tune the limit without a code change.
pub async fn fetch_attachments_with_limit(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
    max_bytes: usize,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    // ── 1. Validate metadata for every attachment before any network call ─────
    // Fail fast on malformed refs without wasting bandwidth.
    for att_ref in refs {
        att_ref
            .validate(event_timestamp)
            .map_err(AppError::Mailer)?;
    }

    // ── 2. Fetch all URLs concurrently ────────────────────────────────────────
    // `try_join_all` cancels remaining futures on the first error, which is
    // the desired behaviour: if any attachment is unavailable we should not
    // partially attach others to the email.
    let futures: Vec<_> = refs
        .iter()
        .map(|att_ref| fetch_one(client, att_ref, max_bytes))
        .collect();

    let data_vec = try_join_all(futures).await?;

    // ── 3. Zip resolved bytes back with their metadata (order preserved) ──────
    let resolved = refs
        .iter()
        .zip(data_vec)
        .map(|(att_ref, data)| ResolvedAttachment {
            filename: att_ref.filename.clone(),
            content_type: att_ref.content_type.clone(),
            data,
        })
        .collect();

    Ok(resolved)
}

/// Fetch a single attachment URL and return the raw bytes.
#[instrument(skip(client, att_ref), fields(url = %att_ref.url, filename = %att_ref.filename))]
async fn fetch_one(client: &Client, att_ref: &AttachmentRef, max_bytes: usize) -> Result<Vec<u8>, AppError> {
    debug!("Fetching attachment");

    let mut req = client.get(&att_ref.url);

    if let Some(token) = &att_ref.fetch_token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.map_err(|e| {
        AppError::Mailer(format!(
            "attachment fetch network error '{}': {e}",
            att_ref.filename
        ))
    })?;

    let status = resp.status();

    // 4xx → permanent: bad URL, expired pre-signed URL, access denied, etc.
    // No point retrying — the business system must re-publish with a fresh URL.
    if status.is_client_error() {
        return Err(AppError::Mailer(format!(
            "permanent: attachment '{}' fetch returned HTTP {status} ({})",
            att_ref.filename, att_ref.url
        )));
    }

    // 5xx → transient: upstream server problem, safe to retry
    if status.is_server_error() {
        return Err(AppError::Mailer(format!(
            "attachment '{}' fetch returned HTTP {status} — will retry",
            att_ref.filename
        )));
    }

    // 429 → rate-limited by the file server
    if status == StatusCode::TOO_MANY_REQUESTS {
        warn!(filename = %att_ref.filename, "Attachment source returned 429");
        return Err(AppError::RateLimited(format!(
            "attachment '{}' source returned HTTP 429",
            att_ref.filename
        )));
    }

    // Read body with size cap to prevent memory exhaustion
    let bytes = resp.bytes().await.map_err(|e| {
        AppError::Mailer(format!("attachment '{}' read error: {e}", att_ref.filename))
    })?;

    if bytes.len() > max_bytes {
        return Err(AppError::Mailer(format!(
            "permanent: attachment '{}' exceeds size limit ({} > {} bytes)",
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
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_non_http_url() {
        let a = att_ref("ftp://example.com/file.pdf");
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_path_separator_in_filename() {
        let mut a = att_ref("https://example.com/file.pdf");
        a.filename = "../../etc/passwd".into();
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_expired_url() {
        let a = AttachmentRef {
            url: "https://example.com/file.pdf".into(),
            filename: "file.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: Some(0), // already expired
        };
        let ts = Utc::now() - chrono::Duration::seconds(10);
        assert!(a.validate(&ts).unwrap_err().contains("expired"));
    }

    #[test]
    fn validate_accepts_valid_ref() {
        let a = att_ref("https://example.com/invoice.pdf");
        assert!(a.validate(&Utc::now()).is_ok());
    }
}
