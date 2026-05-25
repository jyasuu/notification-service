//! Recipient block/allow-list filter.
//!
//! Called in `processor.rs` for every address that will appear in a delivery:
//! the primary TO recipient, and every CC and BCC address.
//! Returns `Ok(())` if the address may receive email, or
//! `Err(AppError::Blocked(...))` if it must be dropped.
//!
//! For TO recipients a blocked address is recorded as `BLOCKED` in
//! `notification_log` and the delivery continues for other recipients.
//! For CC/BCC a blocked address is silently excluded — it is logged at WARN
//! level and stripped from the delivery, but does not affect TO recipients
//! or cause the event to fail.  This keeps CC/BCC semantics consistent
//! with TO: a blocked address is dropped, not a reason to abort.
//!
//! Two operating modes (controlled by config):
//!
//! * **Blocklist-only** (default): any address or domain in `blocked_*` is
//!   dropped; everything else passes.
//! * **Allowlist mode** (`allowed_emails` or `allowed_domains` is non-empty):
//!   only addresses that match the allowlist pass; everything else is dropped.
//!   Useful for staging environments to prevent accidental real sends.
//!
//! Both lists are case-insensitive.

use std::collections::HashSet;

use common::AppError;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Configuration loaded from `[filter]` in `config/default.toml` or env vars.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FilterConfig {
    /// Specific email addresses that must never receive mail.
    #[serde(default)]
    pub blocked_emails: Vec<String>,

    /// Domains whose addresses must never receive mail (e.g. `"competitor.com"`).
    #[serde(default)]
    pub blocked_domains: Vec<String>,

    /// If non-empty, **only** these addresses may receive mail (allowlist mode).
    /// Useful for dev/staging environments.
    #[serde(default)]
    pub allowed_emails: Vec<String>,

    /// If non-empty, **only** addresses at these domains may receive mail.
    /// Useful for dev/staging environments (e.g. `["example.com"]`).
    #[serde(default)]
    pub allowed_domains: Vec<String>,
}

/// A compiled, ready-to-query recipient filter.
///
/// All four lists are stored as `HashSet<String>` after construction so that
/// every `check` call is O(1) regardless of list size.  The `Vec`-based config
/// types are converted once in `new`.
#[derive(Debug, Clone)]
pub struct RecipientFilter {
    blocked_emails: HashSet<String>,
    blocked_domains: HashSet<String>,
    allowed_emails: HashSet<String>,
    allowed_domains: HashSet<String>,
    /// True when at least one allowlist entry is configured.
    allowlist_mode: bool,
}

impl RecipientFilter {
    /// Build from config. All strings are normalised to lowercase on construction.
    pub fn new(cfg: FilterConfig) -> Self {
        let norm = |v: Vec<String>| -> HashSet<String> {
            v.into_iter().map(|s| s.to_lowercase()).collect()
        };

        let allowed_emails = norm(cfg.allowed_emails);
        let allowed_domains = norm(cfg.allowed_domains);
        let allowlist_mode = !allowed_emails.is_empty() || !allowed_domains.is_empty();

        Self {
            blocked_emails: norm(cfg.blocked_emails),
            blocked_domains: norm(cfg.blocked_domains),
            allowed_emails,
            allowed_domains,
            allowlist_mode,
        }
    }

    /// Returns `Ok(())` if the recipient may receive email,
    /// or `Err(AppError::Blocked(...))` if it must be dropped.
    pub fn check(&self, email: &str) -> Result<(), AppError> {
        let email_lc = email.to_lowercase();
        let domain = domain_of(&email_lc);

        // ── Blocklist (always applied, even in allowlist mode) ────────────────
        if self.blocked_emails.contains(&email_lc) {
            debug!(email, "Recipient is on the email blocklist");
            return Err(AppError::Blocked(format!(
                "{email} is on the blocked-email list"
            )));
        }
        if let Some(d) = &domain {
            if self.blocked_domains.contains(d) {
                debug!(email, domain = %d, "Recipient domain is on the blocklist");
                return Err(AppError::Blocked(format!(
                    "{email}: domain '{d}' is on the blocked-domain list"
                )));
            }
        }

        // ── Allowlist mode ────────────────────────────────────────────────────
        if self.allowlist_mode {
            let email_allowed = self.allowed_emails.contains(&email_lc);
            let domain_allowed = domain
                .as_ref()
                .map(|d| self.allowed_domains.contains(d))
                .unwrap_or(false);

            if !email_allowed && !domain_allowed {
                debug!(email, "Recipient not on allowlist — dropping");
                return Err(AppError::Blocked(format!(
                    "{email} is not on the allowed-email/domain list (allowlist mode active)"
                )));
            }
        }

        Ok(())
    }

    /// Returns `true` when no filters are configured (passthrough).
    pub fn is_passthrough(&self) -> bool {
        self.blocked_emails.is_empty() && self.blocked_domains.is_empty() && !self.allowlist_mode
    }
}

/// Extract the domain portion of `user@domain.tld` (already lowercased).
fn domain_of(email: &str) -> Option<String> {
    email.rsplit_once('@').map(|(_, d)| d.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(cfg: FilterConfig) -> RecipientFilter {
        RecipientFilter::new(cfg)
    }

    #[test]
    fn passthrough_when_empty() {
        let f = filter(FilterConfig::default());
        assert!(f.is_passthrough());
        assert!(f.check("anyone@example.com").is_ok());
    }

    #[test]
    fn blocks_specific_email() {
        let f = filter(FilterConfig {
            blocked_emails: vec!["bad@example.com".into()],
            ..Default::default()
        });
        assert!(f.check("bad@example.com").is_err());
        assert!(f.check("BAD@EXAMPLE.COM").is_err()); // case-insensitive
        assert!(f.check("good@example.com").is_ok());
    }

    #[test]
    fn blocks_entire_domain() {
        let f = filter(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });
        assert!(f.check("a@blocked.io").is_err());
        assert!(f.check("b@BLOCKED.IO").is_err());
        assert!(f.check("a@safe.io").is_ok());
    }

    #[test]
    fn allowlist_mode_drops_unlisted() {
        let f = filter(FilterConfig {
            allowed_domains: vec!["example.com".into()],
            ..Default::default()
        });
        assert!(f.check("user@example.com").is_ok());
        assert!(f.check("user@other.com").is_err());
    }

    #[test]
    fn blocklist_takes_priority_over_allowlist() {
        let f = filter(FilterConfig {
            allowed_domains: vec!["example.com".into()],
            blocked_emails: vec!["banned@example.com".into()],
            ..Default::default()
        });
        assert!(f.check("banned@example.com").is_err());
        assert!(f.check("ok@example.com").is_ok());
    }
}
