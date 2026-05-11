//! Mail-server rate limiter.
//!
//! Wraps [`governor`]'s direct rate limiter in a thin, async-friendly API.
//!
//! All consumer tasks share a single `MailRateLimiter` instance (via `Arc`).
//! Before calling `EmailSender::send()`, the task calls `wait_for_token()`,
//! which blocks asynchronously until the token bucket has capacity.
//!
//! **Relationship to the concurrency semaphore**
//!
//! | Control point | Guards against |
//! |---|---|
//! | `Semaphore` (consumer) | Too many *simultaneous* tasks |
//! | `MailRateLimiter` (mailer) | Too many *sends per second* to the mail server |
//!
//! The two are complementary: the semaphore prevents task explosion; the rate
//! limiter enforces the provider's throughput quota.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Configuration loaded from `[rate_limit]` in `config/default.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    /// Maximum emails per second sent to the mail server.
    /// Set to 0 to disable rate limiting entirely.
    #[serde(default = "default_emails_per_second")]
    pub emails_per_second: u32,

    /// Token bucket burst size — allows short bursts above the steady-state
    /// rate. Must be ≥ 1. A value equal to `emails_per_second` gives a
    /// "smooth" limiter; a higher value allows bursting.
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
}

fn default_emails_per_second() -> u32 {
    10
}
fn default_burst_size() -> u32 {
    20
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            emails_per_second: default_emails_per_second(),
            burst_size: default_burst_size(),
        }
    }
}

type DirectLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Shared token-bucket rate limiter for outbound email sends.
///
/// Clone cheaply — the inner limiter is behind an `Arc`.
#[derive(Clone)]
pub struct MailRateLimiter {
    inner: Option<Arc<DirectLimiter>>,
}

impl MailRateLimiter {
    /// Construct from config.  If `emails_per_second == 0`, all `wait_for_token`
    /// calls return immediately (passthrough mode).
    pub fn new(cfg: RateLimitConfig) -> Self {
        if cfg.emails_per_second == 0 {
            return Self { inner: None };
        }

        let per_sec = NonZeroU32::new(cfg.emails_per_second).expect("emails_per_second > 0");
        let burst = NonZeroU32::new(cfg.burst_size.max(1)).expect("burst_size >= 1");
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        let limiter = RateLimiter::direct(quota);

        Self {
            inner: Some(Arc::new(limiter)),
        }
    }

    /// Wait asynchronously until a send token is available.
    ///
    /// Uses `governor::RateLimiter::until_ready()` which suspends the task
    /// without spinning — the executor is not woken until the bucket actually
    /// has capacity.
    ///
    /// Returns immediately when rate limiting is disabled.
    pub async fn wait_for_token(&self) {
        if let Some(limiter) = &self.inner {
            limiter.until_ready().await;
            debug!("Rate limit token acquired");
        }
    }

    /// `true` when rate limiting is disabled (passthrough mode).
    pub fn is_disabled(&self) -> bool {
        self.inner.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn passthrough_when_disabled() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 0,
            burst_size: 1,
        });
        assert!(rl.is_disabled());
        let t = Instant::now();
        rl.wait_for_token().await;
        assert!(t.elapsed().as_millis() < 50);
    }

    #[tokio::test]
    async fn burst_passes_immediately() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 5,
            burst_size: 5,
        });
        let t = Instant::now();
        for _ in 0..5 {
            rl.wait_for_token().await;
        }
        assert!(t.elapsed().as_millis() < 200, "burst took too long");
    }
}
