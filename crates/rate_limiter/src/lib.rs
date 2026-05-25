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

/// Outcome of a [`MailRateLimiter::wait_for_token`] call.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenResult {
    /// Token was available immediately — no throttling occurred.
    Acquired,
    /// Token was acquired after waiting — the service is being rate-limited.
    AcquiredAfterWait,
    /// Shutdown was signalled before a token became available.
    Shutdown,
}

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

    /// Wait asynchronously until a send token is available, or until `shutdown`
    /// is cancelled.
    ///
    /// Returns [`TokenResult::Acquired`] if a token was immediately available,
    /// [`TokenResult::AcquiredAfterWait`] if the caller had to be throttled,
    /// or [`TokenResult::Shutdown`] if shutdown fired before a token was ready.
    ///
    /// Callers should propagate [`TokenResult::Shutdown`] instead of proceeding
    /// with the send.  The distinction between `Acquired` and `AcquiredAfterWait`
    /// lets callers increment a throttle counter only when the service is actually
    /// being rate-limited, rather than on every send.
    ///
    /// Returns [`TokenResult::Acquired`] immediately when rate limiting is disabled.
    pub async fn wait_for_token(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
    ) -> TokenResult {
        let Some(limiter) = &self.inner else {
            return TokenResult::Acquired;
        };
        // Try to grab a token without waiting first. `check` is non-blocking and
        // returns Ok if a token is available right now.
        if limiter.check().is_ok() {
            debug!("Rate limit token acquired immediately");
            return TokenResult::Acquired;
        }
        // No token available — we have to wait. This is the true throttle case.
        tokio::select! {
            _ = limiter.until_ready() => {
                debug!("Rate limit token acquired after wait");
                TokenResult::AcquiredAfterWait
            }
            _ = shutdown.cancelled() => {
                debug!("Rate limit wait interrupted by shutdown");
                TokenResult::Shutdown
            }
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

    // ── Helpers ───────────────────────────────────────────────────────────────
    //
    // We deliberately avoid wall-clock assertions such as `elapsed < 50ms` here.
    // Those checks are inherently flaky under CI load (slow VMs, noisy neighbours,
    // Tokio scheduling jitter) and provide almost no signal beyond "the async
    // runtime is alive". Correctness is verified through the *return value* of
    // `wait_for_token`: `TokenResult::Acquired` means no waiting occurred, while
    // `TokenResult::AcquiredAfterWait` / `TokenResult::Shutdown` indicate that a
    // wait did happen. Testing the actual timing of rate-limit delays would
    // require a mock clock (e.g. `tokio::time::pause` + `advance`), which is
    // left as a future improvement if sub-millisecond precision ever matters.

    #[tokio::test]
    async fn passthrough_when_disabled() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 0,
            burst_size: 1,
        });
        assert!(rl.is_disabled());
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Disabled limiter must always return Acquired (no wait).
        assert_eq!(rl.wait_for_token(&shutdown).await, TokenResult::Acquired);
    }

    #[tokio::test]
    async fn burst_passes_immediately() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 5,
            burst_size: 5,
        });
        let shutdown = tokio_util::sync::CancellationToken::new();
        // All 5 burst tokens must be acquired without waiting.
        for _ in 0..5 {
            assert_eq!(
                rl.wait_for_token(&shutdown).await,
                TokenResult::Acquired,
                "all burst tokens should be available without waiting"
            );
        }
    }

    #[tokio::test]
    async fn throttle_returns_acquired_after_wait() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 100,
            burst_size: 1,
        });
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Drain the burst token immediately.
        assert_eq!(rl.wait_for_token(&shutdown).await, TokenResult::Acquired);
        // Next call must wait — should return AcquiredAfterWait.
        assert_eq!(
            rl.wait_for_token(&shutdown).await,
            TokenResult::AcquiredAfterWait
        );
    }

    #[tokio::test]
    async fn shutdown_interrupts_wait() {
        let rl = MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 1,
            burst_size: 1,
        });
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Drain the one burst token.
        assert_eq!(rl.wait_for_token(&shutdown).await, TokenResult::Acquired);
        // Cancel immediately — next wait must return Shutdown, not block.
        shutdown.cancel();
        assert_eq!(
            rl.wait_for_token(&shutdown).await,
            TokenResult::Shutdown,
            "cancelled wait should return Shutdown without blocking"
        );
    }
}
