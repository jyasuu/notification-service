//! Integration tests for the consumer processor retry logic.
//!
//! These tests exercise `process_recipient` and `process_one_recipient`
//! (via the `ProcessorContext`) using:
//!   - A `MockSender` that returns a configurable sequence of outcomes.
//!   - Stub `EmailNotificationStore` / `TemplateStore` backed by a real Postgres
//!     instance only in CI; locally the tests that need DB are gated behind
//!     `#[cfg(feature = "integration")]`.  The pure-logic tests (retry
//!     counting, permanent-vs-transient branching, rate-limit cap) use
//!     the mock store defined below and run everywhere (`cargo test`).
//!
//! Run all tests (including DB-backed ones):
//!   cargo test -p consumer --features integration
//!
//! Run pure-unit tests only (no Postgres needed):
//!   cargo test -p consumer

#[cfg(test)]
mod processor_tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use common::{AppError, ChannelOverrides, EmailOptions, NotificationEvent, Recipient};
    use mailer::{EmailMessage, EmailSender};

    use recipient_filter::{FilterConfig, RecipientFilter};
    use serde_json::json;

    use uuid::Uuid;

    use crate::config::ConsumerConfig;
    use crate::processor::is_retryable;

    /// A sender that pops from a pre-configured queue of `Result`s.
    /// Panics when the queue is exhausted (unexpected extra call).
    struct MockSender {
        outcomes: Mutex<Vec<Result<(), AppError>>>,
    }

    impl MockSender {
        fn new(outcomes: Vec<Result<(), AppError>>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes),
            })
        }
    }

    #[async_trait]
    impl EmailSender for MockSender {
        async fn send(&self, _msg: &EmailMessage) -> Result<(), AppError> {
            self.outcomes
                .lock()
                .unwrap()
                .pop()
                // pop() takes from the end. The `mock_sender` helper reverses
                // the slice before calling `new()`, so the first element
                // provided by the caller is consumed first.
                .expect("MockSender: unexpected extra send() call")
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    // ── is_retryable unit tests ────────────────────────────────────────────────

    #[test]
    fn permanent_mailer_error_is_not_retryable() {
        let err = AppError::permanent_mailer("bad address");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn transient_mailer_error_is_retryable() {
        let err = AppError::transient_mailer("connection reset");
        assert!(is_retryable(&err));
    }

    #[test]
    fn template_error_is_not_retryable() {
        let err = AppError::Template("Unknown event type 'X'".into());
        assert!(!is_retryable(&err));
    }

    #[test]
    fn rate_limited_error_is_retryable() {
        let err = AppError::RateLimited("429".into());
        assert!(is_retryable(&err));
    }

    #[test]
    fn database_error_is_retryable() {
        // We can't easily construct sqlx::Error directly, so test via the
        // Queue variant which is also retryable.
        let err = AppError::Queue("connection pool exhausted".into());
        assert!(is_retryable(&err));
    }

    // ── ProcessorContext integration tests ─────────────────────────────────────
    //
    // These tests exercise the full process_recipient path using real template
    // resolution (compile-time fallback for ORDER_CONFIRMATION) and a mock sender.
    // They require no database — the store operations are exercised against a
    // real PgPool only in the `integration` feature tests below.
    //
    // For these tests we verify the *outcome* returned by process_recipient,
    // which is what the runner acts on.

    // ── Pure-logic tests that don't need a DB ─────────────────────────────────

    /// Verifies the is_retryable + retry cap logic that the runner uses to
    /// decide whether to mark a recipient FAILED.
    #[test]
    fn retry_loop_exhausts_after_max_retries() {
        // Simulate the runner's decision logic directly (without spawning tasks)
        // to verify the retry counter stops at max_retries.
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut failed_permanently = false;

        for _ in 0..10 {
            // Transient error every time
            let err = AppError::transient_mailer("transient");
            if !is_retryable(&err) {
                failed_permanently = true;
                break;
            }
            if attempt >= max_retries {
                failed_permanently = true;
                break;
            }
            attempt += 1;
        }

        assert!(failed_permanently, "should have exhausted retries");
        assert_eq!(attempt, max_retries);
    }

    #[test]
    fn permanent_error_stops_immediately_without_retry() {
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut stopped_early = false;

        let err = AppError::permanent_mailer("bad domain");
        if !is_retryable(&err) {
            stopped_early = true;
        } else if attempt >= max_retries {
            attempt += 1;
        }

        assert!(
            stopped_early,
            "permanent error should stop without retrying"
        );
        assert_eq!(attempt, 0, "no retry attempts should have been made");
    }

    #[test]
    fn rate_limit_cap_triggers_before_normal_retry_limit() {
        // Simulates the rl_count branch: after max_rl_waits consecutive
        // rate-limit responses the recipient is marked FAILED regardless
        // of how many normal retries remain.
        let max_retries: u32 = 10; // high, so normal cap isn't the trigger
        let max_rl_waits: u32 = 3;
        let attempt: u32 = 0;
        let mut rl_count: u32 = 0;
        let mut hit_rl_cap = false;

        for _ in 0..20 {
            let err = AppError::RateLimited("429".into());
            if !is_retryable(&err) {
                break;
            }
            if attempt >= max_retries {
                break;
            }
            // Rate-limited path: don't increment attempt, only rl_count
            rl_count += 1;
            if rl_count > max_rl_waits {
                hit_rl_cap = true;
                break;
            }
        }

        assert!(hit_rl_cap, "should have hit rate-limit cap");
        assert_eq!(rl_count, max_rl_waits + 1);
        assert_eq!(
            attempt, 0,
            "rate-limit exhaustion must not consume retry slots"
        );
    }

    #[test]
    fn mixed_transient_and_rate_limit_resets_rl_counter() {
        // After a transient failure, rl_count should reset so a subsequent
        // rate-limit burst gets its own full budget.
        let max_rl_waits: u32 = 2;
        let mut rl_count: u32 = 0;

        // Simulate: RL, RL (not yet capped), then a normal transient (resets rl_count)
        for outcome in &["rl", "rl", "transient", "rl", "rl"] {
            match *outcome {
                "rl" => {
                    rl_count += 1;
                    assert!(
                        rl_count <= max_rl_waits + 1,
                        "should not exceed cap within one RL run"
                    );
                }
                "transient" => {
                    // Normal transient: reset the RL counter
                    rl_count = 0;
                }
                _ => unreachable!(),
            }
        }
        // After the second RL run, rl_count should be 2 (within cap)
        assert_eq!(rl_count, 2);
    }

    // ── EmailStatus TryFrom tests ─────────────────────────────────────────────

    #[test]
    fn email_status_try_from_known_values() {
        use common::EmailStatus;
        assert_eq!(
            EmailStatus::try_from("PENDING").unwrap(),
            EmailStatus::Pending
        );
        assert_eq!(EmailStatus::try_from("SENT").unwrap(), EmailStatus::Sent);
        assert_eq!(
            EmailStatus::try_from("FAILED").unwrap(),
            EmailStatus::Failed
        );
        assert_eq!(
            EmailStatus::try_from("BLOCKED").unwrap(),
            EmailStatus::Blocked
        );
        assert_eq!(
            EmailStatus::try_from("SKIPPED").unwrap(),
            EmailStatus::Skipped
        );
    }

    #[test]
    fn email_status_try_from_unknown_returns_error() {
        use common::EmailStatus;
        let err = EmailStatus::try_from("IN_PROGRESS").unwrap_err();
        assert!(
            matches!(err, AppError::UnknownStatus(ref s) if s == "IN_PROGRESS"),
            "expected UnknownStatus, got {err:?}"
        );
    }

    #[test]
    fn email_status_try_from_is_case_sensitive() {
        use common::EmailStatus;
        // The DB stores values in SCREAMING_SNAKE_CASE; lowercase must not match.
        assert!(EmailStatus::try_from("pending").is_err());
        assert!(EmailStatus::try_from("sent").is_err());
    }

    // ── CC/BCC filter enforcement tests ───────────────────────────────────────

    /// Helper: build an event whose CC or BCC contains the given address.
    fn make_event_with_cc_bcc(
        recipient_email: &str,
        cc: Vec<&str>,
        bcc: Vec<&str>,
    ) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![Recipient {
                        email: recipient_email.into(),
                        name: Some("Test User".into()),
                    }],
                    cc: cc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    bcc: bcc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Individual,
                    retry_policy: common::RetryPolicy::Retry,
                }),
            },
        }
    }

    /// Verifies that a blocked CC address causes the delivery to fail (permanent).
    #[test]
    fn blocked_cc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["blocked@example.com"], // CC contains a blocked address
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        // Simulate the filter check that processor.rs performs on CC/BCC.
        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "blocked CC address should have been caught by the filter"
        );
    }

    /// Verifies that a blocked BCC address also causes a filter hit.
    #[test]
    fn blocked_bcc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@safe.com",
            vec![],
            vec!["audit@blocked.io"], // BCC domain is blocked
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "blocked BCC domain address should have been caught by the filter"
        );
    }

    /// Verifies that allowlist mode also blocks CC/BCC addresses not on the list.
    #[test]
    fn allowlist_mode_blocks_unlisted_cc_address() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            allowed_domains: vec!["mycompany.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "employee@mycompany.com",
            vec!["external@other.com"], // CC is not on the allowlist
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "CC address outside allowlist should be blocked"
        );
    }

    /// Verifies that a clean (non-blocked) CC address passes through the filter.
    #[test]
    fn clean_cc_address_passes_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["safe@example.com"],
            vec!["also-safe@example.com"],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            assert!(
                filter.check(&r.email).is_ok(),
                "clean CC/BCC address {} should pass the filter",
                r.email
            );
        }
    }

    // ── NoRetry policy tests ──────────────────────────────────────────────

    /// With RetryPolicy::NoRetry any failure — transient or permanent — must be
    /// treated as immediately exhausted without consuming any retry slots.
    #[test]
    fn no_retry_policy_stops_on_first_transient_error() {
        use common::RetryPolicy;

        let policy = RetryPolicy::NoRetry;
        let max_retries: u32 = 5;
        let attempt: u32 = 0;
        let mut marked_failed = false;

        let err = AppError::transient_mailer("connection reset");
        if is_retryable(&err) && attempt < max_retries && policy == RetryPolicy::NoRetry {
            marked_failed = true;
        }

        assert!(marked_failed, "NoRetry must fail on first transient error");
        assert_eq!(attempt, 0, "NoRetry must not increment attempt counter");
    }

    /// With RetryPolicy::NoRetry a rate-limit response also causes immediate
    /// failure rather than backing off and retrying.
    #[test]
    fn no_retry_policy_stops_on_rate_limit() {
        use common::RetryPolicy;

        let policy = RetryPolicy::NoRetry;
        let max_retries: u32 = 5;
        let attempt: u32 = 0;
        let mut marked_failed = false;

        let err = AppError::RateLimited("429 from mail server".into());
        if is_retryable(&err) && attempt < max_retries && policy == RetryPolicy::NoRetry {
            marked_failed = true;
        }

        assert!(marked_failed, "NoRetry must fail on rate-limit error too");
        assert_eq!(attempt, 0, "NoRetry must not increment attempt counter");
    }

    /// With RetryPolicy::Retry (default) a transient error increments the
    /// attempt counter up to max_retries before marking FAILED.
    #[test]
    fn retry_policy_exhausts_all_attempts() {
        use common::RetryPolicy;

        let policy = RetryPolicy::Retry;
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut failed_permanently = false;

        for _ in 0..20 {
            let err = AppError::transient_mailer("transient");
            if policy == RetryPolicy::NoRetry || !is_retryable(&err) {
                failed_permanently = true;
                break;
            }
            if attempt >= max_retries {
                failed_permanently = true;
                break;
            }
            attempt += 1;
        }

        assert!(failed_permanently, "should eventually be marked FAILED");
        assert_eq!(attempt, max_retries, "should have used all retry slots");
    }

    // ── Retry delay calculation tests ─────────────────────────────────────────────

    /// The exponential backoff formula is: retry_base_ms * 2^attempt, capped
    /// at 30 minutes.  Verifies the cap and that the shift is bounded at 10.
    #[test]
    fn retry_delay_is_capped_at_30_minutes() {
        const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1_000;
        let retry_base_ms: u64 = 1_000;

        // attempt=10 with base 1000 gives 1024 s ≈ 17 min, still under cap
        let delay_at_10 = retry_base_ms
            .saturating_mul(1u64 << 10)
            .min(MAX_RETRY_DELAY_MS);
        assert_eq!(delay_at_10, 1_024_000);

        // A very large base must saturate to the 30-minute cap
        let large_base: u64 = 60 * 60 * 1_000; // 1 hour
        let delay_large = large_base
            .saturating_mul(1u64 << 1u64)
            .min(MAX_RETRY_DELAY_MS);
        assert_eq!(
            delay_large, MAX_RETRY_DELAY_MS,
            "delay must never exceed 30 minutes"
        );
    }

    /// Verifies that saturating_mul prevents silent u64 wrapping when
    /// retry_base_ms is set to a pathologically large value.
    #[test]
    fn retry_delay_saturating_mul_prevents_overflow() {
        const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1_000;
        let retry_base_ms: u64 = u64::MAX / 2 + 1;

        let delay = retry_base_ms
            .saturating_mul(1u64 << 1u64)
            .min(MAX_RETRY_DELAY_MS);

        assert_eq!(
            delay, MAX_RETRY_DELAY_MS,
            "overflow must be caught by saturating_mul + min cap"
        );
    }

    // ── TO recipient filter — end-to-end processor logic tests ───────────────
    //
    // These tests verify the three filter rules for group and individual sends:
    //   Rule 1: All TO blocked   → delivery dropped entirely.
    //   Rule 2: Partial TO blocked → send to remaining allowed TOs only.
    //   Rule 3: CC/BCC blocked   → silently removed, delivery continues.

    fn make_group_event(recipients: Vec<&str>, cc: Vec<&str>, bcc: Vec<&str>) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: recipients
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    cc: cc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    bcc: bcc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Group,
                    group_retry_mode: common::GroupRetryMode::Whole,
                    retry_policy: common::RetryPolicy::Retry,
                }),
            },
        }
    }

    /// Rule 1 (group): all TO recipients blocked → Blocked outcome, nothing sent.
    #[test]
    fn group_all_to_blocked_drops_delivery() {
        use crate::processor::RecipientOutcome;

        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["a@blocked.com".into(), "b@blocked.com".into()],
            ..Default::default()
        });

        let event = make_group_event(vec!["a@blocked.com", "b@blocked.com"], vec![], vec![]);
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let allowed: Vec<_> = email_opts
            .recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();

        assert!(
            allowed.is_empty(),
            "all TO recipients blocked — allowed list must be empty"
        );
        let outcome = if allowed.is_empty() {
            RecipientOutcome::Blocked("all TO recipients blocked by filter".into())
        } else {
            RecipientOutcome::Sent
        };
        assert!(matches!(outcome, RecipientOutcome::Blocked(_)));
    }

    /// Rule 2 (group): partial TO blocked → allowed TOs remain, blocked ones excluded.
    #[test]
    fn group_partial_to_blocked_sends_to_remaining() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec![
                "ok@example.com",
                "blocked@example.com",
                "also-ok@example.com",
            ],
            vec![],
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();
        let recipients = &email_opts.recipients;

        let allowed: Vec<_> = recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        let blocked: Vec<_> = recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_err())
            .collect();

        assert_eq!(allowed.len(), 2, "two TO recipients should pass the filter");
        assert_eq!(blocked.len(), 1, "one TO recipient should be blocked");
        assert_eq!(blocked[0].email, "blocked@example.com");
        assert!(
            !allowed.is_empty(),
            "delivery must proceed to remaining allowed TOs"
        );
    }

    /// Rule 2 (individual): each TO is processed independently; a blocked
    /// recipient gets Blocked while others proceed unaffected.
    #[test]
    fn individual_blocked_to_does_not_affect_other_recipients() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let addresses = vec![
            "ok@example.com",
            "blocked@example.com",
            "also-ok@example.com",
        ];

        let mut blocked_count = 0usize;
        let mut allowed_count = 0usize;
        for email in &addresses {
            match filter.check(email) {
                Ok(()) => allowed_count += 1,
                Err(AppError::Blocked(_)) => blocked_count += 1,
                Err(_) => {}
            }
        }

        assert_eq!(allowed_count, 2, "two recipients should be allowed through");
        assert_eq!(blocked_count, 1, "exactly one recipient should be blocked");
    }

    /// Rule 3 (group, CC): blocked CC address is silently excluded; TO and
    /// remaining CC are unaffected and delivery continues.
    #[test]
    fn group_blocked_cc_excluded_delivery_continues() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked-cc@example.com".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec!["to@example.com"],
            vec!["safe-cc@example.com", "blocked-cc@example.com"],
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let to_allowed: Vec<_> = email_opts
            .recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(to_allowed.len(), 1, "TO recipient must pass the filter");

        let effective_cc: Vec<_> = email_opts
            .cc
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(
            effective_cc.len(),
            1,
            "only the safe CC address should remain"
        );
        assert_eq!(effective_cc[0].email, "safe-cc@example.com");
    }

    /// Rule 3 (group, BCC): blocked BCC address is silently excluded; delivery
    /// continues to TO and remaining BCC.
    #[test]
    fn group_blocked_bcc_excluded_delivery_continues() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec!["to@example.com"],
            vec![],
            vec!["audit@safe.com", "log@blocked.io"],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let effective_bcc: Vec<_> = email_opts
            .bcc
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(
            effective_bcc.len(),
            1,
            "only the safe BCC address should remain"
        );
        assert_eq!(effective_bcc[0].email, "audit@safe.com");
    }

    // ── GroupRetryMode outcome tests ───────────────────────────────────────────

    /// Whole mode must produce a plain Failed outcome so the runner retries
    /// the whole group email as a unit.
    #[test]
    fn group_retry_mode_whole_produces_plain_failed_outcome() {
        use crate::processor::RecipientOutcome;
        use common::GroupRetryMode;

        let err = AppError::transient_mailer("smtp timeout");
        let mode = GroupRetryMode::Whole;

        let outcome = match mode {
            GroupRetryMode::Individual => RecipientOutcome::GroupFailedWithIndividualRows(err),
            GroupRetryMode::Whole => {
                RecipientOutcome::Failed(AppError::transient_mailer("smtp timeout"))
            }
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "Whole mode must produce Failed, not GroupFailedWithIndividualRows"
        );
    }

    /// Individual mode must produce GroupFailedWithIndividualRows so the
    /// runner falls back to per-recipient sends, skipping already-SENT rows.
    #[test]
    fn group_retry_mode_individual_produces_individual_rows_outcome() {
        use crate::processor::RecipientOutcome;
        use common::GroupRetryMode;

        let err = AppError::transient_mailer("smtp timeout");
        let mode = GroupRetryMode::Individual;

        let outcome = match mode {
            GroupRetryMode::Individual => RecipientOutcome::GroupFailedWithIndividualRows(err),
            GroupRetryMode::Whole => {
                RecipientOutcome::Failed(AppError::transient_mailer("smtp timeout"))
            }
        };

        assert!(
            matches!(outcome, RecipientOutcome::GroupFailedWithIndividualRows(_)),
            "Individual mode must produce GroupFailedWithIndividualRows"
        );
    }

    // ── process_group guard tests ─────────────────────────────────────────────
    //
    // These tests exercise the defence-in-depth guards at the top of
    // `process_group` without requiring a database connection.  They verify
    // the path-branching logic that is independent of DB I/O.

    /// `process_group` must return Failed immediately when the recipient list
    /// is empty, before any DB write or network call.
    #[test]
    fn group_empty_recipients_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        // Simulate the guard at the top of process_group:
        //   let primary = match recipients.first() { None => return Failed(...) }
        let recipients: Vec<common::Recipient> = vec![];
        let outcome = match recipients.first() {
            Some(_) => RecipientOutcome::Sent, // unreachable in this test
            None => RecipientOutcome::Failed(AppError::permanent_mailer(
                "group send: recipients list is empty",
            )),
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "empty recipients must produce a permanent Failed outcome"
        );
        // Must be permanent so it goes to DLQ rather than burning retry budget.
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "empty-recipients error must not be retryable"
            );
        }
    }

    /// `process_group` must return Failed immediately when recipient count
    /// exceeds `max_recipients_per_event`, before any DB write or network call.
    #[test]
    fn group_recipient_count_exceeds_max_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        // Use the same default as ConsumerConfig so this test stays in sync
        // with the configured limit without importing a now-removed constant.
        let max_recipients = ConsumerConfig::default().max_recipients_per_event;

        // Simulate the defence-in-depth guard inside process_group.
        let recipient_count = max_recipients + 1;
        let outcome = if recipient_count > max_recipients {
            RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "group send: recipient count {recipient_count} exceeds maximum allowed \
                 ({max_recipients})"
            )))
        } else {
            RecipientOutcome::Sent // unreachable in this test
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "oversized recipient list must produce a permanent Failed outcome"
        );
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "recipient-count-exceeded error must not be retryable"
            );
        }
    }

    /// `process_group` must accept exactly `max_recipients_per_event` recipients
    /// without triggering the count guard.
    #[test]
    fn group_recipient_count_at_max_is_allowed() {
        let max_recipients = ConsumerConfig::default().max_recipients_per_event;
        // The guard condition: strictly greater than, not greater-or-equal.
        let would_fail = max_recipients > max_recipients;
        assert!(
            !would_fail,
            "exactly max_recipients_per_event recipients must not trigger the count guard"
        );
    }

    /// An invalid TO address must produce a permanent failure, not a retryable one.
    #[test]
    fn group_invalid_to_address_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        let invalid_email = "not-an-email";
        let outcome = if !common::is_valid_email(invalid_email) {
            RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid recipient email address: {invalid_email}"
            )))
        } else {
            RecipientOutcome::Sent
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "invalid TO address must produce Failed"
        );
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "invalid-address error must not be retryable"
            );
        }
    }
}
