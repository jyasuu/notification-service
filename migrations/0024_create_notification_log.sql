-- migrations/0024_create_notification_log.sql
--
-- Phase 1 of the multi-channel notification log refactor.
--
-- Creates two new tables alongside the existing `email_log`:
--
--   notification_log          — channel-agnostic delivery tracking
--   email_notification_log    — email-specific detail (1:1 with notification_log)
--
-- `email_log` is NOT touched here and remains fully operational during the
-- transition.  The application will dual-write to both schemas in Phase 3
-- before `email_log` is dropped in Phase 5 (migration 0026).
--
-- Idempotency key change
-- ──────────────────────
-- email_log used (event_id, recipient_email).
-- notification_log uses (event_id, channel, recipient_id) so the same
-- event_id can produce deliveries on multiple channels (email + SMS etc.)
-- without colliding.  `recipient_id` is the channel-native identity:
--   email → recipient email address
--   sms   → E.164 phone number
--   push  → device token
--
-- The email_notification_log table stores all columns that are specific to
-- email delivery.  Future channels add their own sibling tables
-- (sms_notification_log, push_notification_log) without touching
-- notification_log.

-- ── notification_log ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS notification_log (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Event envelope (channel-agnostic)
    event_id         UUID        NOT NULL,
    event_type       TEXT        NOT NULL,

    -- Channel discriminator: 'email', 'sms', 'push', …
    channel          TEXT        NOT NULL,

    -- Channel-native recipient identity.
    -- For email: the recipient email address.
    -- For SMS:   E.164 phone number (e.g. '+886912345678').
    -- For push:  device/registration token.
    recipient_id     TEXT        NOT NULL,

    -- Delivery state machine: PENDING → SENT | FAILED | BLOCKED
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                                 CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'BLOCKED')),

    -- retry_count   — resets to 0 on each manual operator retry.
    -- total_attempts — lifetime counter; never reset; used for audit.
    retry_count      INT         NOT NULL DEFAULT 0,
    total_attempts   INT         NOT NULL DEFAULT 0,

    last_error       TEXT,

    -- Template variables forwarded by the publishing business service.
    payload          JSONB       NOT NULL DEFAULT '{}',

    -- Original NotificationEvent.timestamp from the business service.
    -- Stored separately from created_at so attachment expiry checks use the
    -- publication time, not the consumer processing time.
    event_timestamp  TIMESTAMPTZ NOT NULL,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency: one row per (event, channel, recipient).
    CONSTRAINT notification_log_idempotency
        UNIQUE (event_id, channel, recipient_id)
);

COMMENT ON TABLE  notification_log                IS 'Channel-agnostic notification delivery log. One row per (event, channel, recipient).';
COMMENT ON COLUMN notification_log.channel        IS 'Delivery channel: ''email'', ''sms'', ''push'', etc.';
COMMENT ON COLUMN notification_log.recipient_id   IS 'Channel-native recipient identity (email address, E.164 phone, device token).';
COMMENT ON COLUMN notification_log.retry_count    IS 'Resets to 0 on each manual operator retry. Used to gate automatic retry attempts.';
COMMENT ON COLUMN notification_log.total_attempts IS 'Lifetime attempt counter. Never reset. Used for audit and alerting on persistently failing recipients.';
COMMENT ON COLUMN notification_log.event_timestamp IS 'Original NotificationEvent.timestamp from the publishing service. Used for attachment expiry checks.';

CREATE INDEX IF NOT EXISTS notification_log_event_id_idx
    ON notification_log (event_id);

CREATE INDEX IF NOT EXISTS notification_log_status_idx
    ON notification_log (status);

CREATE INDEX IF NOT EXISTS notification_log_channel_idx
    ON notification_log (channel);

CREATE INDEX IF NOT EXISTS notification_log_event_status_idx
    ON notification_log (event_id, status);

CREATE INDEX IF NOT EXISTS notification_log_created_at_idx
    ON notification_log (created_at DESC);

-- ── email_notification_log ───────────────────────────────────────────────────
--
-- Stores email-specific delivery metadata.  Every row here has exactly one
-- parent row in notification_log (notification_log.channel = 'email').
--
-- Keeping email-specific columns here (rather than nullable columns on
-- notification_log) means:
--   • notification_log stays clean for cross-channel queries.
--   • Adding SMS/push never widens the hot notification_log table.
--   • Each channel's detail table can evolve independently.

CREATE TABLE IF NOT EXISTS email_notification_log (
    -- 1:1 with notification_log; cascade delete keeps the tables in sync.
    notification_id  UUID        PRIMARY KEY
                                 REFERENCES notification_log(id) ON DELETE CASCADE,

    -- Mirrors notification_log.recipient_id but typed as an email address
    -- for legibility and for any email-specific indexes.
    recipient_email  TEXT        NOT NULL,
    recipient_name   TEXT,

    -- From-address override supplied by the publisher.
    -- JSONB: { "email": "billing@acme.com", "name": "Acme Billing" }
    from_override    JSONB,

    -- Named sender account key from [sender_accounts] config.
    -- NULL → use the global [mailer] default.
    sender_account   TEXT,

    -- 'individual' (default) or 'group'.
    -- Stored so manual retries faithfully replay the original delivery mode.
    send_mode        TEXT        CHECK (send_mode IN ('individual', 'group')),

    -- CC / BCC recipients as JSONB arrays.
    -- Each element: { "email": "...", "name": "..." }
    cc               JSONB,
    bcc              JSONB,

    -- Attachment references as a JSONB array.
    -- Each element: { "url": "...", "filename": "...", "content_type": "...", "max_age_secs": N }
    attachments      JSONB
);

COMMENT ON TABLE  email_notification_log               IS 'Email-specific delivery detail. 1:1 with notification_log rows where channel = ''email''.';
COMMENT ON COLUMN email_notification_log.send_mode     IS '''individual'': each recipient gets a separate email. ''group'': all recipients share one To: header.';
COMMENT ON COLUMN email_notification_log.from_override IS 'Per-event From address override. NULL means use global [mailer] defaults.';
COMMENT ON COLUMN email_notification_log.sender_account IS 'Named SMTP account key. NULL means use global [mailer] defaults.';
