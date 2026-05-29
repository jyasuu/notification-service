-- migrations/0001_initial_schema.sql
--
-- Initial schema for the anvil-notify notification DB.
--
-- This single file replaces the 30 incremental migrations (0001–0030) that
-- were accumulated during development.  It represents the final schema
-- directly — no intermediate states, no backfills, no renamed tables.
--
-- DO NOT apply this to a database that already has the incremental migrations
-- applied.  It is intended for fresh installations only.  Existing deployments
-- must continue running the numbered incremental migrations.
--
-- Tables created here:
--   outbox                  — copy of the business-service outbox table;
--                             applied here so the migrate container can run
--                             a single migration set against both DBs.
--                             The notification service itself does not query
--                             this table; the outbox worker does.
--   notification_log        — channel-agnostic delivery tracking (one row per
--                             event × channel × recipient).
--   email_notification_log  — email-specific delivery detail (1:1 with
--                             notification_log where channel = 'email').
--   notification_template   — Jinja2 templates for all notification channels.
--   block_list              — runtime recipient block/allow-list.

-- CREATE EXTENSION IF NOT EXISTS "pgcrypto"; 

-- ── outbox ────────────────────────────────────────────────────────────────────
--
-- The business service writes rows here; the outbox worker polls them and
-- publishes to RabbitMQ.  See migrations/business_db/ for the version applied
-- to the actual business-service database.

CREATE TABLE IF NOT EXISTS outbox (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Stable ID forwarded as NotificationEvent.event_id — used for idempotency
    -- in the notification service.
    event_id     UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),

    -- Logical event type, e.g. 'ORDER_CONFIRMATION', 'PASSWORD_RESET'.
    event_type   TEXT        NOT NULL,

    -- Full event body.  See migrations/business_db/0002_create_outbox.sql for
    -- the detailed payload contract.
    payload      JSONB       NOT NULL,

    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED')),

    -- Incremented by the outbox worker on each failed publish attempt.
    -- Once fail_count reaches the configured threshold the row is permanently
    -- marked FAILED and removed from the retry pool.
    fail_count   INT         NOT NULL DEFAULT 0,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    -- Set to now() when the worker claims a row (IN_PROGRESS).
    -- Cleared when the row reaches PUBLISHED or FAILED.
    -- Used by the stale-row reaper to detect and recover rows stranded by a
    -- worker crash.  NULL for rows created before migration 0016 was applied.
    locked_at    TIMESTAMPTZ
);

-- FOR UPDATE SKIP LOCKED requires an index on (status, created_at).
CREATE INDEX IF NOT EXISTS outbox_status_created_idx
    ON outbox (status, created_at ASC)
    WHERE status = 'PENDING';

-- Monitoring: find permanently failed rows quickly.
CREATE INDEX IF NOT EXISTS outbox_failed_idx
    ON outbox (created_at DESC)
    WHERE status = 'FAILED';

-- Monitoring: find rows with a From-address override.
CREATE INDEX IF NOT EXISTS outbox_from_override_idx
    ON outbox ((payload->>'from_override'))
    WHERE payload ? 'from_override';

-- Monitoring: find rows with attachments.
CREATE INDEX IF NOT EXISTS outbox_has_attachments_idx
    ON outbox ((jsonb_array_length(payload -> 'attachments')))
    WHERE payload ? 'attachments'
      AND jsonb_array_length(payload -> 'attachments') > 0;

-- Reaper index: find stale IN_PROGRESS rows quickly.
CREATE INDEX IF NOT EXISTS outbox_locked_at_idx
    ON outbox (locked_at ASC)
    WHERE status = 'IN_PROGRESS';

-- ── notification_log ──────────────────────────────────────────────────────────
--
-- Channel-agnostic delivery tracking.  One row per (event_id, channel,
-- recipient_id).  The idempotency constraint at the bottom prevents duplicate
-- rows even if the same event is published more than once.
--
-- channel discriminator values: 'email', 'sms', 'push', …
-- recipient_id is the channel-native identity:
--   email → recipient email address
--   sms   → E.164 phone number (e.g. '+886912345678')
--   push  → device/registration token

CREATE TABLE IF NOT EXISTS notification_log (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    event_id         UUID        NOT NULL,
    event_type       TEXT        NOT NULL,

    -- Delivery channel: 'email', 'sms', 'push', …
    channel          TEXT        NOT NULL,

    -- Channel-native recipient identity (email address, phone number, token).
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

-- ── email_notification_log ────────────────────────────────────────────────────
--
-- Email-specific delivery metadata.  Every row here has exactly one parent row
-- in notification_log (notification_log.channel = 'email').
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

    -- Mirrors notification_log.recipient_id but typed as an email address.
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
    attachments      JSONB,

    -- Retry strategy for group-mode events.
    --   'whole'      — retry the whole group email as a unit (default).
    --   'individual' — on failure, fall back to per-recipient individual sends,
    --                  skipping addresses that already have a SENT row.
    -- NULL for rows written before migration 0028; treated as 'whole' on retry.
    group_retry_mode TEXT        CHECK (group_retry_mode IN ('whole', 'individual')),

    -- Full To: recipient list for group sends with group_retry_mode = 'whole'.
    -- NULL for individual sends and group sends with group_retry_mode = 'individual'
    -- (those modes write one row per recipient, so this column would be redundant).
    -- Each element: { "email": "...", "name": "..." }
    to_recipients    JSONB
);

COMMENT ON TABLE  email_notification_log               IS 'Email-specific delivery detail. 1:1 with notification_log rows where channel = ''email''.';
COMMENT ON COLUMN email_notification_log.send_mode     IS '''individual'': each recipient gets a separate email. ''group'': all recipients share one To: header.';
COMMENT ON COLUMN email_notification_log.from_override IS 'Per-event From address override. NULL means use global [mailer] defaults.';
COMMENT ON COLUMN email_notification_log.sender_account IS 'Named SMTP account key. NULL means use global [mailer] defaults.';
COMMENT ON COLUMN email_notification_log.group_retry_mode IS
    'Retry strategy for group-mode events: ''whole'' (retry as a unit) or '
    '''individual'' (fall back to per-recipient sends, skipping SENT rows). '
    'NULL for rows written before migration 0028; treated as ''whole'' on retry.';
COMMENT ON COLUMN email_notification_log.to_recipients IS
    'Full To: recipient list for group sends with group_retry_mode = ''whole''. '
    'NULL for individual sends and group sends with group_retry_mode = ''individual'' '
    '(those modes write one row per recipient, so the column would be redundant). '
    'Each element: {"email": "...", "name": "..."}. NULL for pre-0030 rows.';

-- ── notification_template ─────────────────────────────────────────────────────
--
-- Jinja2 (minijinja) templates for all notification channels.
-- One row per (type, channel).
--
-- type    — matches NotificationEvent.event_type (e.g. 'ORDER_CONFIRMATION').
-- channel — delivery channel this template applies to: 'email', 'sms', etc.
-- subject — Jinja2 template string.
-- body_html / body_text — rendered separately; body_text is required as the
--           plain-text fallback for clients that don't render HTML.
-- version — monotonically increasing; bump when editing a template so audit
--           logs can reference which version sent a given email.
-- active  — set FALSE to disable an event type without deleting it.
--
-- Template syntax quick-reference:
--   {{ variable }}           — HTML-escaped in body_html, verbatim elsewhere
--   {{ variable | safe }}    — insert trusted HTML verbatim (body_html only)
--   {% if condition %}...{% endif %}
--   {% for item in list %}...{% endfor %}
--
-- To add a new event type: INSERT a row here.  No code change or service
-- restart is required; the in-memory cache (default TTL 5 min) picks up
-- new rows automatically.  Use DELETE /templates/<type>/cache for immediate
-- invalidation.

CREATE TABLE IF NOT EXISTS notification_template (
    type         TEXT        NOT NULL,
    channel      TEXT        NOT NULL DEFAULT 'email',
    subject      TEXT        NOT NULL,
    body_html    TEXT        NOT NULL,
    body_text    TEXT        NOT NULL,
    version      INT         NOT NULL DEFAULT 1,
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (type, channel)
);

COMMENT ON TABLE  notification_template         IS 'Jinja2 templates for all notification channels. One row per (event_type, channel).';
COMMENT ON COLUMN notification_template.channel IS 'Delivery channel this template applies to: ''email'', ''sms'', ''push'', etc.';
COMMENT ON COLUMN notification_template.type    IS 'Event type key (e.g. ''ORDER_CONFIRMATION''). Must match NotificationEvent.event_type.';

-- ── Built-in templates (canonical upsert) ────────────────────────────────────
--
-- ON CONFLICT DO UPDATE guarantees idempotency on fresh installs and upgrades
-- without relying on migration ordering.  The CASE expressions avoid bumping
-- version/updated_at when content has not changed, keeping audit logs clean.
-- Templates an operator has deliberately disabled (active = FALSE) are never
-- overwritten.

INSERT INTO notification_template (type, channel, subject, body_html, body_text, version, active)
VALUES

-- ORDER_CONFIRMATION ─────────────────────────────────────────────────────────
(
    'ORDER_CONFIRMATION', 'email',
    'Order {{ orderId }} confirmed',
    '<h1>Hi {{ name }},</h1><p>Your order <strong>{{ orderId }}</strong> of ${{ amount }} has been confirmed.</p>',
    'Hi {{ name }}, Your order {{ orderId }} of ${{ amount }} has been confirmed.',
    1, TRUE
),

-- PASSWORD_RESET ─────────────────────────────────────────────────────────────
(
    'PASSWORD_RESET', 'email',
    'Reset your password',
    '<p>Click <a href="{{ resetLink }}">here</a> to reset your password.</p>',
    'Visit this link to reset your password: {{ resetLink }}',
    1, TRUE
),

-- WELCOME ────────────────────────────────────────────────────────────────────
(
    'WELCOME', 'email',
    'Welcome to {{ appName }}!',
    '<h1>Welcome, {{ name }}!</h1><p>Thanks for joining {{ appName }}.</p>',
    'Welcome, {{ name }}! Thanks for joining {{ appName }}.',
    1, TRUE
),

-- GENERIC_TEXT ───────────────────────────────────────────────────────────────
-- Plain-text email driven entirely by payload fields.
-- Required payload: { "subject": "...", "body": "..." }
(
    'GENERIC_TEXT', 'email',
    '{{ subject }}',
    '<div style="font-family:sans-serif;white-space:pre-wrap">{{ body }}</div>',
    '{{ body }}',
    1, TRUE
),

-- GENERIC_HTML ───────────────────────────────────────────────────────────────
-- Rich HTML email with a plain-text fallback.  The caller supplies pre-rendered
-- HTML; `| safe` bypasses auto-escaping — the caller is responsible for safety.
-- Required payload: { "subject": "...", "body_html": "...", "body_text": "..." }
(
    'GENERIC_HTML', 'email',
    '{{ subject }}',
    '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:24px;font-family:sans-serif;color:#111">{{ body_html | safe }}</body></html>',
    '{{ body_text }}',
    1, TRUE
)

ON CONFLICT (type, channel) DO UPDATE
    SET
        subject    = EXCLUDED.subject,
        body_html  = EXCLUDED.body_html,
        body_text  = EXCLUDED.body_text,
        -- Only bump version and updated_at when content actually changed,
        -- so no-op re-runs don't create spurious audit noise.
        version    = CASE
                         WHEN notification_template.subject   IS DISTINCT FROM EXCLUDED.subject
                           OR notification_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                           OR notification_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                         THEN notification_template.version + 1
                         ELSE notification_template.version
                     END,
        updated_at = CASE
                         WHEN notification_template.subject   IS DISTINCT FROM EXCLUDED.subject
                           OR notification_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                           OR notification_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                         THEN now()
                         ELSE notification_template.updated_at
                     END
    -- Never deactivate a template that an operator has deliberately disabled.
    WHERE notification_template.active = TRUE;

-- ── block_list ────────────────────────────────────────────────────────────────
--
-- Runtime recipient block/allow-list managed via the HTTP API.
-- Changes propagate within seconds (default cache TTL 30 s); use
-- DELETE /admin/blocklist/cache for immediate reload.
--
-- kind values:
--   'blocked_email'   — exact email address that must never receive mail.
--   'blocked_domain'  — entire domain blocked (e.g. 'competitor.com').
--   'allowed_email'   — allowlist mode: only this address may receive mail.
--   'allowed_domain'  — allowlist mode: only this domain may receive mail.

CREATE TABLE IF NOT EXISTS block_list (
    id         BIGSERIAL   PRIMARY KEY,
    kind       TEXT        NOT NULL
                           CHECK (kind IN (
                               'blocked_email', 'blocked_domain',
                               'allowed_email',  'allowed_domain'
                           )),
    value      TEXT        NOT NULL,
    reason     TEXT,
    active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique on (kind, value) so the same entry cannot be added twice.
CREATE UNIQUE INDEX IF NOT EXISTS block_list_kind_value_idx
    ON block_list (kind, lower(value));

COMMENT ON TABLE  block_list       IS 'Runtime recipient block/allow-list managed via the HTTP API.';
COMMENT ON COLUMN block_list.kind  IS 'Entry type: blocked_email | blocked_domain | allowed_email | allowed_domain';
COMMENT ON COLUMN block_list.value IS 'Lowercase email address or domain. Normalised to lowercase on insert.';
COMMENT ON COLUMN block_list.reason IS 'Operator note explaining why this entry was added.';
COMMENT ON COLUMN block_list.active IS 'Soft-delete flag. Set to FALSE to disable without losing history.';

-- Migration 0003: add SKIPPED to notification_log status check constraint.
--
-- SKIPPED is a terminal status written when the consumer ACKs a delivery
-- without attempting to send it:
--   • event has no email channel_overrides (publisher omitted the field)
--   • event has no recipients in the email channel
--   • event exceeds max_recipients_per_event
--   • (future) event targets a channel not yet supported
--
-- Unlike FAILED, SKIPPED rows are not eligible for the manual operator
-- retry API — the publisher must re-publish the event with the correct
-- channel_overrides after fixing the upstream data.
--
-- The constraint is dropped and re-created to add the new value; no existing
-- rows are touched (none can have an unrecognised status today).

ALTER TABLE notification_log
    DROP CONSTRAINT IF EXISTS notification_log_status_check;

ALTER TABLE notification_log
    ADD CONSTRAINT notification_log_status_check
        CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'BLOCKED', 'SKIPPED'));
