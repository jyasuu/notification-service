-- migrations/0026_backfill_notification_log.sql
--
-- Backfills all existing `email_log` rows into the two new tables:
--   notification_log          (channel-agnostic)
--   email_notification_log    (email-specific detail)
--
-- This migration is additive — email_log is NOT dropped here.
-- Drop happens in 0027 after the application cutover is confirmed healthy.
--
-- Safety notes
-- ────────────
-- • Wrapped in a transaction so a partial failure leaves both tables empty
--   rather than partially filled (which would cause FK violations on re-run).
-- • ON CONFLICT DO NOTHING on notification_log makes this re-runnable:
--   if the migration is interrupted and re-applied, already-inserted rows
--   are skipped safely.
-- • email_notification_log rows are inserted only when the parent row was
--   freshly inserted (not a conflict), avoiding duplicate detail rows.
-- • send_mode NULL in email_log falls back to 'individual' (same default
--   the application used before migration 0023 added the column).
-- • event_timestamp NULL falls back to created_at (same proxy the retry
--   handler used before 0023).
--
-- For large email_log tables (>500k rows) consider running this outside a
-- migration via a batched script to avoid long-held locks.

BEGIN;

-- Step 1: insert channel-agnostic rows into notification_log.
--
-- We use a CTE to capture the newly inserted IDs alongside the source
-- email_log primary key (event_id + recipient_email) so Step 2 can join
-- back to pick up the email-specific columns without a correlated subquery.

WITH inserted AS (
    INSERT INTO notification_log (
        event_id,
        event_type,
        channel,
        recipient_id,       -- email address used as channel-native identity
        status,
        retry_count,
        total_attempts,
        last_error,
        payload,
        event_timestamp,
        created_at,
        updated_at
    )
    SELECT
        el.event_id,
        el.event_type,
        'email'                                         AS channel,
        el.recipient_email                              AS recipient_id,
        el.status,
        el.retry_count,
        COALESCE(el.total_attempts, el.retry_count, 0)  AS total_attempts,
        el.last_error,
        COALESCE(el.payload, '{}'::jsonb)               AS payload,
        COALESCE(el.event_timestamp, el.created_at)     AS event_timestamp,
        el.created_at,
        el.updated_at
    FROM email_log el
    ON CONFLICT (event_id, channel, recipient_id) DO NOTHING
    RETURNING id, event_id, recipient_id
)
-- Step 2: insert email-specific detail rows for every row that was
-- freshly inserted in Step 1 (conflicts produce no RETURNING row, so
-- their detail rows are also skipped — correct, since they already exist).
INSERT INTO email_notification_log (
    notification_id,
    recipient_email,
    recipient_name,
    from_override,
    sender_account,
    send_mode,
    cc,
    bcc,
    attachments
)
SELECT
    ins.id                                              AS notification_id,
    el.recipient_email,
    el.recipient_name,
    el.from_override,
    el.sender_account,
    COALESCE(el.send_mode, 'individual')                AS send_mode,
    el.cc,
    el.bcc,
    el.attachments
FROM inserted ins
JOIN email_log el
    ON  el.event_id        = ins.event_id
    AND el.recipient_email = ins.recipient_id;

COMMIT;
