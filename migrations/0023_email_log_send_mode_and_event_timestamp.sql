-- migrations/0023_email_log_send_mode_and_event_timestamp.sql
--
-- Adds two columns to `email_log` required for correct retry reconstruction:
--
--   send_mode        TEXT    NOT NULL DEFAULT 'individual'
--
--     The delivery mode of the original event.  Needed so that manual retries
--     via the HTTP API faithfully replay the original behaviour:
--
--       'individual' — each recipient gets a separate, independently tracked
--                      email (one email_log row per address, separate retry).
--       'group'      — all recipients share one email; all To: addresses are
--                      visible to each other.  Only the primary (first) address
--                      has an email_log row; delivery is retried as a unit.
--
--     Without this column, republish_event() hard-coded Individual for every
--     retry, silently changing a group-mode event (one email, all addresses
--     visible) into N individual emails — a behavioral regression for the
--     event's recipients.
--
--     Nullable for backwards compatibility; existing rows fall back to
--     'individual' (same behaviour as before group-mode was introduced).
--
--   event_timestamp  TIMESTAMPTZ    NOT NULL DEFAULT now()
--
--     The original NotificationEvent.timestamp written by the business service
--     when it published the event.  Distinct from created_at (the DB insertion
--     time, i.e. when the consumer first processed the event).
--
--     Without this column, republish_event() used created_at as a proxy for
--     the event timestamp.  This caused attachment expiry checks to use the
--     processing time instead of the publication time, so an event retried
--     shortly after its attachments were published could incorrectly pass the
--     max_age_secs validation even though the business service intended the
--     URLs to be valid only from the publication moment.  The inverse was also
--     possible: a long DB replication lag could make a fresh URL appear expired.
--
--     Nullable for backwards compatibility; existing rows fall back to
--     created_at in republish_event() (same proxy as before).
--
-- Both columns are nullable so that rows written before this migration are
-- not affected and existing retry logic degrades gracefully.

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS send_mode       TEXT
        CHECK (send_mode IN ('individual', 'group')),
    ADD COLUMN IF NOT EXISTS event_timestamp TIMESTAMPTZ;

COMMENT ON COLUMN email_log.send_mode IS
    'Delivery mode of the original event: ''individual'' (default) or ''group''. '
    'NULL for rows written before migration 0023; treated as ''individual'' on retry.';

COMMENT ON COLUMN email_log.event_timestamp IS
    'Original NotificationEvent.timestamp from the publishing business service. '
    'Used by republish_event() for attachment expiry checks instead of created_at. '
    'NULL for rows written before migration 0023; falls back to created_at on retry.';
