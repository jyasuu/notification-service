-- migrations/0020_email_log_cc_bcc.sql
--
-- Adds `cc` and `bcc` JSONB columns to `email_log` so that CC/BCC recipients
-- are preserved across manual retries, matching how `from_override` and
-- `attachments` are already stored (migrations 0009 and 0008 respectively).
--
-- Both columns are nullable for backwards compatibility with existing rows.
-- Pre-0020 rows will have NULL in both columns and will be retried without
-- CC/BCC (same behaviour as before this feature was added).
--
-- Column contract:
--
--   cc  — JSON array of recipient objects, or NULL.
--   bcc — JSON array of recipient objects, or NULL.
--
--   Each element: { "email": "addr@example.com", "name": "Optional Name" }
--
-- These are stored exactly as they arrive from the EmailEvent so they can be
-- deserialised back into Vec<Recipient> by republish_event without any
-- transformation.
--
-- Example stored value:
--   '[{"email":"manager@acme.com","name":"Alice"},{"email":"audit@acme.com"}]'
--
-- NULL and '[]' are both treated as "no CC/BCC" by the retry path.

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS cc  JSONB,
    ADD COLUMN IF NOT EXISTS bcc JSONB;