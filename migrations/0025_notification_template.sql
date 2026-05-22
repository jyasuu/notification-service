-- migrations/0025_notification_template.sql
--
-- Renames `email_template` to `notification_template` and adds a `channel`
-- column so the same event_type can have different templates per channel
-- (e.g. a verbose HTML email vs a short SMS for ORDER_CONFIRMATION).
--
-- The existing rows all become channel = 'email'.
--
-- New composite PK: (type, channel)
-- Old PK was:       (type)
--
-- TemplateStore::resolve() gains a channel parameter in Phase 2.
-- Until then, the application continues to query with channel = 'email'
-- explicitly so no queries break during the transition.

-- 1. Rename the table.
ALTER TABLE email_template RENAME TO notification_template;

-- 2. Add the channel column; backfill all existing rows to 'email'.
ALTER TABLE notification_template
    ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'email';

-- 3. Swap the primary key from (type) to (type, channel).
-- FIX: Moved IF EXISTS to the correct position
ALTER TABLE notification_template DROP CONSTRAINT IF EXISTS email_template_pkey;
ALTER TABLE notification_template ADD PRIMARY KEY (type, channel);

-- 4. Update the check constraint name to match the new table name.
-- SAFE FIX: Checks if the old constraint exists before trying to rename it.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'email_template_active_check'
    ) THEN
        ALTER TABLE notification_template
            RENAME CONSTRAINT email_template_active_check
            TO notification_template_active_check;
    END IF;
END
$$;

COMMENT ON TABLE  notification_template         IS 'Jinja2 templates for all notification channels. One row per (event_type, channel).';
COMMENT ON COLUMN notification_template.channel IS 'Delivery channel this template applies to: ''email'', ''sms'', ''push'', etc.';
COMMENT ON COLUMN notification_template.type    IS 'Event type key (e.g. ''ORDER_CONFIRMATION''). Must match NotificationEvent.event_type.';