-- migrations/business_db/0022_send_notification_fn_send_mode.sql
--
-- Updates notify_send_email() with two additions:
--
--   1. p_sender_account — fixes a bug where the named SMTP account was never
--      forwarded to the outbox worker despite being documented as supported.
--      The worker already reads "sender_account" from the payload; this
--      migration simply writes it there.
--
--   2. p_send_mode — controls how multiple TO recipients are delivered:
--
--        'individual' (default, existing behaviour)
--            Each recipient in p_recipients is delivered as a separate email.
--            Every address gets its own email_log row, retry counter, and
--            independent success / failure state.  Recipients cannot see each
--            other's addresses.  Use for transactional mail (order receipts,
--            password resets, personalised notifications).
--
--        'group'
--            All recipients in p_recipients share a single email.  All
--            addresses appear together in the To: header, so every recipient
--            can see who else received the message.  Only one email_log row
--            is written (for the first address in the list); the delivery is
--            retried or failed as a unit.  Use for team / group notifications
--            where mutual visibility is intentional (meeting invites, shared
--            alerts, internal digests).
--
-- No schema changes are required: outbox.payload is already JSONB.
-- The consumer and SMTP / webhook backends are updated separately to act on
-- the "send_mode" key.
--
-- Existing callers are unaffected: omitting p_send_mode keeps the default
-- 'individual' behaviour.
--
-- Prerequisites: 0021_send_notification_fn_cc_bcc.sql

-- ── Main function (updated) ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION notify_send_email(
    -- Required
    p_event_type      text,

    -- TO recipient(s): supply exactly one of p_recipient or p_recipients.
    --   p_recipient  — single TO recipient: {"email":"...","name":"..."}
    --   p_recipients — array  of TO recipients
    p_recipient       jsonb    DEFAULT NULL,
    p_recipients      jsonb    DEFAULT NULL,

    -- Delivery mode for multiple recipients.
    --   'individual' (default) — each recipient gets a separate email with
    --                            its own tracking row and retry loop.
    --   'group'                — all recipients share one email; all addresses
    --                            appear in the To: header together.
    p_send_mode       text     DEFAULT 'individual',

    -- Optional CC / BCC recipient arrays.
    --   Each element: {"email":"addr@example.com","name":"Optional Name"}
    --   Omit or pass NULL for no CC / BCC.
    p_cc              jsonb    DEFAULT NULL,
    p_bcc             jsonb    DEFAULT NULL,

    -- Required: template variables forwarded to the renderer
    p_payload         jsonb    DEFAULT '{}',

    -- Optional: override the From address for this event only
    --   {"email":"orders@acme.com","name":"Acme Orders"}
    p_from_override   jsonb    DEFAULT NULL,

    -- Optional: URL-based attachment references
    p_attachments     jsonb    DEFAULT NULL,

    -- Optional: named SMTP sender account (must match a key under
    -- [sender_accounts] in the notification-service config).
    -- When NULL the service uses its global [mailer] default.
    p_sender_account  text     DEFAULT NULL,

    -- Optional: arbitrary metadata forwarded verbatim to the consumer
    --   {"source":"order-service"}
    p_metadata        jsonb    DEFAULT NULL,

    -- Optional: stable idempotency key. When NULL a random UUID is generated.
    p_event_id        uuid     DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_id    uuid;
    v_recipients  jsonb;
    v_payload_env jsonb;
    v_r           jsonb;
BEGIN
    -- ── 1. Validate event_type ─────────────────────────────────────────────
    IF p_event_type IS NULL OR p_event_type = '' THEN
        RAISE EXCEPTION 'notify_send_email: p_event_type must not be empty'
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 2. Validate send_mode ──────────────────────────────────────────────
    IF p_send_mode IS NULL OR p_send_mode NOT IN ('individual', 'group') THEN
        RAISE EXCEPTION 'notify_send_email: p_send_mode must be ''individual'' or ''group'', got: %',
            COALESCE(p_send_mode, 'NULL')
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 3. Resolve TO recipients ───────────────────────────────────────────
    IF p_recipient IS NOT NULL AND p_recipients IS NOT NULL THEN
        RAISE EXCEPTION 'notify_send_email: supply p_recipient OR p_recipients, not both'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_recipient IS NOT NULL THEN
        PERFORM _notify_validate_recipient(p_recipient);
        v_recipients := jsonb_build_array(p_recipient);

    ELSIF p_recipients IS NOT NULL THEN
        IF jsonb_typeof(p_recipients) <> 'array' THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must be a JSON array'
                USING ERRCODE = 'P0001';
        END IF;
        IF jsonb_array_length(p_recipients) = 0 THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must not be empty'
                USING ERRCODE = 'P0001';
        END IF;
        FOR v_r IN SELECT jsonb_array_elements(p_recipients) LOOP
            PERFORM _notify_validate_recipient(v_r);
        END LOOP;
        v_recipients := p_recipients;

    ELSE
        RAISE EXCEPTION 'notify_send_email: one of p_recipient or p_recipients is required'
            USING ERRCODE = 'P0001';
    END IF;

    -- group mode with a single recipient is pointless but harmless; allow it.

    -- ── 4. Validate CC / BCC lists ─────────────────────────────────────────
    PERFORM _notify_validate_recipient_list('p_cc',  p_cc);
    PERFORM _notify_validate_recipient_list('p_bcc', p_bcc);

    -- ── 5. Validate from_override ──────────────────────────────────────────
    IF p_from_override IS NOT NULL THEN
        IF jsonb_typeof(p_from_override) <> 'object' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must be a JSON object'
                USING ERRCODE = 'P0001';
        END IF;
        IF p_from_override->>'email' IS NULL OR p_from_override->>'email' = '' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must contain a non-empty "email" field'
                USING ERRCODE = 'P0001';
        END IF;
        PERFORM _notify_validate_recipient(jsonb_build_object('email', p_from_override->>'email'));
    END IF;

    -- ── 6. Validate attachments ────────────────────────────────────────────
    PERFORM _notify_validate_attachments(p_attachments);

    -- ── 7. Build payload envelope ──────────────────────────────────────────
    v_event_id := COALESCE(p_event_id, gen_random_uuid());

    -- jsonb_strip_nulls omits NULL-valued keys so the worker's .get() returns
    -- None for absent optional fields, which it already treats as the default.
    v_payload_env := jsonb_strip_nulls(jsonb_build_object(
        'recipients',     v_recipients,
        'send_mode',      p_send_mode,          -- 'individual' | 'group'
        'payload',        COALESCE(p_payload, '{}'),
        'from_override',  p_from_override,
        'attachments',    COALESCE(p_attachments, '[]'::jsonb),
        'cc',             p_cc,
        'bcc',            p_bcc,
        'sender_account', p_sender_account,      -- named SMTP account (bug fix)
        'metadata',       COALESCE(p_metadata, '{}'::jsonb)
    ));

    -- ── 8. Insert into outbox (idempotent on p_event_id) ──────────────────
    INSERT INTO outbox (event_id, event_type, payload)
    VALUES             (v_event_id, p_event_type, v_payload_env)
    ON CONFLICT (event_id) DO NOTHING;

    RETURN v_event_id;
END;
$$;

-- ── Usage examples ────────────────────────────────────────────────────────────
--
-- Individual sends (default — each address gets its own email):
--
--   SELECT notify_send_email(
--     p_event_type => 'ORDER_CONFIRMATION',
--     p_recipient  => '{"email":"alice@example.com","name":"Alice"}'::jsonb,
--     p_payload    => '{"orderId":"123","amount":"99.00"}'::jsonb
--   );
--
-- Individual sends with named SMTP account (bug fix):
--
--   SELECT notify_send_email(
--     p_event_type     => 'INVOICE_READY',
--     p_recipient      => '{"email":"alice@example.com"}'::jsonb,
--     p_payload        => '{"invoiceId":"INV-42"}'::jsonb,
--     p_sender_account => 'billing'
--   );
--
-- Group send — all recipients see each other in the To: header:
--
--   SELECT notify_send_email(
--     p_event_type => 'TEAM_ALERT',
--     p_recipients => '[
--       {"email":"alice@acme.com","name":"Alice"},
--       {"email":"bob@acme.com","name":"Bob"},
--       {"email":"carol@acme.com","name":"Carol"}
--     ]'::jsonb,
--     p_send_mode  => 'group',
--     p_payload    => '{"alertTitle":"Disk usage critical","threshold":"90%"}'::jsonb
--   );
--
-- Group send with CC / BCC and custom sender:
--
--   SELECT notify_send_email(
--     p_event_type     => 'SPRINT_REVIEW_INVITE',
--     p_recipients     => '[
--       {"email":"dev1@acme.com","name":"Dev 1"},
--       {"email":"dev2@acme.com","name":"Dev 2"}
--     ]'::jsonb,
--     p_send_mode      => 'group',
--     p_cc             => '[{"email":"manager@acme.com","name":"Manager"}]'::jsonb,
--     p_bcc            => '[{"email":"calendar@acme.com"}]'::jsonb,
--     p_payload        => '{"meetingTime":"Friday 3pm","location":"Conf Room B"}'::jsonb,
--     p_sender_account => 'engineering',
--     p_event_id       => 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid
--   );
