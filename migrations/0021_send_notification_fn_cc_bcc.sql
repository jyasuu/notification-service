-- migrations/0021_send_notification_fn_cc_bcc.sql
--
-- Updates notify_send_email() to accept CC and BCC recipients.
--
-- No schema changes are required: outbox.payload is already JSONB and the
-- outbox worker already reads "cc" and "bcc" keys from it (added alongside
-- migration 0020_email_log_cc_bcc.sql).  This migration only replaces the
-- PL/pgSQL function body.
--
-- New parameters:
--
--   p_cc   — zero or more CC  recipients: [{\"email\":\"...\",\"name\":\"...\"}]
--   p_bcc  — zero or more BCC recipients: [{\"email\":\"...\",\"name\":\"...\"}]
--
-- CC/BCC semantics (mirrors EmailOptions in common/src/event.rs):
--
--   • CC  addresses appear in the Cc: header and are visible to all recipients.
--   • BCC addresses appear in the Bcc: header and are hidden from other recipients.
--   • Neither list creates independent email_log rows, goes through the
--     recipient filter, or is individually retried.
--   • An invalid address in either list fails the whole delivery permanently.
--
-- Prerequisite: 0020_email_log_cc_bcc.sql (business DB does not need it;
--               only the notification-service DB has email_log).

-- ── Helper: validate a recipient array (cc or bcc) ────────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_recipient_list(p_label text, p_list jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_r   jsonb;
    v_idx int := 0;
BEGIN
    IF p_list IS NULL THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_list) <> 'array' THEN
        RAISE EXCEPTION 'notify_send_email: % must be a JSON array, got: %', p_label, jsonb_typeof(p_list)
            USING ERRCODE = 'P0001';
    END IF;

    FOR v_r IN SELECT jsonb_array_elements(p_list) LOOP
        v_idx := v_idx + 1;
        IF jsonb_typeof(v_r) <> 'object' THEN
            RAISE EXCEPTION 'notify_send_email: %[%] must be a JSON object, got: %', p_label, v_idx, v_r
                USING ERRCODE = 'P0001';
        END IF;
        -- Reuse the existing single-recipient validator for the email check.
        PERFORM _notify_validate_recipient(v_r);
    END LOOP;
END;
$$;

-- ── Main function (updated) ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION notify_send_email(
    -- Required
    p_event_type    text,

    -- TO Recipient(s): supply exactly one of p_recipient or p_recipients.
    --   p_recipient  — single TO recipient: {\"email\":\"...\",\"name\":\"...\"}
    --   p_recipients — array  of TO recipients
    p_recipient     jsonb    DEFAULT NULL,
    p_recipients    jsonb    DEFAULT NULL,

    -- Optional CC / BCC recipient arrays.
    --   Each element: {\"email\":\"addr@example.com\",\"name\":\"Optional Name\"}
    --   Omit or pass NULL for no CC / BCC.
    p_cc            jsonb    DEFAULT NULL,
    p_bcc           jsonb    DEFAULT NULL,

    -- Required: template variables forwarded to the renderer
    p_payload       jsonb    DEFAULT '{}',

    -- Optional: override the From address for this event only
    --   {\"email\":\"orders@acme.com\",\"name\":\"Acme Orders\"}
    p_from_override jsonb    DEFAULT NULL,

    -- Optional: URL-based attachment references
    p_attachments   jsonb    DEFAULT NULL,

    -- Optional: arbitrary metadata forwarded verbatim to the consumer
    --   {\"source\":\"order-service\"}
    p_metadata      jsonb    DEFAULT NULL,

    -- Optional: stable idempotency key. When NULL a random UUID is generated.
    p_event_id      uuid     DEFAULT NULL
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

    -- ── 2. Resolve TO recipients ───────────────────────────────────────────
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

    -- ── 3. Validate CC / BCC lists ─────────────────────────────────────────
    PERFORM _notify_validate_recipient_list('p_cc',  p_cc);
    PERFORM _notify_validate_recipient_list('p_bcc', p_bcc);

    -- ── 4. Validate from_override ──────────────────────────────────────────
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

    -- ── 5. Validate attachments ────────────────────────────────────────────
    PERFORM _notify_validate_attachments(p_attachments);

    -- ── 6. Build payload envelope ──────────────────────────────────────────
    v_event_id := COALESCE(p_event_id, gen_random_uuid());

    -- The outbox worker reads: recipients, payload, from_override, attachments,
    -- cc, bcc, sender_account, metadata from this JSONB blob.
    -- NULL cc/bcc keys are omitted (jsonb_build_object skips NULL values),
    -- which the worker treats identically to an absent key (empty list).
    v_payload_env := jsonb_strip_nulls(jsonb_build_object(
        'recipients',    v_recipients,
        'payload',       COALESCE(p_payload, '{}'),
        'from_override', p_from_override,
        'attachments',   COALESCE(p_attachments, '[]'::jsonb),
        'cc',            p_cc,
        'bcc',           p_bcc,
        'metadata',      COALESCE(p_metadata, '{}'::jsonb)
    ));

    -- ── 7. Insert into outbox (idempotent on p_event_id) ──────────────────
    INSERT INTO outbox (event_id, event_type, payload)
    VALUES             (v_event_id, p_event_type, v_payload_env)
    ON CONFLICT (event_id) DO NOTHING;

    RETURN v_event_id;
END;
$$;

-- ── Usage examples ────────────────────────────────────────────────────────────
--
-- Order confirmation — CC the account manager, BCC the audit log:
--
--   SELECT notify_send_email(
--     p_event_type => 'ORDER_CONFIRMATION',
--     p_recipient  => '{"email":"alice@example.com","name":"Alice"}'::jsonb,
--     p_cc         => '[{"email":"manager@example.com","name":"Bob"}]'::jsonb,
--     p_bcc        => '[{"email":"audit@example.com"}]'::jsonb,
--     p_payload    => '{"orderId":"123","amount":"99.00"}'::jsonb
--   );
--
-- Invoice — multiple TO, multiple CC, BCC to compliance:
--
--   SELECT notify_send_email(
--     p_event_type => 'INVOICE_READY',
--     p_recipients => '[
--       {"email":"alice@acme.com","name":"Alice"},
--       {"email":"finance@acme.com","name":"Finance"}
--     ]'::jsonb,
--     p_cc         => '[
--       {"email":"manager@acme.com","name":"Carol"},
--       {"email":"cfo@acme.com","name":"David"}
--     ]'::jsonb,
--     p_bcc        => '[{"email":"compliance@acme.com"}]'::jsonb,
--     p_payload    => '{"invoiceId":"INV-42","amount":"5000.00"}'::jsonb,
--     p_from_override => '{"email":"billing@acme.com","name":"Acme Billing"}'::jsonb,
--     p_event_id   => 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid
--   );
--
-- Simple notification with no CC/BCC (existing callers unchanged):
--
--   SELECT notify_send_email(
--     p_event_type => 'PASSWORD_RESET',
--     p_recipient  => '{"email":"alice@example.com"}'::jsonb,
--     p_payload    => '{"resetLink":"https://app.example.com/reset?token=abc"}'::jsonb
--   );
