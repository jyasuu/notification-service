-- migrations/0017_add_generic_templates.sql
--
-- Adds two built-in generic templates for callers that want to send freeform
-- content without registering a dedicated event type.
--
-- GENERIC_TEXT  — plain-text email. Payload fields:
--                   subject  — email subject line
--                   body     — plain-text body (shown verbatim)
--
-- GENERIC_HTML  — rich HTML email with plain-text fallback. Payload fields:
--                   subject    — email subject line
--                   body_html  — full HTML body (inserted into a styled shell)
--                   body_text  — plain-text fallback for non-HTML clients

INSERT INTO email_template (type, subject, body_html, body_text) VALUES
(
    'GENERIC_TEXT',
    '{{subject}}',
    '<div style="font-family:sans-serif;white-space:pre-wrap">{{body}}</div>',
    '{{body}}'
),
(
    'GENERIC_HTML',
    '{{subject}}',
    '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:24px;font-family:sans-serif;color:#111">{{body_html}}</body></html>',
    '{{body_text}}'
)
ON CONFLICT (type) DO NOTHING;
