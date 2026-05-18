-- migrations/0019_upsert_builtin_templates.sql
--
-- Canonical upsert for all built-in email templates using Jinja2 (minijinja)
-- syntax.  This supersedes the piecemeal approach of migrations 0010, 0017,
-- and 0018, which left the initial INSERT seeds in 0010 using the old
-- {{variable}} hand-rolled syntax and required 0018 to patch them after the
-- fact.
--
-- ON CONFLICT DO UPDATE guarantees idempotency: re-running migrations from
-- scratch (e.g. on a fresh DB) produces the same final state as upgrading an
-- existing DB, without relying on migration ordering to "fix up" earlier seeds.
--
-- version is bumped for any row that actually changes content so audit logs
-- remain meaningful.  Rows that are already at these exact values are
-- left untouched by the CASE expressions.
--
-- Template syntax quick-reference:
--   {{ variable }}           — HTML-escaped in body_html, verbatim elsewhere
--   {{ variable | safe }}    — insert trusted HTML verbatim (body_html only)
--   {% if condition %}...{% endif %}
--   {% for item in list %}...{% endfor %}
--
-- To add a new event type: INSERT a row here and add a corresponding migration.
-- No code change or service restart is required; the in-memory cache picks up
-- new rows within template_cache_ttl_secs (default 300 s).
-- Use DELETE /templates/<event_type>/cache for immediate effect.

INSERT INTO email_template (type, subject, body_html, body_text, version, active)
VALUES

-- ORDER_CONFIRMATION ─────────────────────────────────────────────────────────
(
    'ORDER_CONFIRMATION',
    'Order {{ orderId }} confirmed',
    '<h1>Hi {{ name }},</h1><p>Your order <strong>{{ orderId }}</strong> of ${{ amount }} has been confirmed.</p>',
    'Hi {{ name }}, Your order {{ orderId }} of ${{ amount }} has been confirmed.',
    1,
    TRUE
),

-- PASSWORD_RESET ─────────────────────────────────────────────────────────────
(
    'PASSWORD_RESET',
    'Reset your password',
    '<p>Click <a href="{{ resetLink }}">here</a> to reset your password.</p>',
    'Visit this link to reset your password: {{ resetLink }}',
    1,
    TRUE
),

-- WELCOME ────────────────────────────────────────────────────────────────────
(
    'WELCOME',
    'Welcome to {{ appName }}!',
    '<h1>Welcome, {{ name }}!</h1><p>Thanks for joining {{ appName }}.</p>',
    'Welcome, {{ name }}! Thanks for joining {{ appName }}.',
    1,
    TRUE
),

-- GENERIC_TEXT ───────────────────────────────────────────────────────────────
-- Plain-text email driven entirely by payload fields.
-- Required payload: { "subject": "...", "body": "..." }
(
    'GENERIC_TEXT',
    '{{ subject }}',
    '<div style="font-family:sans-serif;white-space:pre-wrap">{{ body }}</div>',
    '{{ body }}',
    1,
    TRUE
),

-- GENERIC_HTML ───────────────────────────────────────────────────────────────
-- Rich HTML email with a plain-text fallback.  The caller supplies pre-rendered
-- HTML; `| safe` bypasses auto-escaping — the caller is responsible for safety.
-- Required payload: { "subject": "...", "body_html": "...", "body_text": "..." }
(
    'GENERIC_HTML',
    '{{ subject }}',
    '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:24px;font-family:sans-serif;color:#111">{{ body_html | safe }}</body></html>',
    '{{ body_text }}',
    1,
    TRUE
)

ON CONFLICT (type) DO UPDATE
    SET
        subject    = EXCLUDED.subject,
        body_html  = EXCLUDED.body_html,
        body_text  = EXCLUDED.body_text,
        -- Only bump version and updated_at when content actually changed,
        -- so no-op re-runs don't create spurious audit noise.
        version    = CASE
                         WHEN email_template.subject   IS DISTINCT FROM EXCLUDED.subject
                           OR email_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                           OR email_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                         THEN email_template.version + 1
                         ELSE email_template.version
                     END,
        updated_at = CASE
                         WHEN email_template.subject   IS DISTINCT FROM EXCLUDED.subject
                           OR email_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                           OR email_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                         THEN now()
                         ELSE email_template.updated_at
                     END
    -- Never deactivate a template that an operator has deliberately disabled.
    WHERE email_template.active = TRUE;
