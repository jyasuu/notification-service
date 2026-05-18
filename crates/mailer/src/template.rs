//! Template rendering via [minijinja].
//!
//! Templates are Jinja2-compatible strings stored in the `email_template`
//! table.  The engine is configured once per render call (stateless — no
//! shared `Environment` singleton needed at these call volumes).
//!
//! # Escaping contract
//!
//! | Function               | Auto-escape | Use for                        |
//! |------------------------|-------------|--------------------------------|
//! | [`render_html_template`] | **on** (HTML) | `body_html` column           |
//! | [`render_template`]      | **off**     | `subject` and `body_text`      |
//!
//! Within an HTML template, every `{{ variable }}` is HTML-escaped by
//! default.  To insert a pre-rendered HTML block verbatim — e.g. the
//! `body_html` field of `GENERIC_HTML` — use the `| safe` filter:
//!
//! ```jinja
//! {{ body_html | safe }}
//! ```
//!
//! The `| safe` filter is the **only** way to bypass auto-escaping.  It must
//! only be used for values that are already safe HTML (operator-owned content,
//! not end-user data).
//!
//! # Syntax quick-reference
//!
//! ```jinja
//! {# comment — stripped from output #}
//!
//! {# variable interpolation — HTML-escaped in body_html, verbatim elsewhere #}
//! {{ orderId }}
//! {{ name | upper }}
//! {{ amount | round(2) }}
//!
//! {# conditional #}
//! {% if isPremium %}
//!   <p>Premium member</p>
//! {% endif %}
//!
//! {# loop over an array payload field #}
//! {% for item in items %}
//!   <li>{{ item.name }} — ${{ item.price }}</li>
//! {% endfor %}
//!
//! {# dot-path access into nested objects #}
//! {{ order.shipping.address }}
//!
//! {# verbatim HTML block — only for operator-trusted content #}
//! {{ body_html | safe }}
//! ```
//!
//! For the full filter/test catalogue see the
//! [minijinja docs](https://docs.rs/minijinja/latest/minijinja/).

use common::AppError;
use minijinja::{Environment, ErrorKind};
use serde_json::Value;

// ── Environment builders ──────────────────────────────────────────────────────

/// Build a minijinja `Environment` for **HTML** templates.
///
/// Auto-escape is enabled for all templates so every `{{ variable }}`
/// is HTML-escaped.  The `| safe` filter (built-in) is the explicit
/// opt-out for trusted HTML blocks.
fn html_env() -> Environment<'static> {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_name| minijinja::AutoEscape::Html);
    // Strict: referencing a variable absent from the payload is a hard error
    // rather than silently rendering an empty string.  A missing variable
    // means the payload contract is wrong — better to fail permanently (DLQ)
    // than deliver an email with a blank field.
    env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    env
}

/// Build a minijinja `Environment` for **plain-text** templates (subject,
/// body_text).
///
/// Auto-escape is disabled: values are inserted verbatim.  This is correct
/// for plain-text email parts where HTML entities must not appear.
fn text_env() -> Environment<'static> {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_name| minijinja::AutoEscape::None);
    env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    env
}

// ── Public rendering API ──────────────────────────────────────────────────────

/// Render a **plain-text** template (subject line or `body_text`).
///
/// Values are inserted verbatim; no HTML escaping is applied.
/// Jinja2 syntax (conditionals, loops, filters) is fully supported.
///
/// # Errors
/// Returns [`AppError::Template`] when the template fails to parse or render.
/// This is a permanent error — the consumer routes it to DLQ without retrying.
pub fn render_template(template: &str, payload: &Value) -> Result<String, AppError> {
    render_with_env(text_env(), template, payload)
}

/// Render an **HTML** template (`body_html`).
///
/// Every `{{ variable }}` is HTML-escaped automatically.  Use `{{ x | safe }}`
/// to insert a pre-rendered HTML block verbatim — only for operator-owned
/// content, never for raw end-user data.
///
/// # Errors
/// Returns [`AppError::Template`] when the template fails to parse or render.
/// This is a permanent error — the consumer routes it to DLQ without retrying.
pub fn render_html_template(template: &str, payload: &Value) -> Result<String, AppError> {
    render_with_env(html_env(), template, payload)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn render_with_env(
    mut env: Environment<'static>,
    template: &str,
    payload: &Value,
) -> Result<String, AppError> {
    // add_template_owned takes a String, so the environment holds no borrow
    // into the caller's `template` slice — no lifetime coupling needed.
    env.add_template_owned("t", template.to_owned())
        .map_err(|e| template_err("parse", e))?;

    let tmpl = env
        .get_template("t")
        // Safety: we just added it above; this cannot fail in practice.
        .map_err(|e| template_err("load", e))?;

    tmpl.render(payload).map_err(|e| template_err("render", e))
}

fn template_err(phase: &str, err: minijinja::Error) -> AppError {
    // Surface the template source location when available so operators
    // can fix DB template rows without guesswork.
    let detail = match err.kind() {
        ErrorKind::UndefinedError => {
            format!("undefined variable during {phase}: {err}")
        }
        _ => format!("template {phase} error: {err}"),
    };
    AppError::Template(detail)
}

// ── Built-in template strings (used in tests only) ────────────────────────────
//
// The canonical, authoritative versions of these templates live in the
// `email_template` database table, seeded and kept up-to-date by migrations
// 0010, 0017, and 0018.  The constants below are used exclusively by unit
// tests that exercise the rendering functions without a database.
//
// Do NOT use these constants at runtime.  All runtime template resolution goes
// through `TemplateStore::resolve()` (crates/store/src/template_store.rs),
// which queries the DB with a TTL cache and returns `AppError::Template` for
// unknown event types so the consumer can immediately route to DLQ.
//
// To add a new event type: INSERT a row into `email_template`.  No code change
// or redeploy is required; the cache picks it up within `template_cache_ttl_secs`.

#[cfg(test)]
mod builtin {
    // ORDER_CONFIRMATION
    pub const ORDER_CONFIRMATION_SUBJECT: &str = "Order {{ orderId }} confirmed";
    pub const ORDER_CONFIRMATION_HTML: &str = "<h1>Hi {{ name }},</h1>\
        <p>Your order <strong>{{ orderId }}</strong> of ${{ amount }} has been confirmed.</p>";
    pub const ORDER_CONFIRMATION_TEXT: &str =
        "Hi {{ name }}, Your order {{ orderId }} of ${{ amount }} has been confirmed.";

    // GENERIC_TEXT
    pub const GENERIC_TEXT_SUBJECT: &str = "{{ subject }}";
    pub const GENERIC_TEXT_HTML: &str =
        r#"<div style="font-family:sans-serif;white-space:pre-wrap">{{ body }}</div>"#;
    pub const GENERIC_TEXT_BODY: &str = "{{ body }}";

    // GENERIC_HTML
    pub const GENERIC_HTML_SUBJECT: &str = "{{ subject }}";
    pub const GENERIC_HTML_HTML: &str = concat!(
        "<!DOCTYPE html><html>",
        r#"<head><meta charset="utf-8">"#,
        r#"<meta name="viewport" content="width=device-width,initial-scale=1"></head>"#,
        r#"<body style="margin:0;padding:24px;font-family:sans-serif;color:#111">"#,
        "{{ body_html | safe }}",
        "</body></html>"
    );
    pub const GENERIC_HTML_TEXT: &str = "{{ body_text }}";
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── render_template (plain-text) ──────────────────────────────────────────

    #[test]
    fn plain_renders_variable() {
        let out = render_template("Hello {{ name }}", &json!({"name": "World"})).unwrap();
        assert_eq!(out, "Hello World");
    }

    #[test]
    fn plain_does_not_escape_html() {
        let out = render_template("Hello {{ name }}", &json!({"name": "<World>"})).unwrap();
        assert_eq!(out, "Hello <World>");
    }

    #[test]
    fn plain_undefined_variable_is_error() {
        let err = render_template("{{ missing }}", &json!({})).unwrap_err();
        assert!(matches!(err, AppError::Template(_)));
    }

    #[test]
    fn plain_dot_path_access() {
        let out = render_template("{{ order.id }}", &json!({"order": {"id": "X-999"}})).unwrap();
        assert_eq!(out, "X-999");
    }

    #[test]
    fn plain_loop_over_array() {
        let out = render_template(
            "{% for x in items %}{{ x }}{% if not loop.last %},{% endif %}{% endfor %}",
            &json!({"items": ["a", "b", "c"]}),
        )
        .unwrap();
        assert_eq!(out, "a,b,c");
    }

    #[test]
    fn plain_conditional() {
        let out = render_template(
            "{% if premium %}VIP{% else %}Standard{% endif %}",
            &json!({"premium": true}),
        )
        .unwrap();
        assert_eq!(out, "VIP");
    }

    // ── render_html_template ──────────────────────────────────────────────────

    #[test]
    fn html_escapes_ampersand() {
        let out = render_html_template("<p>{{ company }}</p>", &json!({"company": "Acme & Sons"}))
            .unwrap();
        assert_eq!(out, "<p>Acme &amp; Sons</p>");
    }

    #[test]
    fn html_escapes_angle_brackets() {
        let out = render_html_template(
            "<p>{{ name }}</p>",
            &json!({"name": "<script>alert(1)</script>"}),
        )
        .unwrap();
        // minijinja's HTML escaper also encodes `/` as `&#x2f;` (defence-in-depth
        // against </script> injection in JS contexts).  We assert the tags are
        // neutralised rather than pinning the exact entity spellings.
        assert!(
            out.contains("&lt;script&gt;"),
            "opening tag must be escaped"
        );
        assert!(!out.contains("<script>"), "raw opening tag must not appear");
        assert!(
            !out.contains("</script>"),
            "raw closing tag must not appear"
        );
    }

    #[test]
    fn html_escapes_double_quotes() {
        let out = render_html_template(
            r#"<a href="{{ url }}">click</a>"#,
            &json!({"url": r#"" onclick="bad()"#}),
        )
        .unwrap();
        // minijinja encodes `"` as `&#34;`, neutralising the attribute-break
        // injection attempt.  We assert the quote is encoded (by any entity
        // spelling) and that the injected attribute name cannot appear as a
        // bare word in the output.
        assert!(
            out.contains("&#34;") || out.contains("&quot;"),
            "double-quote must be encoded: {out}"
        );
        assert!(
            !out.contains(r#"" onclick"#),
            "attribute-break injection must be neutralised: {out}"
        );
    }

    #[test]
    fn html_safe_filter_passes_html_verbatim() {
        let out = render_html_template(
            "<body>{{ body_html | safe }}</body>",
            &json!({"body_html": "<p>Hello</p>"}),
        )
        .unwrap();
        assert_eq!(out, "<body><p>Hello</p></body>");
    }

    #[test]
    fn html_without_safe_escapes_tags() {
        let out = render_html_template(
            "<body>{{ body_html }}</body>",
            &json!({"body_html": "<p>Hello</p>"}),
        )
        .unwrap();
        // Tags must be escaped; we don't pin the exact entity for `/`.
        assert!(
            out.contains("&lt;p&gt;Hello"),
            "opening tag must be escaped"
        );
        assert!(!out.contains("<p>"), "raw tag must not appear");
        assert!(!out.contains("</p>"), "raw closing tag must not appear");
    }

    #[test]
    fn html_filter_upper() {
        let out = render_html_template("{{ name | upper }}", &json!({"name": "alice"})).unwrap();
        assert_eq!(out, "ALICE");
    }

    #[test]
    fn html_undefined_variable_is_error() {
        let err = render_html_template("{{ missing }}", &json!({})).unwrap_err();
        assert!(matches!(err, AppError::Template(_)));
    }

    #[test]
    fn html_loop_and_conditional() {
        let tpl = concat!(
            "{%- for item in items -%}",
            "<li>{{ item.name }}{% if item.sale %} — SALE{% endif %}</li>",
            "{%- endfor -%}",
        );
        let out = render_html_template(
            tpl,
            &json!({"items": [
                {"name": "Widget", "sale": false},
                {"name": "Gadget", "sale": true},
            ]}),
        )
        .unwrap();
        assert!(out.contains("<li>Widget</li>"));
        assert!(out.contains("<li>Gadget — SALE</li>"));
    }

    // ── Built-in template string rendering ───────────────────────────────────
    //
    // These tests verify the rendering functions against the same template
    // strings that are seeded into the `email_template` table by migrations.
    // They do not test DB access — that is covered by integration tests in
    // crates/store.

    #[test]
    fn order_confirmation_renders() {
        use super::builtin::*;
        let payload = json!({"name": "Alice", "orderId": "ORD-1", "amount": "42.00"});
        assert_eq!(
            render_template(ORDER_CONFIRMATION_SUBJECT, &payload).unwrap(),
            "Order ORD-1 confirmed"
        );
        assert!(render_html_template(ORDER_CONFIRMATION_HTML, &payload)
            .unwrap()
            .contains("Alice"));
        assert!(render_template(ORDER_CONFIRMATION_TEXT, &payload)
            .unwrap()
            .contains("ORD-1"));
    }

    #[test]
    fn order_confirmation_escapes_xss_in_name() {
        use super::builtin::ORDER_CONFIRMATION_HTML;
        let payload = json!({"name": "<script>alert(1)</script>", "orderId": "X", "amount": "0"});
        let out = render_html_template(ORDER_CONFIRMATION_HTML, &payload).unwrap();
        assert!(!out.contains("<script>"));
        assert!(out.contains("&lt;script&gt;"));
    }

    #[test]
    fn generic_text_renders() {
        use super::builtin::*;
        let payload = json!({"subject": "Hello", "body": "Line one\nLine two"});
        assert_eq!(
            render_template(GENERIC_TEXT_SUBJECT, &payload).unwrap(),
            "Hello"
        );
        assert_eq!(
            render_template(GENERIC_TEXT_BODY, &payload).unwrap(),
            "Line one\nLine two"
        );
        assert!(render_html_template(GENERIC_TEXT_HTML, &payload)
            .unwrap()
            .contains("Line one\nLine two"));
    }

    #[test]
    fn generic_html_passes_body_html_verbatim() {
        use super::builtin::*;
        let payload = json!({
            "subject":   "Your invoice",
            "body_html": "<p>Please find your invoice attached.</p>",
            "body_text": "Please find your invoice attached.",
        });
        assert_eq!(
            render_template(GENERIC_HTML_SUBJECT, &payload).unwrap(),
            "Your invoice"
        );
        let rendered = render_html_template(GENERIC_HTML_HTML, &payload).unwrap();
        assert!(
            rendered.contains("<p>Please find your invoice attached.</p>"),
            "body_html must arrive verbatim via | safe, not escaped: {rendered}"
        );
        assert!(rendered.contains("<body"));
        assert_eq!(
            render_template(GENERIC_HTML_TEXT, &payload).unwrap(),
            "Please find your invoice attached."
        );
    }

    #[test]
    fn generic_html_subject_is_plain_text_no_escaping() {
        use super::builtin::GENERIC_HTML_SUBJECT;
        let payload = json!({"subject": "Hello & Goodbye", "body_html": "", "body_text": ""});
        assert_eq!(
            render_template(GENERIC_HTML_SUBJECT, &payload).unwrap(),
            "Hello & Goodbye"
        );
    }
}
