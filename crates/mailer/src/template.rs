use common::AppError;
use serde_json::Value;

/// Escape HTML special characters to prevent XSS when payload values are
/// interpolated into the HTML body template.
///
/// Only five characters require escaping per WHATWG HTML:
/// `&`, `<`, `>`, `"`, `'`.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(c),
        }
    }
    out
}

/// Render a template string by replacing `{{key}}` placeholders
/// with values from the JSON payload.
///
/// When `html` is `true`, string values are HTML-escaped before substitution
/// to prevent XSS in rendered HTML body templates.  Plain-text templates
/// (`html = false`) receive raw values.
fn render_template_inner(template: &str, payload: &Value, html: bool) -> Result<String, AppError> {
    let obj = payload
        .as_object()
        .ok_or_else(|| AppError::Template("Payload must be a JSON object".into()))?;

    let mut result = template.to_string();

    for (key, val) in obj {
        let placeholder = format!("{{{{{key}}}}}");
        let raw = match val {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let replacement = if html { escape_html(&raw) } else { raw };
        result = result.replace(&placeholder, &replacement);
    }

    Ok(result)
}

/// Render a plain-text template. Values are substituted verbatim.
pub fn render_template(template: &str, payload: &Value) -> Result<String, AppError> {
    render_template_inner(template, payload, false)
}

/// Render an HTML template. String values are HTML-escaped before
/// substitution to prevent XSS from untrusted payload data.
pub fn render_html_template(template: &str, payload: &Value) -> Result<String, AppError> {
    render_template_inner(template, payload, true)
}

/// Resolve a (subject_template, html_template, text_template) triplet
/// from the event type.
///
/// Returns `AppError::Template` for unknown event types so the message is
/// immediately routed to DLQ without wasting retry slots.
///
/// # Built-in default templates
///
/// Two generic templates are provided for callers that want to send
/// freeform content without registering a custom template:
///
/// ## `GENERIC_TEXT`
/// Plain-text only email. Payload fields:
/// - `subject` — email subject line
/// - `body`    — plain-text body (rendered verbatim in both parts)
///
/// ## `GENERIC_HTML`
/// Rich HTML email with a plain-text fallback. Payload fields:
/// - `subject`    — email subject line
/// - `body_html`  — full HTML body (inserted inside a styled wrapper)
/// - `body_text`  — plain-text fallback for clients that don't render HTML
pub fn templates_for(
    event_type: &str,
) -> Result<(&'static str, &'static str, &'static str), AppError> {
    match event_type {
        "ORDER_CONFIRMATION" => Ok((
            "Order {{orderId}} confirmed",
            r#"<h1>Hi {{name}},</h1><p>Your order <strong>{{orderId}}</strong> of ${{amount}} has been confirmed.</p>"#,
            "Hi {{name}}, Your order {{orderId}} of ${{amount}} has been confirmed.",
        )),
        "PASSWORD_RESET" => Ok((
            "Reset your password",
            r#"<p>Click <a href="{{resetLink}}">here</a> to reset your password.</p>"#,
            "Visit this link to reset your password: {{resetLink}}",
        )),
        "WELCOME" => Ok((
            "Welcome to {{appName}}!",
            r#"<h1>Welcome, {{name}}!</h1><p>Thanks for joining {{appName}}.</p>"#,
            "Welcome, {{name}}! Thanks for joining {{appName}}.",
        )),
        // ── Generic default templates ─────────────────────────────────────────
        //
        // Use these when the calling service needs to send freeform content
        // without registering a dedicated template.  All content is supplied
        // via `payload` fields — no code change or redeploy required.
        "GENERIC_TEXT" => Ok((
            "{{subject}}",
            // Wrap in a minimal HTML shell so the message is valid HTML even
            // though the intent is plain-text.  The real content lives in
            // body_text; this HTML part is just a safety fallback.
            r#"<div style="font-family:sans-serif;white-space:pre-wrap">{{body}}</div>"#,
            "{{body}}",
        )),
        "GENERIC_HTML" => Ok((
            "{{subject}}",
            // Caller supplies the full HTML in `body_html`; we wrap it in a
            // minimal responsive shell with sane font defaults.
            r#"<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:24px;font-family:sans-serif;color:#111">{{body_html}}</body></html>"#,
            // Plain-text fallback is required; caller must supply it.
            "{{body_text}}",
        )),
        other => Err(AppError::Template(format!(
            "Unknown event type '{other}' — no template registered"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_placeholders() {
        let out = render_template("Hello {{name}}", &json!({"name": "World"})).unwrap();
        assert_eq!(out, "Hello World");
    }

    #[test]
    fn leaves_unknown_placeholders_intact() {
        let out = render_template("{{missing}}", &json!({})).unwrap();
        assert_eq!(out, "{{missing}}");
    }

    #[test]
    fn unknown_event_type_is_template_error() {
        let err = templates_for("NONEXISTENT").unwrap_err();
        assert!(matches!(err, AppError::Template(_)));
    }

    #[test]
    fn html_template_escapes_ampersand() {
        let out =
            render_html_template("<p>{{company}}</p>", &json!({"company": "Acme & Sons"})).unwrap();
        assert_eq!(out, "<p>Acme &amp; Sons</p>");
    }

    #[test]
    fn html_template_escapes_angle_brackets() {
        let out = render_html_template(
            "<p>{{name}}</p>",
            &json!({"name": "<script>alert(1)</script>"}),
        )
        .unwrap();
        assert_eq!(out, "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>");
    }

    #[test]
    fn html_template_escapes_quotes() {
        let out = render_html_template(
            "<a href=\"{{url}}\">click</a>",
            &json!({"url": "\" onclick=\"bad()"}),
        )
        .unwrap();
        assert!(out.contains("&quot;"));
    }

    #[test]
    fn plain_template_does_not_escape() {
        let out = render_template("Hello {{name}}", &json!({"name": "<World>"})).unwrap();
        assert_eq!(out, "Hello <World>");
    }

    #[test]
    fn generic_text_renders_subject_and_body() {
        let (subj_tpl, html_tpl, text_tpl) = templates_for("GENERIC_TEXT").unwrap();
        let payload = json!({ "subject": "Hello there", "body": "Line one\nLine two" });
        assert_eq!(render_template(subj_tpl, &payload).unwrap(), "Hello there");
        assert_eq!(render_template(text_tpl, &payload).unwrap(), "Line one\nLine two");
        // HTML wrapper preserves the body verbatim (pre-wrap, no escaping for plain sender)
        assert!(render_html_template(html_tpl, &payload).unwrap().contains("Line one\nLine two"));
    }

    #[test]
    fn generic_html_renders_all_three_parts() {
        let (subj_tpl, html_tpl, text_tpl) = templates_for("GENERIC_HTML").unwrap();
        let payload = json!({
            "subject":   "Your invoice is ready",
            "body_html": "<p>Please find your invoice attached.</p>",
            "body_text": "Please find your invoice attached.",
        });
        assert_eq!(render_template(subj_tpl, &payload).unwrap(), "Your invoice is ready");
        let html = render_html_template(html_tpl, &payload).unwrap();
        assert!(html.contains("<p>Please find your invoice attached.</p>"));
        assert!(html.contains("<body"));
        assert_eq!(render_template(text_tpl, &payload).unwrap(), "Please find your invoice attached.");
    }

    #[test]
    fn generic_html_escapes_body_html_xss() {
        let (_, html_tpl, _) = templates_for("GENERIC_HTML").unwrap();
        let payload = json!({
            "subject":   "Test",
            "body_html": "<script>alert(1)</script>",
            "body_text": "test",
        });
        let html = render_html_template(html_tpl, &payload).unwrap();
        assert!(!html.contains("<script>"), "raw <script> must be escaped");
        assert!(html.contains("&lt;script&gt;"));
    }
}
