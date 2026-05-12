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
        other => Err(AppError::Template(format!(
            "Unknown event type '{other}' — no template registered"
        ))),
    }
}

pub use template_store::TemplateStore;
mod template_store {
    use super::*;

    /// In-memory template store.  
    /// Phase 2: replace with a DB-backed store that loads from `email_template` table.
    #[derive(Clone)]
    pub struct TemplateStore;

    impl TemplateStore {
        pub fn new() -> Self {
            Self
        }

        pub fn resolve(
            &self,
            event_type: &str,
        ) -> Result<(&'static str, &'static str, &'static str), AppError> {
            templates_for(event_type)
        }
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
}
