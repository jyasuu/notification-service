use common::AppError;
use serde_json::Value;

/// Render a template string by replacing `{{key}}` placeholders
/// with values from the JSON payload.
pub fn render_template(template: &str, payload: &Value) -> Result<String, AppError> {
    let obj = payload
        .as_object()
        .ok_or_else(|| AppError::Template("Payload must be a JSON object".into()))?;

    let mut result = template.to_string();

    for (key, val) in obj {
        let placeholder = format!("{{{{{key}}}}}");
        let replacement = match val {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }

    Ok(result)
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
}
