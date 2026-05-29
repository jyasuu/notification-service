use anyhow::{bail, Context, Result};
use chrono::Utc;
use common::{
    AttachmentRef, ChannelOverrides, EmailOptions, FromOverride, GroupRetryMode, Metadata,
    NotificationEvent, Recipient, RetryPolicy,
};
use dialoguer::Confirm;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use uuid::Uuid;

use crate::{cli::SendArgs, config::CliConfig};

pub async fn run(args: SendArgs, cfg: CliConfig) -> Result<()> {
    // ── 1. Parse payload ──────────────────────────────────────────────────────
    let payload: serde_json::Value = if args.payload.starts_with('@') {
        let path = &args.payload[1..];
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read payload file: {path}"))?;
        serde_json::from_str(&text).with_context(|| format!("Invalid JSON in {path}"))?
    } else {
        serde_json::from_str(&args.payload)
            .context("--payload must be valid JSON or @<path> to a JSON file")?
    };

    // ── 2. Build recipients ───────────────────────────────────────────────────
    if args.to.is_empty() {
        bail!("At least one --to address is required");
    }
    let recipients: Vec<Recipient> = args
        .to
        .iter()
        .enumerate()
        .map(|(i, email)| Recipient {
            email: email.clone(),
            name: args.name.get(i).cloned(),
        })
        .collect();

    // ── 3. Validate recipient emails ──────────────────────────────────────────
    for r in &recipients {
        if !common::is_valid_email(&r.email) {
            bail!("Invalid email address: {}", r.email);
        }
    }

    // ── 3b. Build and validate CC / BCC ────────────────────────────────
    let cc: Vec<Recipient> = args
        .cc
        .iter()
        .map(|email| {
            if !common::is_valid_email(email) {
                bail!("Invalid --cc address: {email}");
            }
            Ok(Recipient {
                email: email.clone(),
                name: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let bcc: Vec<Recipient> = args
        .bcc
        .iter()
        .map(|email| {
            if !common::is_valid_email(email) {
                bail!("Invalid --bcc address: {email}");
            }
            Ok(Recipient {
                email: email.clone(),
                name: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // ── 4. Parse attachments ──────────────────────────────────────────────────
    let attachments: Vec<AttachmentRef> = args
        .attachment
        .iter()
        .map(|raw| {
            serde_json::from_str::<AttachmentRef>(raw)
                .with_context(|| format!("Invalid attachment JSON: {raw}"))
        })
        .collect::<Result<Vec<_>>>()?;

    for att in &attachments {
        att.validate(&Utc::now(), Utc::now())
            .map_err(|e| anyhow::anyhow!("Attachment validation failed: {e}"))?;
    }

    // ── 5. Build from_override ────────────────────────────────────────────────
    let from_override = match &args.from_email {
        Some(email) => {
            if !common::is_valid_email(email) {
                bail!("Invalid --from-email address: {email}");
            }
            Some(FromOverride {
                email: email.clone(),
                name: args.from_name.clone(),
            })
        }
        None => None,
    };

    // ── 6. Build event ────────────────────────────────────────────────────────

    // When --subject/--body-html/--body-text are all supplied, route through the
    // built-in GENERIC_HTML template by injecting the values into the payload.
    // This replaces the old body_override bypass — everything goes through the
    // template engine, keeping the code path uniform.
    let (event_type, payload) = match (&args.subject, &args.body_html, &args.body_text) {
        (Some(subject), Some(body_html), Some(body_text)) => (
            "GENERIC_HTML".to_string(),
            serde_json::json!({
                "subject":   subject,
                "body_html": body_html,
                "body_text": body_text,
            }),
        ),
        _ => (args.event_type.clone(), payload),
    };

    let event_id = args.event_id.unwrap_or_else(Uuid::new_v4);
    let event = NotificationEvent {
        event_id,
        timestamp: Utc::now(),
        event_type: event_type.clone(),
        payload,
        metadata: Metadata {
            source: Some(args.source.clone()),
        },
        channel_overrides: ChannelOverrides {
            email: Some(EmailOptions {
                recipients: recipients.clone(),
                cc,
                bcc,
                from_override,
                attachments,
                sender_account: None,
                send_mode: common::SendMode::Individual,
                group_retry_mode: GroupRetryMode::default(),
                retry_policy: RetryPolicy::default(),
            }),
        },
    };

    // ── 7. Confirm ────────────────────────────────────────────────────────────
    // Convenience reference for the preview block below.
    let email_opts = event
        .channel_overrides
        .email
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("event has no email channel override"))?;
    if !args.yes {
        let to_str: Vec<&str> = recipients.iter().map(|r| r.email.as_str()).collect();
        println!("About to publish:");
        println!("  Event type   : {event_type}");
        println!("  Event ID     : {event_id}");
        println!("  Recipients   : {}", to_str.join(", "));
        if !email_opts.cc.is_empty() {
            let cc_str: Vec<&str> = email_opts.cc.iter().map(|r| r.email.as_str()).collect();
            println!("  CC           : {}", cc_str.join(", "));
        }
        if !email_opts.bcc.is_empty() {
            println!(
                "  BCC          : {} address(es) [hidden]",
                email_opts.bcc.len()
            );
        }
        println!("  Attachments  : {}", email_opts.attachments.len());
        if args.subject.is_some() {
            println!("  Body mode    : generic HTML (GENERIC_HTML template)");
            println!("  Subject      : {}", args.subject.as_deref().unwrap_or(""));
        } else {
            println!("  Body mode    : template ({event_type})");
        }

        let ok = Confirm::new()
            .with_prompt("Publish this event?")
            .default(true)
            .interact()
            .context("Prompt failed")?;

        if !ok {
            println!("Aborted.");
            return Ok(());
        }
    }

    // ── 8. Publish to RabbitMQ ────────────────────────────────────────────────
    let body = serde_json::to_vec(&event).context("Failed to serialize event")?;

    let conn = Connection::connect(&cfg.amqp.url, ConnectionProperties::default())
        .await
        .context("Failed to connect to RabbitMQ")?;
    let channel = conn
        .create_channel()
        .await
        .context("Failed to create AMQP channel")?;

    // Enable publisher confirms so the second `.await` on `basic_publish`
    // actually waits for a broker Ack/Nack frame.  Without this the channel
    // runs in "fire-and-forget" mode and the confirm future completes
    // immediately regardless of whether the broker accepted the message.
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .context("Failed to enable publisher confirms")?;

    // Declare exchange (idempotent — safe if consumer already declared it).
    channel
        .exchange_declare(
            &cfg.amqp.exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("exchange_declare failed")?;

    channel
        .basic_publish(
            &cfg.amqp.exchange,
            &cfg.amqp.routing_key,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await
        .context("basic_publish failed")?
        .await
        .context("Publisher confirm failed")?;

    println!("✓ Published event {event_id}");
    println!("  Run `anctl status {event_id}` to track delivery.");
    Ok(())
}
