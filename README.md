# AnvilNotify

A production-grade Rust microservice implementing the **Transactional Outbox + Notification Service** pattern.

## Architecture

```
[Business Service]
  └── writes Outbox record (same DB transaction as business data)
        ↓
[Outbox Worker]  (external, publishes to RabbitMQ)
        ↓
[anvil-notify]  ← this repo
  ├── AMQP consumer  (lapin)     — receives EmailEvent messages
  ├── Idempotency    (sqlx/pg)   — deduplicates by event_id
  ├── Template engine            — renders subject/body
  ├── EmailSender trait          — SMTP (lettre) or Webhook (reqwest)
  ├── Retry + DLQ                — exponential backoff, DLX
  └── HTTP API       (axum)      — status queries, manual retry
```

## Workspace crates

| Crate      | Responsibility                                    |
|------------|---------------------------------------------------|
| `common`   | Shared types: `EmailEvent`, `EmailLog`, `AppError`|
| `store`    | PostgreSQL CRUD via `sqlx`                        |
| `mailer`   | `EmailSender` trait + SMTP & webhook impls        |
| `consumer` | RabbitMQ consumer loop with retry/backoff         |
| `api`      | Axum HTTP API (status, retry, health)             |

## Quick start

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Copy and edit config
cp config/default.toml config/local.toml

# 3. Run
cargo run
```

## Configuration

Config is loaded from `config/default.toml` then `config/local.toml`,
with environment variable overrides using the `AN__` prefix and `__` separator:

```bash
AN__DATABASE__URL="postgres://..."
AN__AMQP__URL="amqp://..."
AN__MAILER__BACKEND="webhook"
AN__MAILER__URL="https://hooks.example.com/email"
```

## Email backends

### SMTP
```toml
[mailer]
backend    = "smtp"
host       = "smtp.example.com"
port       = 587
username   = "user"
password   = "secret"
from_email = "no-reply@example.com"
from_name  = "My App"
```

### Webhook
```toml
[mailer]
backend    = "webhook"
url        = "https://hooks.example.com/email"
auth_token = "bearer-token"  # optional
```

## HTTP API

| Method | Path                                        | Description                                   |
|--------|---------------------------------------------|-----------------------------------------------|
| GET    | `/health`                                   | Liveness check (always 200 if process is up)  |
| GET    | `/ready`                                    | Readiness probe — verifies DB connectivity    |
| GET    | `/emails/:event_id`                         | Delivery status for all recipients in event   |
| GET    | `/emails/:event_id/recipients/:email`       | Delivery status for one recipient             |
| POST   | `/emails/:event_id/retry`                   | Reset all FAILED recipients → PENDING         |
| POST   | `/emails/:event_id/recipients/:email/retry` | Reset one FAILED recipient → PENDING          |
| DELETE | `/templates/:event_type/cache`              | Evict one template from the in-memory cache   |
| DELETE | `/templates/cache`                          | Clear the entire template cache               |

> **Kubernetes probes**: use `/ready` for `readinessProbe` and `/health` for `livenessProbe`.
> `/ready` performs a live DB ping; `/health` is a shallow process check only.

### Example event (publish to `email.requested` queue)

```json
{
  "event_id":  "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-09T10:00:00Z",
  "type":      "ORDER_CONFIRMATION",
  "recipient": { "email": "user@example.com", "name": "Alice" },
  "payload":   { "orderId": "123", "amount": 99.90, "name": "Alice" },
  "metadata":  { "source": "order-service" }
}
```

## Retry strategy

| Attempt | Delay   |
|---------|---------|
| 1       | 2 s     |
| 2       | 4 s     |
| 3       | 8 s     |
| > 3     | → DLX   |

Messages that exhaust retries are routed to `email.requested.dlq` via the
`anvil-notify.dlx` dead-letter exchange.

### Attachment URL expiry sizing

Pre-signed URLs (S3, GCS, Azure Blob) must remain valid for the full retry
window. Use this formula as a minimum:

```
expiry ≥ max_age_secs + (max_retries × max_retry_delay_secs)
```

With the defaults (`max_retries = 3`, last backoff step = 8 s):

```
expiry ≥ max_age_secs + (3 × 8) = max_age_secs + 24 s
```

In practice, add a generous buffer for queue lag and clock skew — **5 minutes
is the recommended minimum** for any pre-signed URL referenced in an event.

If a URL expires before the service can fetch it, the delivery is permanently
marked FAILED (no further retries). The retry API (`POST /emails/{id}/retry`)
will return a `400` error listing the expired filenames; the business service
must re-publish the event with fresh URLs before retrying.

## Per-recipient FAILED recovery

When a recipient exhausts all in-process retries it is marked `FAILED` in
`notification_log` and the AMQP message is ACK'd (other recipients in the same event
are unaffected). There is no automatic re-queue; recovery requires a manual
operator action via the HTTP API:

```bash
# Inspect which recipients failed
GET /emails/{event_id}

# Reset one recipient and re-enqueue the event
POST /emails/{event_id}/recipients/{email}/retry

# Reset ALL failed recipients for an event at once
POST /emails/{event_id}/retry
```

Both endpoints atomically reset the affected row(s) to `PENDING` and
re-publish the event to RabbitMQ. The consumer's idempotency guard ensures
already-`SENT` or `BLOCKED` recipients are skipped on re-delivery; only the
reset `PENDING` rows are re-processed.

For automated recovery, poll `GET /emails/{event_id}` and trigger the retry
endpoint when `summary.failed > 0`, or set up an alert on the
`emails_failed_total` Prometheus metric combined with the DLQ queue depth.

## Adding a new template

Templates are stored in the `notification_template` database table (with a `channel` column — use `'email'` for email templates). To add a new event type, insert a row — no code change or service restart required:

```sql
INSERT INTO notification_template (type, channel, subject, body_html, body_text)
VALUES (
    'INVOICE_READY',
    'email',
    'Your invoice #{{ invoiceId }} is ready',
    '<h1>Hi {{ name }},</h1><p>Invoice <strong>#{{ invoiceId }}</strong> for ${{ amount }} is ready.</p>',
    'Hi {{ name }}, invoice #{{ invoiceId }} for ${{ amount }} is ready.'
);
```

Then flush the cache so the service picks it up immediately (otherwise it will
be loaded automatically within `template_cache_ttl_secs`, default 5 minutes):

```bash
DELETE /templates/INVOICE_READY/cache
# or via CLI:
ns template flush --event-type INVOICE_READY
```

Templates use [Jinja2 (minijinja)](https://docs.rs/minijinja/latest/minijinja/)
syntax. See the [syntax quick-reference](#syntax-quick-reference) above.
The built-in templates (`ORDER_CONFIRMATION`, `PASSWORD_RESET`, `WELCOME`,
`GENERIC_TEXT`, `GENERIC_HTML`) are seeded and kept up-to-date by migrations.

## Business service database migrations

The `migrations/business_db/` folder contains SQL files that must be applied to
**your business service's database** (not the notification service DB). The
notification service's own `sqlx::migrate!()` only touches `database.url`.

See [`migrations/business_db/README.md`](migrations/business_db/README.md) for
the full table and instructions. Quick reference:

```bash
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0002_create_outbox.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0005_outbox_from_override.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0006_outbox_fail_count.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0008_outbox_attachments.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0016_outbox_locked_at.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0021_send_notification_fn_cc_bcc.sql
```

### Sending email from a Postgres function

After applying the migrations, call `notify_send_email()` from any business transaction:

```sql
-- Single TO, with CC and BCC
SELECT notify_send_email(
  p_event_type => 'ORDER_CONFIRMATION',
  p_recipient  => '{"email":"alice@example.com","name":"Alice"}',
  p_cc         => '[{"email":"manager@example.com","name":"Bob"}]',
  p_bcc        => '[{"email":"audit@example.com"}]',
  p_payload    => '{"orderId":"123","amount":"99.00"}'
);

-- Multiple TO recipients, multiple CC, BCC to compliance
SELECT notify_send_email(
  p_event_type => 'INVOICE_READY',
  p_recipients => '[
    {"email":"alice@acme.com","name":"Alice"},
    {"email":"finance@acme.com","name":"Finance"}
  ]',
  p_cc         => '[
    {"email":"manager@acme.com","name":"Carol"},
    {"email":"cfo@acme.com","name":"David"}
  ]',
  p_bcc        => '[{"email":"compliance@acme.com"}]',
  p_payload    => '{"invoiceId":"INV-42","amount":"5000.00"}',
  p_from_override => '{"email":"billing@acme.com","name":"Acme Billing"}',
  p_event_id   => 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid
);
```

**Parameter reference**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
|  |  | ✓ | Template key, e.g.  |
|  |  | one of these | Single TO object:  |
|  |  | one of these | Array of TO objects |
|  |  | — | Array of CC objects;  = no CC |
|  |  | — | Array of BCC objects;  = no BCC |
|  |  | — | Template variables, default  |
|  |  | — | Custom From:  |
|  |  | — | Array of  |
|  |  | — | Forwarded verbatim; useful for tracing |
|  |  | — | Idempotency key; auto-generated if omitted |

> **CC/BCC semantics:** CC and BCC addresses are included in every delivery for
> the event but do not get their own  rows. They bypass the recipient
> filter, the rate-limiter, and per-address retry. An invalid address in either
> list fails the entire delivery permanently.

## Known limitations

### Internationalized email addresses (IDN / EAI)

The built-in email validator (`crates/common/src/email_validation.rs`) accepts
only ASCII characters in both the local part and the domain. Addresses with
non-ASCII characters — such as `用户@例子.广告` or `üser@münchen.de` — are
rejected before delivery is attempted.

This is intentional: most SMTP relays and transactional providers do not
support [RFC 6531 (SMTPUTF8)](https://datatracker.ietf.org/doc/html/rfc6531)
and would reject such addresses at the protocol level anyway.

If your user base requires internationalized addresses, the domain portion can
be handled today by converting it to its Punycode representation
(`münchen.de` → `xn--mnchen-3ya.de`) before publishing the event. Full
SMTPUTF8 local-part support would require replacing the validator with an
[`idna`](https://crates.io/crates/idna)-aware implementation and confirming
your SMTP provider supports it.
