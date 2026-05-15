# notification-service

A production-grade Rust microservice implementing the **Transactional Outbox + Notification Service** pattern.

## Architecture

```
[Business Service]
  └── writes Outbox record (same DB transaction as business data)
        ↓
[Outbox Worker]  (external, publishes to RabbitMQ)
        ↓
[notification-service]  ← this repo
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
with environment variable overrides using the `NS__` prefix and `__` separator:

```bash
NS__DATABASE__URL="postgres://..."
NS__AMQP__URL="amqp://..."
NS__MAILER__BACKEND="webhook"
NS__MAILER__URL="https://hooks.example.com/email"
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
`notifications.dlx` dead-letter exchange.

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
`email_log` and the AMQP message is ACK'd (other recipients in the same event
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

Edit `crates/mailer/src/template.rs` → `templates_for()` to add a new
`event_type` branch. In a production system, replace this with a DB lookup
against the `email_template` table.

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
```
