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
NS_DATABASE__URL="postgres://..."
NS_AMQP__URL="amqp://..."
NS_MAILER__BACKEND="webhook"
NS_MAILER__URL="https://hooks.example.com/email"
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

| Method | Path                        | Description                      |
|--------|-----------------------------|----------------------------------|
| GET    | `/health`                   | Liveness check                   |
| GET    | `/emails/:event_id`         | Query delivery status            |
| POST   | `/emails/:event_id/retry`   | Reset FAILED → PENDING for replay|

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

## Adding a new template

Edit `crates/mailer/src/template.rs` → `templates_for()` to add a new
`event_type` branch. In a production system, replace this with a DB lookup
against the `email_template` table.
