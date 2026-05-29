# AnvilNotify

> A production-grade Rust microservice that reliably delivers transactional email — built on the **Transactional Outbox pattern**, RabbitMQ, and PostgreSQL.

AnvilNotify sits between your business services and your mail provider. Business services write to an outbox table in the same database transaction as their business data; AnvilNotify picks it up, renders templates, handles retries with exponential backoff, and delivers via SMTP or webhook. No lost emails on crashes, no double-sends on retries.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/github/actions/workflow/status/jyasuu/anvil-notify/docker-publish.yml?branch=main)](https://github.com/jyasuu/anvil-notify/actions)

---

## ✨ Features

- 📬 **Guaranteed delivery via Transactional Outbox** — email is enqueued atomically with your business data, so a crash between "write" and "send" is never a lost message.
- 🔁 **Exponential backoff retry with Dead Letter Queue** — transient failures retry automatically; exhausted messages land in a DLQ for operator inspection rather than silently disappearing.
- 🧩 **Idempotent processing** — every delivery is deduplicated by `(event_id, recipient)` so AMQP redeliveries never cause duplicate emails.
- 🎨 **DB-managed Jinja2 templates** — add or edit templates with a single SQL `INSERT`; no code changes or restarts required.
- 📎 **Attachment support** — fetch from pre-signed URLs (S3, GCS, Azure Blob) with configurable size limits and expiry validation.
- 🏢 **Multi-tenant sender accounts** — route events from different business systems through their own SMTP credentials and `From:` addresses.
- ⚡ **Dual mail backends** — SMTP (lettre connection pool) or a webhook endpoint; switch with a one-line config change.
- 🛡️ **Recipient block/allow-list** — static config-file lists and a runtime DB-backed list manageable via the HTTP API, with in-memory caching.
- 📊 **Prometheus metrics + Grafana** — all key counters and histograms are instrumented out of the box.
- 🐳 **Docker Compose and Kubernetes manifests** — everything you need to run locally or deploy to a cluster is included.

---

## 🏗️ Architecture

```
[Business Service]
  └── INSERT INTO outbox  (same DB transaction as your business data)
            │
            ▼
  [Outbox Worker]  ── polls outbox table ──►  RabbitMQ
                                                  │
                                                  ▼
                                          [anvil-notify consumer]
                                            ├── Idempotency guard    (PostgreSQL)
                                            ├── Template renderer    (minijinja)
                                            ├── Recipient filter     (block/allow-list)
                                            ├── Rate limiter         (token bucket)
                                            ├── EmailSender          (SMTP or Webhook)
                                            └── Retry / DLQ          (exponential backoff)

                                          [anvil-notify HTTP API]
                                            ├── GET  /emails/:id          (delivery status)
                                            ├── POST /emails/:id/retry    (manual recovery)
                                            ├── CRUD /templates/cache     (cache management)
                                            └── CRUD /admin/blocklist     (runtime filtering)
```

### Workspace crates

| Crate              | Responsibility                                                            |
| ------------------ | ------------------------------------------------------------------------- |
| `common`           | Shared types: `NotificationEvent`, `AppError`, email validation           |
| `store`            | PostgreSQL CRUD via `sqlx` — notification log, templates, blocklist       |
| `mailer`           | `EmailSender` trait + SMTP (lettre) and webhook (reqwest) implementations |
| `consumer`         | RabbitMQ consumer loop, retry/backoff logic, delivery orchestration       |
| `api`              | Axum HTTP API — status queries, manual retry, cache management, blocklist |
| `outbox`           | Outbox worker — polls business DB and publishes events to RabbitMQ        |
| `rate_limiter`     | Token-bucket rate limiter for outbound mail throughput                    |
| `recipient_filter` | Block/allow-list filtering (config-file and DB-backed)                    |
| `anctl`            | Operator CLI: `send`, `retry`, `status`, `logs`, `template`, `blocklist`  |

---

## 🚀 Getting Started

### Prerequisites

| Tool           | Version   | Notes                                   |
| -------------- | --------- | --------------------------------------- |
| Rust           | `>= 1.94` | Install via [rustup](https://rustup.rs) |
| Docker         | `>= 24.0` | Required for local infrastructure       |
| Docker Compose | `>= 2.0`  | Bundled with Docker Desktop             |
| `sqlx-cli`     | latest    | Only needed to create new migrations    |

### Installation

**1. Clone the repository**

```bash
git clone https://github.com/jyasuu/anvil-notify.git
cd anvil-notify
```

**2. Start infrastructure** (PostgreSQL, RabbitMQ, Mailpit)

```bash
docker compose up -d
```

This starts:

- **PostgreSQL** on `localhost:5432` — notification service DB
- **PostgreSQL** on `localhost:5433` — business service DB (outbox)
- **RabbitMQ** on `localhost:5672` (management UI: `localhost:15672`)
- **Mailpit** SMTP catch-all on `localhost:1025` (inbox UI: `localhost:8025`)

**3. Configure the service**

```bash
cp config/default.toml config/local.toml
# Edit config/local.toml — the defaults work out of the box with Docker Compose
```

**4. Copy environment variables**

```bash
cp .env.example .env
# The defaults in .env.example match the Docker Compose services exactly
```

**5. Build and run**

```bash
cargo run
```

The API will be available at **`http://localhost:8080`**. Open **`http://localhost:8025`** in your browser to see delivered emails in Mailpit.

### Verify it's working

```bash
# Health check
curl http://localhost:8080/health

# Readiness check (verifies DB connectivity)
curl http://localhost:8080/ready
```

---

## 📨 Sending Your First Email

Publish an event to the `email.requested` RabbitMQ queue:

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-09T10:00:00Z",
  "event_type": "ORDER_CONFIRMATION",
  "payload": { "orderId": "123", "amount": "99.90", "name": "Alice" },
  "channel_overrides": {
    "email": {
      "recipients": [{ "email": "alice@example.com", "name": "Alice" }]
    }
  }
}
```

Or use the CLI to send a test event directly:

```bash
cargo run --bin anctl -- send \
  --event-type ORDER_CONFIRMATION \
  --to alice@example.com \
  --payload '{"orderId":"123","amount":"99.90","name":"Alice"}'
```

Check delivery status:

```bash
cargo run --bin anctl -- status --event-id 550e8400-e29b-41d4-a716-446655440000
```

### Using the Transactional Outbox from your business service

Apply the business DB migrations once:

```bash
# See migrations/business_db/README.md for full instructions
psql -h localhost -p 5433 -U postgres business \
  -f migrations/business_db/0001_initial_schema.sql \
  -f migrations/business_db/0002_send_notification_fn.sql
```

Then call `notify_send_email()` inside any transaction:

```sql
-- Atomically enqueue a notification alongside your business data
BEGIN;
  UPDATE orders SET status = 'confirmed' WHERE id = '123';

  SELECT notify_send_email(
    p_event_type => 'ORDER_CONFIRMATION',
    p_recipient  => '{"email":"alice@example.com","name":"Alice"}',
    p_payload    => '{"orderId":"123","amount":"99.90","name":"Alice"}'
  );
COMMIT;
-- If either statement fails, both are rolled back — the notification is never lost
```

---

## ⚙️ Configuration

Configuration is layered: `config/default.toml` → `config/local.toml` → environment variables. Environment variable overrides use the `AN__` prefix and `__` as a separator:

```bash
AN__DATABASE__URL="postgres://user:pass@host:5432/db"
AN__MAILER__PASSWORD="smtp-secret"
AN__HTTP__API_KEY="your-bearer-token"
```

### Key settings

| Setting                        | Env var override                    | Default                             | Description                                                                               |
| ------------------------------ | ----------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------- |
| `database.url`                 | `AN__DATABASE__URL`                 | `postgres://…localhost…`            | PostgreSQL connection string for the notification DB                                      |
| `amqp.url`                     | `AN__AMQP__URL`                     | `amqp://guest:guest@localhost:5672` | RabbitMQ connection string                                                                |
| `amqp.max_retries`             | `AN__AMQP__MAX_RETRIES`             | `3`                                 | Max delivery attempts before routing to DLQ                                               |
| `amqp.max_concurrency`         | `AN__AMQP__MAX_CONCURRENCY`         | `10`                                | Max parallel message handlers                                                             |
| `http.port`                    | `AN__HTTP__PORT`                    | `8080`                              | API server port                                                                           |
| `http.api_key`                 | `AN__HTTP__API_KEY`                 | _(none)_                            | **Required in production.** Bearer token for all `/emails/*` and `/templates/*` endpoints |
| `metrics_port`                 | `AN__METRICS_PORT`                  | `9091`                              | Prometheus `/metrics` port — keep this internal                                           |
| `rate_limit.emails_per_second` | `AN__RATE_LIMIT__EMAILS_PER_SECOND` | `10`                                | Steady-state outbound send rate. Set to `0` to disable                                    |
| `rate_limit.burst_size`        | `AN__RATE_LIMIT__BURST_SIZE`        | `20`                                | Token-bucket burst capacity                                                               |
| `template_cache_ttl_secs`      | `AN__TEMPLATE_CACHE_TTL_SECS`       | `300`                               | Template in-memory cache lifetime                                                         |
| `block_list_cache_ttl_secs`    | `AN__BLOCK_LIST_CACHE_TTL_SECS`     | `30`                                | Blocklist cache lifetime                                                                  |
| `max_attachment_bytes`         | `AN__MAX_ATTACHMENT_BYTES`          | `10485760` (10 MiB)                 | Hard cap per attachment; excess is permanently rejected                                   |
| `shutdown_timeout_secs`        | `AN__SHUTDOWN_TIMEOUT_SECS`         | `30`                                | Graceful shutdown window before forced exit                                               |

### Email backends

**SMTP** (default)

```toml
# config/local.toml
[mailer]
backend    = "smtp"
host       = "smtp.sendgrid.net"
port       = 587
username   = "apikey"
password   = "SG.your-key-here"
from_email = "no-reply@example.com"
from_name  = "My App"
```

TLS mode is inferred automatically from the port (`465` → implicit TLS, `587`/`25` → STARTTLS, other → none). Override with `tls_mode = "smtps" | "starttls" | "none"`.

**Webhook**

```toml
[mailer]
backend    = "webhook"
url        = "https://hooks.example.com/email"
auth_token = "bearer-token"   # optional
```

### Multi-tenant sender accounts

Route events from different systems through their own SMTP credentials:

```toml
[sender_accounts.billing]
host       = "smtp.sendgrid.net"
port       = 587
username   = "apikey"
password   = "SG.billing-key"
from_email = "billing@example.com"
from_name  = "Example Billing"

[sender_accounts.support]
host       = "smtp.gmail.com"
port       = 587
username   = "support@example.com"
password   = "app-password"
from_email = "support@example.com"
from_name  = "Example Support"
```

The publisher selects an account by setting `"sender_account": "billing"` in the event. Falls back to the global `[mailer]` config when absent or unknown.

> **Security:** Never commit `config/local.toml` or `.env` if they contain real credentials. Both are in `.gitignore`. Use environment variable overrides (`AN__MAILER__PASSWORD`) in CI and production deployments.

---

## 🌐 HTTP API

All endpoints except `/health` and `/ready` require `Authorization: Bearer <api_key>` when `http.api_key` is configured.

| Method   | Path                                        | Description                                              |
| -------- | ------------------------------------------- | -------------------------------------------------------- |
| `GET`    | `/health`                                   | Liveness check — always `200` if the process is up       |
| `GET`    | `/ready`                                    | Readiness probe — performs a live DB ping                |
| `GET`    | `/emails/:event_id`                         | Delivery status for all recipients in an event           |
| `GET`    | `/emails/:event_id/recipients/:email`       | Delivery status for one recipient                        |
| `POST`   | `/emails/:event_id/retry`                   | Reset all `FAILED` recipients → `PENDING` and re-enqueue |
| `POST`   | `/emails/:event_id/recipients/:email/retry` | Reset one `FAILED` recipient and re-enqueue              |
| `DELETE` | `/templates/:event_type/cache`              | Evict one template from the in-memory cache              |
| `DELETE` | `/templates/cache`                          | Clear the entire template cache                          |
| `GET`    | `/admin/blocklist`                          | List all active block/allow-list entries                 |
| `POST`   | `/admin/blocklist`                          | Add or reactivate a block/allow-list entry               |
| `DELETE` | `/admin/blocklist/:id`                      | Soft-delete an entry by ID                               |
| `DELETE` | `/admin/blocklist/cache`                    | Evict the blocklist cache (triggers lazy reload)         |
| `POST`   | `/admin/blocklist/cache`                    | Evict and eagerly reload the blocklist cache             |

> **Kubernetes probes:** use `/ready` for `readinessProbe` (it pings the DB) and `/health` for `livenessProbe` (shallow process check only).

---

## 🎨 Templates

Templates are stored in the `notification_template` table. Add a new template with a SQL `INSERT` — no code change or service restart required:

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

Then flush the cache so the service picks it up immediately:

```bash
curl -X DELETE http://localhost:8080/templates/INVOICE_READY/cache \
  -H "Authorization: Bearer your-api-key"

# or via the CLI
cargo run --bin anctl -- template flush --event-type INVOICE_READY
```

Templates use [minijinja](https://docs.rs/minijinja/latest/minijinja/) (Jinja2-compatible) syntax. HTML templates are **auto-escaped** — `{{ user_input }}` is safe from XSS by default. Plain-text templates (subject, body_text) have escaping disabled.

Built-in templates (`ORDER_CONFIRMATION`, `PASSWORD_RESET`, `WELCOME`, `GENERIC_TEXT`, `GENERIC_HTML`) are seeded and kept up to date by migrations.

---

## 🛠️ CLI (`anctl`)

The `anctl` binary provides operator commands for inspecting and recovering state without touching the DB directly.

```bash
# Build the CLI
cargo build --bin anctl

# Check delivery status
anctl status --event-id <uuid>

# View recent delivery logs
anctl logs --limit 20 --status FAILED

# Retry a failed event
anctl retry --event-id <uuid>

# Send a test event
anctl send --event-type ORDER_CONFIRMATION --to user@example.com --payload '{}'

# Manage templates
anctl template list
anctl template flush --event-type ORDER_CONFIRMATION

# Manage the runtime blocklist
anctl blocklist list
anctl blocklist add --email unsubscribed@example.com
anctl blocklist remove --id <entry-id>

# Health check
anctl health
```

---

## 🚢 Deployment

### Docker Compose (local / staging)

```bash
# Full stack including observability (Prometheus + Grafana)
docker compose --profile observability up -d

# Application only (assumes external Postgres and RabbitMQ)
docker compose up anvil-notify -d
```

### Kubernetes

A ready-to-use manifest is included:

```bash
# Edit image tags and AN__* env vars in anvil-notify.yaml first
kubectl apply -f anvil-notify.yaml
```

The manifest creates a Namespace, ConfigMap, Secret, Deployment, and Service. The `readinessProbe` is wired to `/ready` and `livenessProbe` to `/health` automatically.

---

## 🗄️ Database Migrations

> **⚠️ Fresh installs vs existing deployments**
>
> `migrations/0001_initial_schema.sql` is a **consolidated** schema for **fresh installations only**. Do **not** apply it to a database that already has incremental migrations applied — doing so will produce duplicate-table errors or silently diverge from the expected schema.
>
> **Existing deployments** must continue running `sqlx migrate run`. The consolidated file is only for spinning up a new environment from scratch.

Migrations run automatically at startup via `sqlx::migrate!()`. To run them manually:

```bash
# Install sqlx-cli if needed
cargo install sqlx-cli --no-default-features --features postgres

# Run migrations
sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/anvil_notify
```

---

## 🧪 Testing

```bash
# Run all unit tests
cargo test

# Run tests for a specific crate
cargo test -p consumer

# Run with output for debugging
cargo test -- --nocapture
```

The test suite includes 109 unit tests covering the retry loop, idempotency logic, template rendering, rate limiter, recipient filter, and store layer. Integration tests against a live Postgres and RabbitMQ instance can be run via Docker Compose:

```bash
docker compose up -d
cargo test --features integration
```

---

## 🤝 Contributing

Contributions are very welcome. Here's the workflow:

1. **Fork** the repository and clone your fork locally.
2. **Create a feature branch:**
   ```bash
   git checkout -b feat/your-feature-name
   ```
3. **Make your changes**, including tests where relevant.
4. **Verify the test suite passes:**
   ```bash
   cargo test
   cargo clippy -- -D warnings
   cargo fmt --check
   ```
5. **Commit** with a descriptive message using [Conventional Commits](https://www.conventionalcommits.org/):
   ```bash
   git commit -m "feat(consumer): add support for SMS channel"
   ```
6. **Push** your branch and **open a Pull Request** against `main`.

For significant changes, please open an issue first to discuss the approach before investing time in implementation. The pre-commit hook in `.githooks/pre-commit` runs `cargo fmt` and `cargo clippy` automatically — enable it with:

```bash
git config core.hooksPath .githooks
```

---

## 📄 License

Distributed under the **MIT License**. See [LICENSE](LICENSE) for the full text.

---

<p align="center">Built with ❤️ in Rust</p>
