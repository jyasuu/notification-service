# Business DB Migrations

These SQL files apply to the **business service database** — the one your
application writes orders, users, etc. into. They are **not** run by the
notification service's `sqlx::migrate!()` call (which targets the
notification DB at `database.url`).

Run them manually, or embed them in your business service's own migration
pipeline:

```bash
# Example — psql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0002_create_outbox.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0005_outbox_from_override.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0006_outbox_fail_count.sql
psql "$BUSINESS_DATABASE_URL" -f migrations/business_db/0008_outbox_attachments.sql
```

| File | Purpose |
|------|---------|
| `0002_create_outbox.sql` | Core outbox table the business service writes into |
| `0005_outbox_from_override.sql` | Documents the `from_override` payload field, adds monitoring index |
| `0006_outbox_fail_count.sql` | Adds `fail_count` column to cap permanently broken rows |
| `0008_outbox_attachments.sql` | Documents the `attachments` payload field, adds monitoring index |
