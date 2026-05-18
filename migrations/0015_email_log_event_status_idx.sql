-- migrations/0015_email_log_event_status_idx.sql
--
-- Adds a composite index on (event_id, status) to speed up the bulk retry
-- query:
--
--   UPDATE email_log
--   SET    status='PENDING', retry_count=0, ...
--   WHERE  event_id = $1 AND status = 'FAILED'
--
-- Without this index, Postgres uses the existing email_log_event_id_idx for
-- the event_id predicate but then filters for status in a heap scan.  With
-- this index both predicates are covered in a single index scan, which matters
-- as email_log grows large.
--
-- The partial index covers only the two non-terminal statuses that appear in
-- WHERE clauses: PENDING and FAILED.  SENT and BLOCKED rows are never targeted
-- by UPDATE or retry queries, so excluding them keeps the index small.

CREATE INDEX IF NOT EXISTS email_log_event_status_idx
    ON email_log (event_id, status)
    WHERE status IN ('PENDING', 'FAILED');
