-- migrations/business_db/0016_outbox_locked_at.sql
--
-- Adds a `locked_at` timestamp to the outbox table so the worker can detect
-- and recover rows that are stuck in the IN_PROGRESS state after a crash.
--
-- Without this column, any row that transitions to IN_PROGRESS and then has
-- its worker process killed (OOM, pod eviction, SIGKILL) stays IN_PROGRESS
-- forever — it is invisible to the PENDING query and never retried.
--
-- The outbox worker now runs a periodic reaper that resets any IN_PROGRESS
-- row whose locked_at is older than a configurable threshold (default: 5 min)
-- back to PENDING, making it eligible for re-processing.
--
-- Apply to the BUSINESS SERVICE database, not the anvil-notify DB.

ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

-- Index used by the reaper query: find stale IN_PROGRESS rows quickly.
CREATE INDEX IF NOT EXISTS outbox_locked_at_idx
    ON outbox (locked_at ASC)
    WHERE status = 'IN_PROGRESS';

-- Back-fill: rows already IN_PROGRESS at migration time are given a locked_at
-- of now() so the reaper clock starts ticking from the migration date rather
-- than treating them as permanently stuck from epoch.
UPDATE outbox
SET    locked_at = now()
WHERE  status = 'IN_PROGRESS'
  AND  locked_at IS NULL;