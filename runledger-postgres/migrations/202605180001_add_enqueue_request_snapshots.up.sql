-- Existing rows intentionally keep NULL snapshots. Their original enqueue
-- requests cannot be reconstructed safely after workflow steps or queue rows
-- may have been mutated, so runtime code uses explicit legacy fallback checks.
ALTER TABLE job_queue
    ADD COLUMN IF NOT EXISTS enqueue_request jsonb;

ALTER TABLE workflow_runs
    ADD COLUMN IF NOT EXISTS enqueue_request jsonb;

INSERT INTO runledger_migration_history (version)
VALUES (202605180001)
ON CONFLICT (version) DO NOTHING;
