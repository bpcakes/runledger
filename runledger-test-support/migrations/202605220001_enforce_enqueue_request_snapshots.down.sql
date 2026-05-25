-- This down migration is intended for development/test rollback only. Runtime
-- code no longer contains the legacy idempotency fallback this constraint
-- replaced.
ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS ck_workflow_runs_idempotency_enqueue_request;

ALTER TABLE job_queue
    DROP CONSTRAINT IF EXISTS ck_job_queue_idempotency_enqueue_request;

DROP INDEX IF EXISTS idx_workflow_runs_missing_enqueue_request_snapshot;

DROP INDEX IF EXISTS idx_job_queue_missing_enqueue_request_snapshot;

DELETE FROM runledger_migration_history
WHERE version = 202605220001;
