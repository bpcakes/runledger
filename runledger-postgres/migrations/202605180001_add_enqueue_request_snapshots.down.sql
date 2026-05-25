-- Down migrations must run in reverse version order. If this file is executed
-- manually without first running 202605220001 down, drop the cutover constraints
-- before dropping enqueue_request.
ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS ck_workflow_runs_idempotency_enqueue_request;

ALTER TABLE job_queue
    DROP CONSTRAINT IF EXISTS ck_job_queue_idempotency_enqueue_request;

ALTER TABLE workflow_runs
    DROP COLUMN IF EXISTS enqueue_request;

ALTER TABLE job_queue
    DROP COLUMN IF EXISTS enqueue_request;

DELETE FROM runledger_migration_history
WHERE version = 202605180001;
