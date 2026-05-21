ALTER TABLE workflow_runs
    DROP COLUMN IF EXISTS enqueue_request;

ALTER TABLE job_queue
    DROP COLUMN IF EXISTS enqueue_request;

DELETE FROM runledger_migration_history
WHERE version = 202605180001;
