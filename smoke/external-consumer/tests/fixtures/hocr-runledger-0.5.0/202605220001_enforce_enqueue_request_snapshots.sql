-- Block new keyed rows that do not carry the canonical enqueue request
-- snapshot, while allowing startup validation to report any pre-cutover rows.
-- These constraints start NOT VALID so the migration can be applied before
-- legacy rows are remediated; migrate_after_idempotency_cutover validates them
-- after the legacy-row check passes.
--
-- The helper indexes are intentionally regular CREATE INDEX statements because
-- SQLx migrator runs migrations transactionally. Apply this migration during a
-- maintenance window sized for your write volume on job_queue/workflow_runs.
CREATE INDEX IF NOT EXISTS idx_job_queue_missing_enqueue_request_snapshot
    ON job_queue (id)
    WHERE idempotency_key IS NOT NULL
      AND enqueue_request IS NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_missing_enqueue_request_snapshot
    ON workflow_runs (id)
    WHERE idempotency_key IS NOT NULL
      AND enqueue_request IS NULL;

ALTER TABLE job_queue
    DROP CONSTRAINT IF EXISTS ck_job_queue_idempotency_enqueue_request;

ALTER TABLE job_queue
    ADD CONSTRAINT ck_job_queue_idempotency_enqueue_request
    CHECK (idempotency_key IS NULL OR enqueue_request IS NOT NULL)
    NOT VALID;

ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS ck_workflow_runs_idempotency_enqueue_request;

ALTER TABLE workflow_runs
    ADD CONSTRAINT ck_workflow_runs_idempotency_enqueue_request
    CHECK (idempotency_key IS NULL OR enqueue_request IS NOT NULL)
    NOT VALID;

INSERT INTO runledger_migration_history (version)
VALUES (202605220001)
ON CONFLICT (version) DO NOTHING;
