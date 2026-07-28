DROP TRIGGER IF EXISTS trg_job_queue_release_execution_resource ON job_queue;
DROP FUNCTION IF EXISTS release_job_execution_resource_claim();

DROP TRIGGER IF EXISTS trg_job_queue_enforce_execution_resource_claim ON job_queue;
DROP FUNCTION IF EXISTS enforce_job_execution_resource_claim_on_lease();

DROP TABLE IF EXISTS job_execution_resource_claims;

DROP INDEX IF EXISTS idx_job_queue_execution_resource_claim_order;

ALTER TABLE workflow_steps
    DROP CONSTRAINT IF EXISTS chk_workflow_steps_execution_resource_key,
    DROP COLUMN IF EXISTS execution_resource_key;

ALTER TABLE job_queue
    DROP CONSTRAINT IF EXISTS chk_job_queue_execution_resource_key,
    DROP COLUMN IF EXISTS execution_resource_key;
