ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS fk_workflow_runs_result_step;

ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS chk_workflow_runs_result_step_key_not_blank;

ALTER TABLE workflow_runs
    DROP COLUMN IF EXISTS result,
    DROP COLUMN IF EXISTS result_step_key;

ALTER TABLE workflow_steps
    DROP COLUMN IF EXISTS output;

ALTER TABLE job_queue
    DROP COLUMN IF EXISTS output;

DELETE FROM runledger_migration_history
WHERE version = 202606030001;
