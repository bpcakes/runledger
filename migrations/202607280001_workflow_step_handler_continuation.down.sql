ALTER TABLE workflow_steps
    DROP CONSTRAINT IF EXISTS chk_workflow_steps_handler_continuation_job_only;

ALTER TABLE workflow_steps
    DROP COLUMN IF EXISTS allow_handler_continuation;
