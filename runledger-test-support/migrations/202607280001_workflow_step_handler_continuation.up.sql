ALTER TABLE workflow_steps
    ADD COLUMN allow_handler_continuation boolean NOT NULL DEFAULT false;

ALTER TABLE workflow_steps
    ADD CONSTRAINT chk_workflow_steps_handler_continuation_job_only
    CHECK (
        execution_kind = 'JOB'
        OR NOT allow_handler_continuation
    );
