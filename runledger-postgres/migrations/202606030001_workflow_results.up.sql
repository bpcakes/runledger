ALTER TABLE job_queue
    ADD COLUMN output jsonb;

ALTER TABLE workflow_steps
    ADD COLUMN output jsonb;

ALTER TABLE workflow_runs
    ADD COLUMN result_step_key text,
    ADD COLUMN result jsonb;

ALTER TABLE workflow_runs
    ADD CONSTRAINT chk_workflow_runs_result_step_key_not_blank
    CHECK (result_step_key IS NULL OR length(trim(result_step_key)) > 0)
    NOT VALID;

ALTER TABLE workflow_runs
    VALIDATE CONSTRAINT chk_workflow_runs_result_step_key_not_blank;

ALTER TABLE workflow_runs
    ADD CONSTRAINT fk_workflow_runs_result_step
    FOREIGN KEY (id, result_step_key)
    REFERENCES workflow_steps (workflow_run_id, step_key)
    DEFERRABLE INITIALLY DEFERRED
    NOT VALID;

ALTER TABLE workflow_runs
    VALIDATE CONSTRAINT fk_workflow_runs_result_step;

INSERT INTO runledger_migration_history (version)
VALUES (202606030001)
ON CONFLICT (version) DO NOTHING;
