DROP VIEW IF EXISTS job_metrics_rollup;

DROP INDEX IF EXISTS uq_workflow_run_mutations_run_mutation_key;
DROP TRIGGER IF EXISTS trg_workflow_run_mutations_set_updated_at
    ON workflow_run_mutations;
DROP TABLE IF EXISTS workflow_run_mutations;

DROP TRIGGER IF EXISTS trg_job_runtime_configs_set_updated_at
    ON job_runtime_configs;
DROP TABLE IF EXISTS job_runtime_configs;

DROP INDEX IF EXISTS idx_job_logs_level_time;
DROP INDEX IF EXISTS idx_job_logs_job_run_id;
DROP TABLE IF EXISTS job_logs;

DROP TRIGGER IF EXISTS trg_workflow_steps_job_linkage_symmetry ON workflow_steps;
DROP TRIGGER IF EXISTS trg_job_queue_workflow_step_linkage_symmetry ON job_queue;
DROP FUNCTION IF EXISTS enforce_workflow_job_linkage_symmetry();

DROP INDEX IF EXISTS idx_workflow_step_dependencies_dependent;
DROP INDEX IF EXISTS idx_workflow_step_dependencies_prerequisite;
DROP TABLE IF EXISTS workflow_step_dependencies;

ALTER TABLE job_queue DROP CONSTRAINT IF EXISTS uq_job_queue_workflow_step_id;
ALTER TABLE job_queue DROP CONSTRAINT IF EXISTS fk_job_queue_workflow_step;

DROP INDEX IF EXISTS idx_workflow_steps_run_status_created;
DROP TRIGGER IF EXISTS trg_workflow_steps_set_updated_at
    ON workflow_steps;
DROP TABLE IF EXISTS workflow_steps;

DROP INDEX IF EXISTS idx_workflow_runs_status_created;
DROP INDEX IF EXISTS uq_workflow_runs_type_idempotency_global;
DROP INDEX IF EXISTS uq_workflow_runs_type_idempotency_org;
DROP TRIGGER IF EXISTS trg_workflow_runs_set_updated_at
    ON workflow_runs;
DROP TABLE IF EXISTS workflow_runs;

DROP INDEX IF EXISTS idx_job_schedules_next_fire;
DROP TRIGGER IF EXISTS trg_job_schedules_set_updated_at
    ON job_schedules;
DROP TABLE IF EXISTS job_schedules;

DROP INDEX IF EXISTS idx_job_dead_letters_failed_at;
DROP TABLE IF EXISTS job_dead_letters;

DROP INDEX IF EXISTS idx_job_events_type_time;
DROP INDEX IF EXISTS idx_job_events_job_run_id;
DROP TABLE IF EXISTS job_events;

DROP INDEX IF EXISTS idx_job_attempts_job_run_created;
DROP TABLE IF EXISTS job_attempts;

DROP INDEX IF EXISTS idx_job_queue_type_status_created;
DROP INDEX IF EXISTS idx_job_queue_org_status_created;
DROP INDEX IF EXISTS idx_job_queue_leased_expiry;
DROP INDEX IF EXISTS idx_job_queue_claim;
DROP INDEX IF EXISTS uq_job_queue_type_idempotency_global;
DROP INDEX IF EXISTS uq_job_queue_type_idempotency_org;
DROP TRIGGER IF EXISTS trg_job_queue_set_updated_at
    ON job_queue;
DROP TABLE IF EXISTS job_queue;

DROP TRIGGER IF EXISTS trg_job_definitions_set_updated_at
    ON job_definitions;
DROP TABLE IF EXISTS job_definitions;

DROP TYPE IF EXISTS workflow_step_execution_kind;
DROP TYPE IF EXISTS workflow_dependency_release_mode;
DROP TYPE IF EXISTS workflow_step_status;
DROP TYPE IF EXISTS workflow_run_status;
DROP TYPE IF EXISTS job_failure_kind;
DROP TYPE IF EXISTS job_event_type;
DROP TYPE IF EXISTS job_status;

DROP FUNCTION IF EXISTS set_updated_at_timestamp() CASCADE;
