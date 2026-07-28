DROP TABLE IF EXISTS workflow_recoveries;

ALTER TABLE workflow_run_mutations
    DROP CONSTRAINT IF EXISTS chk_workflow_run_mutations_kind,
    DROP COLUMN IF EXISTS mutation_kind;
