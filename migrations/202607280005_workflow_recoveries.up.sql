ALTER TABLE workflow_run_mutations
    ADD COLUMN mutation_kind text NOT NULL DEFAULT 'APPEND_STEPS';

ALTER TABLE workflow_run_mutations
    ADD CONSTRAINT chk_workflow_run_mutations_kind
    CHECK (mutation_kind IN ('APPEND_STEPS'));

CREATE TABLE workflow_recoveries (
    recovery_run_id uuid PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE NO ACTION,
    source_run_id uuid NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    source_step_id uuid,
    request_key text NOT NULL,
    mode text NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_workflow_recoveries_source_request
        UNIQUE (source_run_id, request_key),
    -- Column-list SET NULL is available in PostgreSQL 15+; Runledger's
    -- authoritative minimum is PostgreSQL 18.
    CONSTRAINT fk_workflow_recoveries_source_step
        FOREIGN KEY (source_run_id, source_step_id)
        REFERENCES workflow_steps (workflow_run_id, id)
        ON DELETE SET NULL (source_step_id),
    CONSTRAINT chk_workflow_recoveries_distinct_runs
        CHECK (recovery_run_id <> source_run_id),
    CONSTRAINT chk_workflow_recoveries_request_key
        CHECK (
            request_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
            AND octet_length(request_key) <= 512
        ),
    CONSTRAINT chk_workflow_recoveries_mode
        CHECK (mode IN ('FULL_REPLAY')),
    CONSTRAINT chk_workflow_recoveries_reason
        CHECK (
            reason ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
        )
);

CREATE INDEX idx_workflow_recoveries_source_run
    ON workflow_recoveries (source_run_id, created_at);
