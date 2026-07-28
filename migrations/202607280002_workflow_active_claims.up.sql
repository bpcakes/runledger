CREATE TABLE workflow_active_claims (
    scope text NOT NULL,
    active_key text NOT NULL,
    workflow_run_id uuid NOT NULL,
    release_pending boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_workflow_active_claims
        PRIMARY KEY (scope, active_key),
    CONSTRAINT uq_workflow_active_claims_run
        UNIQUE (workflow_run_id),
    CONSTRAINT fk_workflow_active_claims_run
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (id) ON DELETE CASCADE,
    CONSTRAINT chk_workflow_active_claims_scope_not_blank
        CHECK (
            scope ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
        ),
    CONSTRAINT chk_workflow_active_claims_key_not_blank
        CHECK (
            active_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
            AND octet_length(active_key) <= 512
        )
);

CREATE TRIGGER trg_workflow_active_claims_set_updated_at
    BEFORE UPDATE ON workflow_active_claims
    FOR EACH ROW
    WHEN (OLD IS DISTINCT FROM NEW)
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE INDEX idx_workflow_active_claims_release_pending
    ON workflow_active_claims (updated_at, workflow_run_id)
    WHERE release_pending;

CREATE FUNCTION mark_terminal_workflow_active_claim_release_pending()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE workflow_active_claims
    SET release_pending = true
    WHERE workflow_run_id = NEW.id;

    RETURN NEW;
END
$$;

CREATE TRIGGER trg_workflow_runs_mark_active_claim_release_pending
AFTER UPDATE OF status ON workflow_runs
FOR EACH ROW
WHEN (
    OLD.status IS DISTINCT FROM NEW.status
    AND NEW.status IN ('SUCCEEDED', 'COMPLETED_WITH_ERRORS', 'CANCELED')
)
EXECUTE FUNCTION mark_terminal_workflow_active_claim_release_pending();
