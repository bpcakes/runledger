-- no-transaction
CREATE INDEX CONCURRENTLY idx_workflow_runs_admin_created
    ON workflow_runs (created_at DESC, id DESC);
