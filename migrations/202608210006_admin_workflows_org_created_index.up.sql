-- no-transaction
CREATE INDEX CONCURRENTLY idx_workflow_runs_admin_org_created
    ON workflow_runs (organization_id, created_at DESC, id DESC);
