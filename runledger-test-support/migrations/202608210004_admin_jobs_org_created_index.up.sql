-- no-transaction
CREATE INDEX CONCURRENTLY idx_job_queue_admin_org_created
    ON job_queue (organization_id, created_at DESC, id DESC);
