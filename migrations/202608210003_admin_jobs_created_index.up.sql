-- no-transaction
CREATE INDEX CONCURRENTLY idx_job_queue_admin_created
    ON job_queue (created_at DESC, id DESC);
