-- no-transaction
CREATE INDEX CONCURRENTLY idx_job_logs_job_id_newest
    ON job_logs (job_id, id DESC);
