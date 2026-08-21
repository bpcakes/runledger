-- no-transaction
CREATE INDEX CONCURRENTLY idx_job_events_job_id_newest
    ON job_events (job_id, id DESC);
