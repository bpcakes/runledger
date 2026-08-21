CREATE INDEX idx_job_events_job_id_newest
    ON job_events (job_id, id DESC);

CREATE INDEX idx_job_logs_job_id_newest
    ON job_logs (job_id, id DESC);
