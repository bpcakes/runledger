ALTER TABLE job_attempts
    DROP CONSTRAINT IF EXISTS chk_job_attempts_retry_timing_audit_shape,
    DROP CONSTRAINT IF EXISTS chk_job_attempts_retry_timing_source;

ALTER TABLE job_attempts
    DROP COLUMN IF EXISTS retry_timing_source,
    DROP COLUMN IF EXISTS effective_next_run_at,
    DROP COLUMN IF EXISTS requested_retry_not_before;
