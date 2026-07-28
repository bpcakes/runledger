ALTER TABLE job_attempts
    ADD COLUMN requested_retry_not_before timestamptz,
    ADD COLUMN effective_next_run_at timestamptz,
    ADD COLUMN retry_timing_source text;

ALTER TABLE job_attempts
    ADD CONSTRAINT chk_job_attempts_retry_timing_source
    CHECK (
        retry_timing_source IS NULL
        OR retry_timing_source IN ('POLICY', 'HANDLER_NOT_BEFORE')
    );

ALTER TABLE job_attempts
    ADD CONSTRAINT chk_job_attempts_retry_timing_audit_shape
    CHECK (
        (
            effective_next_run_at IS NULL
            AND retry_timing_source IS NULL
            AND requested_retry_not_before IS NULL
        )
        OR (
            effective_next_run_at IS NOT NULL
            AND retry_timing_source IS NOT NULL
        )
    );
