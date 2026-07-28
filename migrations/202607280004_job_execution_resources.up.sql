ALTER TABLE job_queue
    ADD COLUMN execution_resource_key text;

ALTER TABLE job_queue
    ADD CONSTRAINT chk_job_queue_execution_resource_key
    CHECK (
        execution_resource_key IS NULL
        OR (
            execution_resource_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
            AND octet_length(execution_resource_key) <= 512
        )
    );

CREATE INDEX idx_job_queue_execution_resource_claim_order
    ON job_queue (
        priority DESC,
        next_run_at,
        created_at,
        id
    )
    INCLUDE (execution_resource_key, job_type)
    WHERE status = 'PENDING'
      AND execution_resource_key IS NOT NULL;

ALTER TABLE workflow_steps
    ADD COLUMN execution_resource_key text;

ALTER TABLE workflow_steps
    ADD CONSTRAINT chk_workflow_steps_execution_resource_key
    CHECK (
        (
            execution_resource_key IS NULL
            OR (
                execution_resource_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
                AND octet_length(execution_resource_key) <= 512
            )
        )
        AND (
            execution_kind = 'JOB'
            OR execution_resource_key IS NULL
        )
    );

CREATE TABLE job_execution_resource_claims (
    resource_key text PRIMARY KEY,
    job_id uuid NOT NULL UNIQUE REFERENCES job_queue(id) ON DELETE CASCADE,
    run_number integer NOT NULL,
    attempt integer NOT NULL,
    worker_id text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    release_after timestamptz,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_job_execution_resource_claims_resource_key
        CHECK (
            resource_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
            AND octet_length(resource_key) <= 512
        ),
    CONSTRAINT chk_job_execution_resource_claims_run_number
        CHECK (run_number > 0),
    CONSTRAINT chk_job_execution_resource_claims_attempt
        CHECK (attempt > 0),
    CONSTRAINT chk_job_execution_resource_claims_worker_id
        CHECK (
            worker_id ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
        )
);

CREATE INDEX idx_job_execution_resource_claims_release_after
    ON job_execution_resource_claims (release_after)
    WHERE release_after IS NOT NULL;

CREATE INDEX idx_job_execution_resource_claims_lease_expires_at
    ON job_execution_resource_claims (lease_expires_at)
    WHERE release_after IS NULL;

CREATE FUNCTION enforce_job_execution_resource_claim_on_lease()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM job_execution_resource_claims claim
        WHERE claim.resource_key = NEW.execution_resource_key
          AND claim.job_id = NEW.id
          AND claim.run_number = NEW.run_number
          AND claim.attempt = NEW.attempt
          AND claim.worker_id = NEW.worker_id
    )
    THEN
        RAISE EXCEPTION
            'resource-constrained job lease requires a matching durable claim'
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'chk_job_queue_execution_resource_claim_required';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER trg_job_queue_enforce_execution_resource_claim
BEFORE UPDATE OF status ON job_queue
FOR EACH ROW
WHEN (
    OLD.status IS DISTINCT FROM NEW.status
    AND NEW.status = 'LEASED'
    AND NEW.execution_resource_key IS NOT NULL
)
EXECUTE FUNCTION enforce_job_execution_resource_claim_on_lease();

CREATE FUNCTION release_job_execution_resource_claim()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status = 'LEASED'
       AND (
            NEW.status <> 'LEASED'
            OR NEW.run_number <> OLD.run_number
            OR NEW.attempt <> OLD.attempt
            OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
       )
    THEN
        IF NEW.status = 'CANCELED'
           AND OLD.lease_expires_at IS NOT NULL
           AND OLD.lease_expires_at > clock_timestamp()
        THEN
            UPDATE job_execution_resource_claims
            SET release_after = OLD.lease_expires_at
            WHERE job_id = OLD.id
              AND run_number = OLD.run_number
              AND attempt = OLD.attempt
              AND worker_id = OLD.worker_id;
        ELSE
            DELETE FROM job_execution_resource_claims
            WHERE job_id = OLD.id
              AND run_number = OLD.run_number
              AND attempt = OLD.attempt
              AND worker_id = OLD.worker_id;
        END IF;
    ELSIF OLD.status = 'LEASED'
          AND NEW.status = 'LEASED'
          AND NEW.run_number = OLD.run_number
          AND NEW.attempt = OLD.attempt
          AND NEW.worker_id IS NOT DISTINCT FROM OLD.worker_id
          AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at
    THEN
        UPDATE job_execution_resource_claims
        SET lease_expires_at = NEW.lease_expires_at
        WHERE job_id = NEW.id
          AND run_number = NEW.run_number
          AND attempt = NEW.attempt
          AND worker_id = NEW.worker_id;
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER trg_job_queue_release_execution_resource
AFTER UPDATE OF status, run_number, attempt, worker_id, lease_expires_at ON job_queue
FOR EACH ROW
WHEN (
    OLD.status = 'LEASED'
    AND OLD.execution_resource_key IS NOT NULL
)
EXECUTE FUNCTION release_job_execution_resource_claim();
