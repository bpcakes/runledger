CREATE TABLE job_enqueue_intents (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    job_type text NOT NULL,
    organization_id uuid,
    payload jsonb NOT NULL,
    priority integer,
    max_attempts integer,
    timeout_seconds integer,
    next_run_at timestamptz,
    idempotency_key text NOT NULL,
    stage text NOT NULL DEFAULT 'queued',
    enqueue_request_version smallint NOT NULL DEFAULT 1,
    enqueue_request jsonb NOT NULL,
    execution_resource_key text,
    promotion_attempts integer NOT NULL DEFAULT 0,
    next_promotion_at timestamptz NOT NULL DEFAULT now(),
    last_attempted_at timestamptz,
    status text NOT NULL DEFAULT 'PENDING',
    promoted_job_id uuid,
    promoted_at timestamptz,
    conflicted_at timestamptz,
    last_error_code text,
    last_error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_enqueue_intents_promoted_job
        FOREIGN KEY (promoted_job_id)
        -- Preserve promotion lineage and the idempotency fence until queue
        -- retention removes the exact link in its job-deletion transaction.
        REFERENCES job_queue (id) ON DELETE RESTRICT,
    CONSTRAINT chk_job_enqueue_intents_job_type_not_blank
        CHECK (length(trim(job_type)) > 0),
    CONSTRAINT chk_job_enqueue_intents_max_attempts_positive
        CHECK (max_attempts IS NULL OR max_attempts > 0),
    CONSTRAINT chk_job_enqueue_intents_timeout_positive
        CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    CONSTRAINT chk_job_enqueue_intents_idempotency_key_not_blank
        CHECK (length(trim(idempotency_key)) > 0),
    CONSTRAINT chk_job_enqueue_intents_stage_not_blank
        CHECK (length(trim(stage)) > 0),
    CONSTRAINT chk_job_enqueue_intents_enqueue_request_version
        CHECK (enqueue_request_version = 1),
    CONSTRAINT chk_job_enqueue_intents_execution_resource_key
        CHECK (
            execution_resource_key IS NULL
            OR (
                execution_resource_key ~ U&'[^[:space:]\00A0\0085\1680\2000-\200A\2028\2029\202F\205F\3000]'
                AND octet_length(execution_resource_key) <= 512
            )
        ),
    CONSTRAINT chk_job_enqueue_intents_promotion_attempts
        CHECK (promotion_attempts >= 0),
    CONSTRAINT chk_job_enqueue_intents_status
        CHECK (status IN ('PENDING', 'PROMOTED', 'CONFLICTED')),
    CONSTRAINT chk_job_enqueue_intents_state_fields
        CHECK (
            (
                status = 'PENDING'
                AND promoted_job_id IS NULL
                AND promoted_at IS NULL
                AND conflicted_at IS NULL
                AND (
                    (
                        promotion_attempts = 0
                        AND last_attempted_at IS NULL
                        AND last_error_code IS NULL
                        AND last_error_message IS NULL
                    )
                    OR (
                        promotion_attempts > 0
                        AND last_attempted_at IS NOT NULL
                        AND last_error_code IS NOT NULL
                        AND length(trim(last_error_code)) > 0
                        AND last_error_message IS NOT NULL
                        AND length(trim(last_error_message)) > 0
                    )
                )
            )
            OR (
                status = 'PROMOTED'
                AND promotion_attempts > 0
                AND last_attempted_at IS NOT NULL
                AND promoted_job_id IS NOT NULL
                AND promoted_at IS NOT NULL
                AND conflicted_at IS NULL
                AND last_error_code IS NULL
                AND last_error_message IS NULL
            )
            OR (
                status = 'CONFLICTED'
                AND promotion_attempts > 0
                AND last_attempted_at IS NOT NULL
                AND promoted_job_id IS NULL
                AND promoted_at IS NULL
                AND conflicted_at IS NOT NULL
                AND last_error_code IS NOT NULL
                AND length(trim(last_error_code)) > 0
                AND last_error_message IS NOT NULL
                AND length(trim(last_error_message)) > 0
            )
        )
);

CREATE TRIGGER trg_job_enqueue_intents_set_updated_at
    BEFORE UPDATE ON job_enqueue_intents
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE UNIQUE INDEX uq_job_enqueue_intents_type_idempotency_org
    ON job_enqueue_intents (job_type, organization_id, idempotency_key)
    WHERE organization_id IS NOT NULL;

CREATE UNIQUE INDEX uq_job_enqueue_intents_type_idempotency_global
    ON job_enqueue_intents (job_type, idempotency_key)
    WHERE organization_id IS NULL;

CREATE INDEX idx_job_enqueue_intents_pending
    ON job_enqueue_intents (next_promotion_at, created_at, id)
    INCLUDE (job_type, enqueue_request_version)
    WHERE status = 'PENDING';

-- Complements the global-order index above when stale or misspelled job types
-- dominate the pending backlog. PostgreSQL can choose this index to constrain
-- a worker's registered-type allowlist before ordering eligible rows.
CREATE INDEX idx_job_enqueue_intents_pending_type
    ON job_enqueue_intents (job_type, next_promotion_at, created_at, id)
    INCLUDE (enqueue_request_version)
    WHERE status = 'PENDING';

CREATE INDEX idx_job_enqueue_intents_pending_metrics
    ON job_enqueue_intents (job_type, created_at)
    INCLUDE (promotion_attempts)
    WHERE status = 'PENDING';

CREATE INDEX idx_job_enqueue_intents_org_pending_metrics
    ON job_enqueue_intents (organization_id, job_type, created_at)
    INCLUDE (promotion_attempts)
    WHERE status = 'PENDING'
      AND organization_id IS NOT NULL;

CREATE INDEX idx_job_enqueue_intents_conflicted_metrics
    ON job_enqueue_intents (job_type)
    WHERE status = 'CONFLICTED';

CREATE INDEX idx_job_enqueue_intents_org_conflicted_metrics
    ON job_enqueue_intents (organization_id, job_type)
    WHERE status = 'CONFLICTED'
      AND organization_id IS NOT NULL;

CREATE INDEX idx_job_enqueue_intents_created
    ON job_enqueue_intents (created_at DESC, id DESC)
    INCLUDE (status, job_type, organization_id);

CREATE INDEX idx_job_enqueue_intents_org_created
    ON job_enqueue_intents (organization_id, created_at DESC, id DESC)
    INCLUDE (status, job_type);

CREATE INDEX idx_job_enqueue_intents_promoted_cleanup
    ON job_enqueue_intents (promoted_at, id)
    INCLUDE (job_type, organization_id)
    WHERE status = 'PROMOTED';

-- PostgreSQL does not automatically index the referencing side of a foreign
-- key. This supports both exact-job retention cleanup and the RESTRICT check
-- when an application deletes job_queue rows.
CREATE INDEX idx_job_enqueue_intents_promoted_job
    ON job_enqueue_intents (promoted_job_id)
    WHERE promoted_job_id IS NOT NULL;

-- Deliberately omit this additive migration from runledger_migration_history.
-- Older filtered startup guards, workers, and non-retention writers can coexist
-- with the new table during migration-first rollout and code rollback. Once an
-- intent is promoted, its RESTRICT foreign key deliberately fences deletion of
-- the linked job until application retention deletes the promoted intent first.
