CREATE FUNCTION set_updated_at_timestamp()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TYPE job_status AS ENUM (
    'PENDING',
    'LEASED',
    'SUCCEEDED',
    'DEAD_LETTERED',
    'CANCELED'
);

CREATE TYPE job_event_type AS ENUM (
    'ENQUEUED',
    'LEASED',
    'HEARTBEAT',
    'STAGE_CHANGED',
    'PROGRESS',
    'RETRY_SCHEDULED',
    'SUCCEEDED',
    'FAILED',
    'DEAD_LETTERED',
    'CANCELED',
    'REQUEUED'
);

CREATE TYPE job_failure_kind AS ENUM (
    'RETRYABLE',
    'TERMINAL',
    'PANICKED',
    'TIMEOUT',
    'LEASE_EXPIRED'
);

CREATE TYPE workflow_run_status AS ENUM (
    'RUNNING',
    'WAITING_FOR_EXTERNAL',
    'SUCCEEDED',
    'COMPLETED_WITH_ERRORS',
    'CANCELED'
);

CREATE TYPE workflow_step_status AS ENUM (
    'BLOCKED',
    'WAITING_FOR_EXTERNAL',
    'ENQUEUED',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'CANCELED'
);

CREATE TYPE workflow_dependency_release_mode AS ENUM (
    'ON_TERMINAL',
    'ON_SUCCESS'
);

CREATE TYPE workflow_step_execution_kind AS ENUM (
    'JOB',
    'EXTERNAL'
);

CREATE TABLE job_definitions (
    job_type text PRIMARY KEY,
    version integer NOT NULL DEFAULT 1,
    max_attempts integer NOT NULL DEFAULT 8,
    default_timeout_seconds integer NOT NULL DEFAULT 900,
    default_priority integer NOT NULL DEFAULT 100,
    is_enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_job_definitions_type_not_blank
        CHECK (length(trim(job_type)) > 0),
    CONSTRAINT chk_job_definitions_version_positive
        CHECK (version > 0),
    CONSTRAINT chk_job_definitions_max_attempts_positive
        CHECK (max_attempts > 0),
    CONSTRAINT chk_job_definitions_timeout_positive
        CHECK (default_timeout_seconds > 0)
);

CREATE TRIGGER trg_job_definitions_set_updated_at
    BEFORE UPDATE ON job_definitions
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE job_queue (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    job_type text NOT NULL,
    organization_id uuid,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status job_status NOT NULL DEFAULT 'PENDING',
    priority integer NOT NULL DEFAULT 100,
    run_number integer NOT NULL DEFAULT 1,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    timeout_seconds integer NOT NULL DEFAULT 900,
    next_run_at timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    last_heartbeat_at timestamptz,
    worker_id text,
    started_at timestamptz,
    finished_at timestamptz,
    stage text NOT NULL DEFAULT 'queued',
    progress_done bigint,
    progress_total bigint,
    progress_pct numeric GENERATED ALWAYS AS (
        CASE
            WHEN progress_done IS NULL
                OR progress_total IS NULL
                OR progress_total <= 0 THEN NULL
            ELSE round((progress_done::numeric * 100.0) / progress_total::numeric, 2)
        END
    ) STORED,
    checkpoint jsonb,
    idempotency_key text,
    status_reason text,
    last_error_code text,
    last_error_message text,
    workflow_step_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_queue_job_type
        FOREIGN KEY (job_type)
        REFERENCES job_definitions (job_type) ON DELETE RESTRICT,
    CONSTRAINT chk_job_queue_attempt_nonnegative
        CHECK (attempt >= 0),
    CONSTRAINT chk_job_queue_max_attempts_positive
        CHECK (max_attempts > 0),
    CONSTRAINT chk_job_queue_run_number_positive
        CHECK (run_number > 0),
    CONSTRAINT chk_job_queue_timeout_positive
        CHECK (timeout_seconds > 0),
    CONSTRAINT chk_job_queue_progress_nonnegative
        CHECK (
            (progress_done IS NULL OR progress_done >= 0)
            AND (progress_total IS NULL OR progress_total >= 0)
            AND (
                progress_done IS NULL
                OR progress_total IS NULL
                OR progress_done <= progress_total
            )
        ),
    CONSTRAINT chk_job_queue_stage_not_blank
        CHECK (length(trim(stage)) > 0),
    CONSTRAINT chk_job_queue_worker_id_not_blank
        CHECK (worker_id IS NULL OR length(trim(worker_id)) > 0),
    CONSTRAINT chk_job_queue_idempotency_key_not_blank
        CHECK (idempotency_key IS NULL OR length(trim(idempotency_key)) > 0)
);

CREATE TRIGGER trg_job_queue_set_updated_at
    BEFORE UPDATE ON job_queue
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE UNIQUE INDEX uq_job_queue_type_idempotency_org
    ON job_queue (job_type, organization_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND organization_id IS NOT NULL;

CREATE UNIQUE INDEX uq_job_queue_type_idempotency_global
    ON job_queue (job_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND organization_id IS NULL;

CREATE INDEX idx_job_queue_claim
    ON job_queue (status, priority DESC, next_run_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_job_queue_leased_expiry
    ON job_queue (lease_expires_at)
    WHERE status = 'LEASED';

CREATE INDEX idx_job_queue_org_status_created
    ON job_queue (organization_id, status, created_at DESC);

CREATE INDEX idx_job_queue_type_status_created
    ON job_queue (job_type, status, created_at DESC);

CREATE TABLE job_attempts (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    job_id uuid NOT NULL,
    run_number integer NOT NULL DEFAULT 1,
    attempt integer NOT NULL,
    worker_id text NOT NULL,
    leased_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome job_failure_kind,
    error_code text,
    error_message text,
    retry_delay_ms integer,
    claim_origin text NOT NULL DEFAULT 'DIRECT',
    execution_started_persisted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_attempts_job
        FOREIGN KEY (job_id)
        REFERENCES job_queue (id) ON DELETE CASCADE,
    CONSTRAINT uq_job_attempts_job_run_attempt
        UNIQUE (job_id, run_number, attempt),
    CONSTRAINT chk_job_attempts_attempt_positive
        CHECK (attempt > 0),
    CONSTRAINT chk_job_attempts_run_number_positive
        CHECK (run_number > 0),
    CONSTRAINT chk_job_attempts_worker_not_blank
        CHECK (length(trim(worker_id)) > 0),
    CONSTRAINT chk_job_attempts_retry_delay_nonnegative
        CHECK (retry_delay_ms IS NULL OR retry_delay_ms >= 0),
    CONSTRAINT chk_job_attempts_claim_origin
        CHECK (claim_origin IN ('DIRECT', 'WORKER_PRESTART'))
);

CREATE INDEX idx_job_attempts_job_run_created
    ON job_attempts (job_id, run_number, created_at DESC);

CREATE TABLE job_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL,
    run_number integer NOT NULL DEFAULT 1,
    attempt integer,
    event_type job_event_type NOT NULL,
    stage text,
    progress_done bigint,
    progress_total bigint,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_events_job
        FOREIGN KEY (job_id)
        REFERENCES job_queue (id) ON DELETE CASCADE,
    CONSTRAINT chk_job_events_attempt_nonnegative
        CHECK (attempt IS NULL OR attempt > 0),
    CONSTRAINT chk_job_events_run_number_positive
        CHECK (run_number > 0),
    CONSTRAINT chk_job_events_stage_not_blank
        CHECK (stage IS NULL OR length(trim(stage)) > 0),
    CONSTRAINT chk_job_events_progress_nonnegative
        CHECK (
            (progress_done IS NULL OR progress_done >= 0)
            AND (progress_total IS NULL OR progress_total >= 0)
            AND (
                progress_done IS NULL
                OR progress_total IS NULL
                OR progress_done <= progress_total
            )
        )
);

CREATE INDEX idx_job_events_job_run_id
    ON job_events (job_id, run_number, id);

CREATE INDEX idx_job_events_type_time
    ON job_events (event_type, occurred_at DESC);

CREATE TABLE job_dead_letters (
    job_id uuid PRIMARY KEY,
    job_type text NOT NULL,
    organization_id uuid,
    run_number integer NOT NULL DEFAULT 1,
    attempt integer NOT NULL,
    error_code text,
    error_message text,
    payload_snapshot jsonb NOT NULL,
    checkpoint_snapshot jsonb,
    failed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_dead_letters_job
        FOREIGN KEY (job_id)
        REFERENCES job_queue (id) ON DELETE CASCADE,
    CONSTRAINT fk_job_dead_letters_job_type
        FOREIGN KEY (job_type)
        REFERENCES job_definitions (job_type) ON DELETE RESTRICT,
    CONSTRAINT chk_job_dead_letters_run_number_positive
        CHECK (run_number > 0),
    CONSTRAINT chk_job_dead_letters_attempt_positive
        CHECK (attempt > 0)
);

CREATE INDEX idx_job_dead_letters_failed_at
    ON job_dead_letters (failed_at DESC);

CREATE TABLE job_schedules (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    name text NOT NULL UNIQUE,
    job_type text NOT NULL,
    organization_id uuid,
    payload_template jsonb NOT NULL DEFAULT '{}'::jsonb,
    cron_expr text NOT NULL,
    timezone text NOT NULL DEFAULT 'UTC',
    is_active boolean NOT NULL DEFAULT true,
    next_fire_at timestamptz NOT NULL,
    last_fired_at timestamptz,
    max_jitter_seconds integer NOT NULL DEFAULT 0,
    created_by_user_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_schedules_job_type
        FOREIGN KEY (job_type)
        REFERENCES job_definitions (job_type) ON DELETE RESTRICT,
    CONSTRAINT chk_job_schedules_name_not_blank
        CHECK (length(trim(name)) > 0),
    CONSTRAINT chk_job_schedules_cron_not_blank
        CHECK (length(trim(cron_expr)) > 0),
    CONSTRAINT chk_job_schedules_timezone_not_blank
        CHECK (length(trim(timezone)) > 0),
    CONSTRAINT chk_job_schedules_timezone_utc
        CHECK (timezone = 'UTC'),
    CONSTRAINT chk_job_schedules_jitter_nonnegative
        CHECK (max_jitter_seconds >= 0)
);

CREATE TRIGGER trg_job_schedules_set_updated_at
    BEFORE UPDATE ON job_schedules
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE INDEX idx_job_schedules_next_fire
    ON job_schedules (next_fire_at)
    WHERE is_active;

CREATE TABLE workflow_runs (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    workflow_type text NOT NULL,
    organization_id uuid,
    status workflow_run_status NOT NULL DEFAULT 'RUNNING',
    idempotency_key text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_workflow_runs_workflow_type_not_blank
        CHECK (length(trim(workflow_type)) > 0),
    CONSTRAINT chk_workflow_runs_idempotency_key_not_blank
        CHECK (idempotency_key IS NULL OR length(trim(idempotency_key)) > 0)
);

CREATE TRIGGER trg_workflow_runs_set_updated_at
    BEFORE UPDATE ON workflow_runs
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE UNIQUE INDEX uq_workflow_runs_type_idempotency_org
    ON workflow_runs (workflow_type, organization_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND organization_id IS NOT NULL;

CREATE UNIQUE INDEX uq_workflow_runs_type_idempotency_global
    ON workflow_runs (workflow_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND organization_id IS NULL;

CREATE INDEX idx_workflow_runs_status_created
    ON workflow_runs (status, created_at DESC);

CREATE TABLE workflow_steps (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    workflow_run_id uuid NOT NULL,
    step_key text NOT NULL,
    job_type text,
    organization_id uuid,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    priority integer,
    max_attempts integer,
    timeout_seconds integer,
    stage text,
    execution_kind workflow_step_execution_kind NOT NULL DEFAULT 'JOB',
    status workflow_step_status NOT NULL DEFAULT 'BLOCKED',
    job_id uuid,
    released_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    dependency_count_total integer NOT NULL DEFAULT 0,
    dependency_count_pending integer NOT NULL DEFAULT 0,
    dependency_count_unsatisfied integer NOT NULL DEFAULT 0,
    status_reason text,
    last_error_code text,
    last_error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_workflow_steps_workflow_run
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_steps_job_type
        FOREIGN KEY (job_type)
        REFERENCES job_definitions (job_type) ON DELETE RESTRICT,
    CONSTRAINT fk_workflow_steps_job
        FOREIGN KEY (job_id)
        REFERENCES job_queue (id) ON DELETE SET NULL,
    CONSTRAINT uq_workflow_steps_run_step_key
        UNIQUE (workflow_run_id, step_key),
    CONSTRAINT uq_workflow_steps_job_id
        UNIQUE (job_id),
    CONSTRAINT uq_workflow_steps_run_step_id
        UNIQUE (workflow_run_id, id),
    CONSTRAINT chk_workflow_steps_step_key_not_blank
        CHECK (length(trim(step_key)) > 0),
    CONSTRAINT chk_workflow_steps_job_type_not_blank
        CHECK (job_type IS NULL OR length(trim(job_type)) > 0),
    CONSTRAINT chk_workflow_steps_stage_not_blank
        CHECK (stage IS NULL OR length(trim(stage)) > 0),
    CONSTRAINT chk_workflow_steps_max_attempts_positive
        CHECK (max_attempts IS NULL OR max_attempts > 0),
    CONSTRAINT chk_workflow_steps_timeout_positive
        CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    CONSTRAINT chk_workflow_steps_dependency_counts_nonnegative
        CHECK (
            dependency_count_total >= 0
            AND dependency_count_pending >= 0
            AND dependency_count_unsatisfied >= 0
            AND dependency_count_pending <= dependency_count_total
            AND dependency_count_unsatisfied <= dependency_count_total
        ),
    CONSTRAINT chk_workflow_steps_execution_shape
        CHECK (
            (
                execution_kind = 'JOB'
                AND job_type IS NOT NULL
                AND priority IS NOT NULL
                AND max_attempts IS NOT NULL
                AND timeout_seconds IS NOT NULL
                AND stage IS NOT NULL
            )
            OR (
                execution_kind = 'EXTERNAL'
                AND job_id IS NULL
                AND job_type IS NULL
                AND priority IS NULL
                AND max_attempts IS NULL
                AND timeout_seconds IS NULL
                AND stage IS NULL
            )
        )
);

CREATE TRIGGER trg_workflow_steps_set_updated_at
    BEFORE UPDATE ON workflow_steps
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE INDEX idx_workflow_steps_run_status_created
    ON workflow_steps (workflow_run_id, status, created_at ASC);

ALTER TABLE job_queue ADD CONSTRAINT fk_job_queue_workflow_step
    FOREIGN KEY (workflow_step_id)
    REFERENCES workflow_steps (id) ON DELETE SET NULL;

ALTER TABLE job_queue ADD CONSTRAINT uq_job_queue_workflow_step_id
    UNIQUE (workflow_step_id);

CREATE TABLE workflow_step_dependencies (
    workflow_run_id uuid NOT NULL,
    prerequisite_step_id uuid NOT NULL,
    dependent_step_id uuid NOT NULL,
    release_mode workflow_dependency_release_mode NOT NULL DEFAULT 'ON_TERMINAL',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_workflow_step_dependencies
        PRIMARY KEY (workflow_run_id, prerequisite_step_id, dependent_step_id),
    CONSTRAINT fk_workflow_step_dependencies_run
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_step_dependencies_prerequisite
        FOREIGN KEY (workflow_run_id, prerequisite_step_id)
        REFERENCES workflow_steps (workflow_run_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_step_dependencies_dependent
        FOREIGN KEY (workflow_run_id, dependent_step_id)
        REFERENCES workflow_steps (workflow_run_id, id) ON DELETE CASCADE,
    CONSTRAINT chk_workflow_step_dependencies_no_self_reference
        CHECK (prerequisite_step_id <> dependent_step_id)
);

CREATE INDEX idx_workflow_step_dependencies_prerequisite
    ON workflow_step_dependencies (prerequisite_step_id);

CREATE INDEX idx_workflow_step_dependencies_dependent
    ON workflow_step_dependencies (dependent_step_id);

CREATE OR REPLACE FUNCTION enforce_workflow_job_linkage_symmetry()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    linked_job_id uuid;
    linked_workflow_step_id uuid;
    current_workflow_step_id uuid;
    current_job_id uuid;
BEGIN
    IF TG_TABLE_NAME = 'job_queue' THEN
        SELECT jq.workflow_step_id
        INTO current_workflow_step_id
        FROM job_queue jq
        WHERE jq.id = NEW.id;

        IF current_workflow_step_id IS NOT NULL THEN
            SELECT ws.job_id
            INTO linked_job_id
            FROM workflow_steps ws
            WHERE ws.id = current_workflow_step_id;

            IF linked_job_id IS DISTINCT FROM NEW.id THEN
                RAISE EXCEPTION
                    'workflow job linkage symmetry violation: job_queue.id=% job_queue.workflow_step_id=% workflow_steps.job_id=%',
                    NEW.id,
                    current_workflow_step_id,
                    linked_job_id
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'os_workflow_job_linkage_symmetry';
            END IF;
        ELSIF EXISTS (
            SELECT 1
            FROM workflow_steps ws
            WHERE ws.job_id = NEW.id
        ) THEN
            RAISE EXCEPTION
                'workflow job linkage symmetry violation: job_queue.id=% has workflow_steps.job_id reference but job_queue.workflow_step_id is NULL',
                NEW.id
                USING ERRCODE = '23514',
                      CONSTRAINT = 'os_workflow_job_linkage_symmetry';
        END IF;

        RETURN NEW;
    END IF;

    IF TG_TABLE_NAME = 'workflow_steps' THEN
        SELECT ws.job_id
        INTO current_job_id
        FROM workflow_steps ws
        WHERE ws.id = NEW.id;

        IF current_job_id IS NOT NULL THEN
            SELECT jq.workflow_step_id
            INTO linked_workflow_step_id
            FROM job_queue jq
            WHERE jq.id = current_job_id;

            IF linked_workflow_step_id IS DISTINCT FROM NEW.id THEN
                RAISE EXCEPTION
                    'workflow job linkage symmetry violation: workflow_steps.id=% workflow_steps.job_id=% job_queue.workflow_step_id=%',
                    NEW.id,
                    current_job_id,
                    linked_workflow_step_id
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'os_workflow_job_linkage_symmetry';
            END IF;
        ELSIF EXISTS (
            SELECT 1
            FROM job_queue jq
            WHERE jq.workflow_step_id = NEW.id
        ) THEN
            RAISE EXCEPTION
                'workflow job linkage symmetry violation: workflow_steps.id=% has job_queue.workflow_step_id reference but workflow_steps.job_id is NULL',
                NEW.id
                USING ERRCODE = '23514',
                      CONSTRAINT = 'os_workflow_job_linkage_symmetry';
        END IF;

        RETURN NEW;
    END IF;

    RAISE EXCEPTION
        'workflow job linkage symmetry trigger called by unsupported table: %',
        TG_TABLE_NAME
        USING ERRCODE = '23514',
              CONSTRAINT = 'os_workflow_job_linkage_symmetry_trigger_table';
END;
$$;

CREATE CONSTRAINT TRIGGER trg_job_queue_workflow_step_linkage_symmetry
AFTER INSERT OR UPDATE OF workflow_step_id ON job_queue
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION enforce_workflow_job_linkage_symmetry();

CREATE CONSTRAINT TRIGGER trg_workflow_steps_job_linkage_symmetry
AFTER INSERT OR UPDATE OF job_id ON workflow_steps
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION enforce_workflow_job_linkage_symmetry();

CREATE TABLE job_logs (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    job_id uuid NOT NULL,
    run_number integer NOT NULL DEFAULT 1,
    attempt integer,
    level text NOT NULL,
    message text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_logs_job
        FOREIGN KEY (job_id)
        REFERENCES job_queue (id) ON DELETE CASCADE,
    CONSTRAINT chk_job_logs_attempt_nonnegative
        CHECK (attempt IS NULL OR attempt >= 0),
    CONSTRAINT chk_job_logs_run_number_positive
        CHECK (run_number > 0),
    CONSTRAINT chk_job_logs_level_not_blank
        CHECK (length(trim(level)) > 0),
    CONSTRAINT chk_job_logs_message_not_blank
        CHECK (length(trim(message)) > 0)
);

CREATE INDEX idx_job_logs_job_run_id
    ON job_logs (job_id, run_number, id);

CREATE INDEX idx_job_logs_level_time
    ON job_logs (level, occurred_at DESC);

CREATE TABLE job_runtime_configs (
    job_type text PRIMARY KEY,
    schema_version integer NOT NULL,
    config jsonb NOT NULL,
    updated_by_user_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_runtime_configs_job_type
        FOREIGN KEY (job_type)
        REFERENCES job_definitions (job_type) ON DELETE CASCADE,
    CONSTRAINT chk_job_runtime_configs_schema_version_positive
        CHECK (schema_version > 0),
    CONSTRAINT chk_job_runtime_configs_config_object
        CHECK (jsonb_typeof(config) = 'object')
);

CREATE TRIGGER trg_job_runtime_configs_set_updated_at
    BEFORE UPDATE ON job_runtime_configs
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE TABLE workflow_run_mutations (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    workflow_run_id uuid NOT NULL,
    mutation_key text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    request jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_workflow_run_mutations_workflow_run
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (id) ON DELETE CASCADE,
    CONSTRAINT chk_workflow_run_mutations_mutation_key_not_blank
        CHECK (length(trim(mutation_key)) > 0)
);

CREATE TRIGGER trg_workflow_run_mutations_set_updated_at
    BEFORE UPDATE ON workflow_run_mutations
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

CREATE UNIQUE INDEX uq_workflow_run_mutations_run_mutation_key
    ON workflow_run_mutations (workflow_run_id, mutation_key);

CREATE VIEW job_metrics_rollup AS
WITH queue_counts AS (
    SELECT
        jq.organization_id,
        jq.job_type,
        COUNT(*) FILTER (WHERE jq.status = 'PENDING')::bigint AS pending_count,
        COUNT(*) FILTER (WHERE jq.status = 'LEASED')::bigint AS leased_count,
        COUNT(*) FILTER (
            WHERE jq.status = 'LEASED'
              AND jq.lease_expires_at IS NOT NULL
              AND jq.lease_expires_at < now()
        )::bigint AS stale_leases
    FROM job_queue jq
    GROUP BY jq.organization_id, jq.job_type
),
recent_attempts AS (
    SELECT
        jq.organization_id,
        jq.job_type,
        ja.outcome,
        CASE
            WHEN ja.finished_at IS NOT NULL THEN extract(epoch FROM (ja.finished_at - ja.started_at)) * 1000.0
            ELSE NULL
        END AS duration_ms
    FROM job_attempts ja
    JOIN job_queue jq ON jq.id = ja.job_id
    WHERE ja.created_at >= now() - interval '24 hours'
),
attempt_rollup AS (
    SELECT
        ra.organization_id,
        ra.job_type,
        COUNT(*) FILTER (WHERE ra.outcome = 'RETRYABLE')::bigint AS retryable_24h,
        COUNT(*) FILTER (WHERE ra.outcome = 'TERMINAL')::bigint AS terminal_24h,
        COUNT(*) FILTER (WHERE ra.outcome = 'PANICKED')::bigint AS panicked_24h,
        COUNT(*) FILTER (WHERE ra.outcome = 'TIMEOUT')::bigint AS timeout_24h,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY ra.duration_ms) AS p50_duration_ms_24h,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY ra.duration_ms) AS p95_duration_ms_24h
    FROM recent_attempts ra
    WHERE ra.duration_ms IS NOT NULL
    GROUP BY ra.organization_id, ra.job_type
),
dead_letter_rollup AS (
    SELECT
        jdl.organization_id,
        jdl.job_type,
        COUNT(*)::bigint AS dead_lettered_24h
    FROM job_dead_letters jdl
    WHERE jdl.failed_at >= now() - interval '24 hours'
    GROUP BY jdl.organization_id, jdl.job_type
),
succeeded_rollup AS (
    SELECT
        jq.organization_id,
        jq.job_type,
        COUNT(*)::bigint AS succeeded_24h
    FROM job_events je
    JOIN job_queue jq ON jq.id = je.job_id
    WHERE je.event_type = 'SUCCEEDED'
      AND je.occurred_at >= now() - interval '24 hours'
    GROUP BY jq.organization_id, jq.job_type
),
job_keys AS (
    SELECT qc.organization_id, qc.job_type
    FROM queue_counts qc
    UNION
    SELECT ar.organization_id, ar.job_type
    FROM attempt_rollup ar
    UNION
    SELECT dl.organization_id, dl.job_type
    FROM dead_letter_rollup dl
    UNION
    SELECT sr.organization_id, sr.job_type
    FROM succeeded_rollup sr
)
SELECT
    jk.organization_id,
    jk.job_type,
    COALESCE(qc.pending_count, 0) AS pending_count,
    COALESCE(qc.leased_count, 0) AS leased_count,
    COALESCE(qc.stale_leases, 0) AS stale_leases,
    COALESCE(sr.succeeded_24h, 0) AS succeeded_24h,
    COALESCE(ar.retryable_24h, 0) AS retryable_24h,
    COALESCE(ar.terminal_24h, 0) AS terminal_24h,
    COALESCE(ar.panicked_24h, 0) AS panicked_24h,
    COALESCE(ar.timeout_24h, 0) AS timeout_24h,
    COALESCE(dl.dead_lettered_24h, 0) AS dead_lettered_24h,
    ar.p50_duration_ms_24h,
    ar.p95_duration_ms_24h
FROM job_keys jk
LEFT JOIN queue_counts qc
    ON qc.organization_id IS NOT DISTINCT FROM jk.organization_id
   AND qc.job_type = jk.job_type
LEFT JOIN attempt_rollup ar
    ON ar.organization_id IS NOT DISTINCT FROM jk.organization_id
   AND ar.job_type = jk.job_type
LEFT JOIN dead_letter_rollup dl
    ON dl.organization_id IS NOT DISTINCT FROM jk.organization_id
   AND dl.job_type = jk.job_type
LEFT JOIN succeeded_rollup sr
    ON sr.organization_id IS NOT DISTINCT FROM jk.organization_id
   AND sr.job_type = jk.job_type;
