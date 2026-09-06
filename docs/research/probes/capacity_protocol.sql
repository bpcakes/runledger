-- Disposable research schema layered on ALL current Runledger migrations.
-- This is an executable protocol model, not a production migration.
CREATE TABLE capacity_probe_policies (
    id integer PRIMARY KEY,
    capacity integer NOT NULL CHECK (capacity > 0)
);
CREATE TABLE capacity_probe_requirements (
    job_id uuid REFERENCES job_queue(id) ON DELETE CASCADE,
    policy_id integer REFERENCES capacity_probe_policies(id),
    PRIMARY KEY (job_id, policy_id)
);
ALTER TABLE job_queue ADD COLUMN capacity_probe_admission uuid;
CREATE TABLE capacity_probe_permits (
    policy_id integer REFERENCES capacity_probe_policies(id),
    admission_id uuid NOT NULL,
    job_id uuid NOT NULL REFERENCES job_queue(id) ON DELETE RESTRICT,
    run_number integer NOT NULL,
    attempt integer NOT NULL,
    worker_id text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    release_after timestamptz,
    PRIMARY KEY (policy_id, admission_id)
);
CREATE INDEX ON capacity_probe_permits(job_id);
CREATE FUNCTION capacity_probe_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'LEASED' AND EXISTS (
        SELECT 1 FROM capacity_probe_requirements r
        WHERE r.job_id = NEW.id AND NOT EXISTS (
            SELECT 1 FROM capacity_probe_permits p
            WHERE p.job_id = NEW.id AND p.policy_id = r.policy_id
              AND p.admission_id = NEW.capacity_probe_admission
              AND p.run_number = NEW.run_number AND p.attempt = NEW.attempt
              AND p.worker_id = NEW.worker_id
              AND p.lease_expires_at = NEW.lease_expires_at
              AND p.release_after IS NULL
        )
    ) THEN
        RAISE EXCEPTION 'missing capacity admission' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER capacity_probe_guard BEFORE INSERT OR UPDATE OF status ON job_queue
    FOR EACH ROW EXECUTE FUNCTION capacity_probe_guard();
CREATE FUNCTION capacity_probe_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'LEASED' AND OLD.capacity_probe_admission IS NOT NULL THEN
        IF NEW.status <> 'LEASED'
           OR NEW.capacity_probe_admission IS DISTINCT FROM OLD.capacity_probe_admission
           OR NEW.run_number <> OLD.run_number OR NEW.attempt <> OLD.attempt
           OR NEW.worker_id IS DISTINCT FROM OLD.worker_id THEN
            IF NEW.status = 'CANCELED' AND OLD.lease_expires_at > clock_timestamp() THEN
                UPDATE capacity_probe_permits SET release_after = OLD.lease_expires_at
                WHERE job_id = OLD.id AND admission_id = OLD.capacity_probe_admission;
            ELSE
                DELETE FROM capacity_probe_permits
                WHERE job_id = OLD.id AND admission_id = OLD.capacity_probe_admission;
            END IF;
        ELSE
            UPDATE capacity_probe_permits SET lease_expires_at = NEW.lease_expires_at
            WHERE job_id = OLD.id AND admission_id = OLD.capacity_probe_admission;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER capacity_probe_lifecycle AFTER UPDATE ON job_queue
    FOR EACH ROW EXECUTE FUNCTION capacity_probe_lifecycle();

-- The caller creates a SAVEPOINT before this function. Explicit lock acquisition
-- uses NOWAIT. The caller caps implicit FK/unique-index waits with lock_timeout.
CREATE FUNCTION capacity_probe_claim(target uuid, owner text) RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    j job_queue%ROWTYPE;
    locked_policy capacity_probe_policies%ROWTYPE;
    admission uuid := uuidv7();
    deadline timestamptz;
BEGIN
    SELECT * INTO j FROM job_queue WHERE id = target AND status = 'PENDING'
        AND next_run_at <= clock_timestamp() FOR UPDATE NOWAIT;
    IF NOT FOUND THEN RETURN NULL; END IF;
    -- Matches on_claimed's job -> step order; do not wait holding policy locks.
    PERFORM id FROM workflow_steps WHERE job_id = target FOR UPDATE NOWAIT;
    FOR locked_policy IN SELECT cp.* FROM capacity_probe_policies cp
        JOIN capacity_probe_requirements r ON r.policy_id = cp.id
        WHERE r.job_id = target ORDER BY cp.id
        FOR NO KEY UPDATE OF cp NOWAIT
    LOOP
        NULL;
    END LOOP;
    -- A separate command in this VOLATILE function obtains a new RC snapshot.
    IF EXISTS (
        SELECT 1 FROM capacity_probe_requirements r
        JOIN capacity_probe_policies cp ON cp.id = r.policy_id
        WHERE r.job_id = target AND (
            SELECT count(*) FROM capacity_probe_permits p WHERE p.policy_id = cp.id
        ) >= cp.capacity
    ) THEN RETURN NULL; END IF;
    deadline := clock_timestamp() + interval '60 seconds';
    IF j.execution_resource_key IS NOT NULL THEN
        INSERT INTO job_execution_resource_claims (
            resource_key, job_id, run_number, attempt, worker_id, lease_expires_at
        ) VALUES (j.execution_resource_key, j.id, j.run_number, j.attempt + 1, owner, deadline)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN RETURN NULL; END IF;
    END IF;
    INSERT INTO capacity_probe_permits (
        policy_id, admission_id, job_id, run_number, attempt, worker_id, lease_expires_at
    ) SELECT policy_id, admission, j.id, j.run_number, j.attempt + 1, owner, deadline
      FROM capacity_probe_requirements WHERE job_id = j.id;
    UPDATE job_queue SET status = 'LEASED', attempt = attempt + 1, worker_id = owner,
        lease_expires_at = deadline, capacity_probe_admission = admission,
        last_heartbeat_at = clock_timestamp(), started_at = COALESCE(started_at, now())
        WHERE id = target;
    UPDATE workflow_steps SET status = 'RUNNING', started_at = COALESCE(started_at, now())
        WHERE job_id = target AND status IN ('ENQUEUED', 'RUNNING');
    INSERT INTO job_attempts(job_id, run_number, attempt, worker_id, leased_at, started_at, claim_origin)
        VALUES (target, j.run_number, j.attempt + 1, owner, now(), now(), 'WORKER_PRESTART');
    INSERT INTO job_events(job_id, run_number, attempt, event_type, payload)
        VALUES (target, j.run_number, j.attempt + 1, 'LEASED',
                jsonb_build_object('admission_id', admission));
    RETURN admission;
END $$;
