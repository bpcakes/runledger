-- Expand phase for making workflow_steps.job_id the only authoritative
-- workflow-step/job relationship. During this phase the deprecated
-- job_queue.workflow_step_id column remains as a trigger-maintained projection
-- so the previous dual-column binary and the new single-source binary can run
-- concurrently.
LOCK TABLE job_queue, workflow_steps IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    conflicting_link_count bigint;
    inconsistent_link_count bigint;
BEGIN
    SELECT count(*)
    INTO conflicting_link_count
    FROM (
        SELECT jq.id
        FROM job_queue jq
        JOIN workflow_steps ws ON ws.id = jq.workflow_step_id
        WHERE jq.workflow_step_id IS NOT NULL
          AND ws.job_id IS NOT NULL
          AND ws.job_id <> jq.id

        UNION ALL

        SELECT ws.id
        FROM workflow_steps ws
        JOIN job_queue jq ON jq.id = ws.job_id
        WHERE ws.job_id IS NOT NULL
          AND jq.workflow_step_id IS NOT NULL
          AND jq.workflow_step_id <> ws.id
    ) conflicts;

    IF conflicting_link_count <> 0 THEN
        RAISE EXCEPTION
            'workflow job linkage expand audit found % conflicting reciprocal relationships',
            conflicting_link_count
            USING ERRCODE = '23514',
                  CONSTRAINT = 'os_workflow_job_linkage_expand_audit';
    END IF;

    -- Repair either one-sided shape before replacing strict symmetry checks
    -- with compatibility projection triggers. The conflict audit above keeps
    -- this backfill from choosing between two non-null relationships.
    UPDATE workflow_steps ws
    SET job_id = jq.id,
        updated_at = now()
    FROM job_queue jq
    WHERE jq.workflow_step_id = ws.id
      AND ws.job_id IS NULL;

    UPDATE job_queue jq
    SET workflow_step_id = ws.id,
        updated_at = now()
    FROM workflow_steps ws
    WHERE ws.job_id = jq.id
      AND jq.workflow_step_id IS NULL;

    SELECT count(*)
    INTO inconsistent_link_count
    FROM (
        SELECT jq.id
        FROM job_queue jq
        WHERE jq.workflow_step_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_steps ws
              WHERE ws.id = jq.workflow_step_id
                AND ws.job_id = jq.id
          )

        UNION ALL

        SELECT ws.id
        FROM workflow_steps ws
        WHERE ws.job_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM job_queue jq
              WHERE jq.id = ws.job_id
                AND jq.workflow_step_id = ws.id
          )
    ) inconsistencies;

    IF inconsistent_link_count <> 0 THEN
        RAISE EXCEPTION
            'workflow job linkage expand audit found % inconsistent reciprocal relationships after backfill',
            inconsistent_link_count
            USING ERRCODE = '23514',
                  CONSTRAINT = 'os_workflow_job_linkage_expand_audit';
    END IF;
END;
$$;

-- Keep the released binary's deferred symmetry checks throughout the expand
-- window. The additional one-way projection lets the new binary write only
-- workflow_steps.job_id without changing the old binary's insert-then-update
-- protocol.
CREATE FUNCTION project_workflow_step_job_linkage_compatibility()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME <> 'workflow_steps' THEN
        RAISE EXCEPTION
            'workflow job linkage compatibility trigger called by unsupported table: %',
            TG_TABLE_NAME
            USING ERRCODE = '23514',
                  CONSTRAINT = 'os_workflow_job_linkage_compatibility_trigger_table';
    END IF;

    IF TG_OP = 'UPDATE'
       AND OLD.job_id IS DISTINCT FROM NEW.job_id
       AND OLD.job_id IS NOT NULL THEN
        UPDATE job_queue
        SET workflow_step_id = NULL,
            updated_at = now()
        WHERE id = OLD.job_id
          AND workflow_step_id = NEW.id;
    END IF;

    IF NEW.job_id IS NOT NULL THEN
        UPDATE job_queue
        SET workflow_step_id = NEW.id,
            updated_at = now()
        WHERE id = NEW.job_id
          AND workflow_step_id IS DISTINCT FROM NEW.id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_workflow_steps_job_linkage_compatibility
AFTER INSERT OR UPDATE OF job_id ON workflow_steps
FOR EACH ROW
EXECUTE FUNCTION project_workflow_step_job_linkage_compatibility();

-- This expand migration is deliberately absent from
-- runledger_migration_history. SQLx records it in _sqlx_migrations, while the
-- custom compatibility fence lets the preceding released binary coexist with
-- the projection column during the rolling deployment window.
