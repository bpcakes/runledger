LOCK TABLE job_queue, workflow_steps IN ACCESS EXCLUSIVE MODE;

DELETE FROM runledger_migration_history
WHERE version = 202608240002;

ALTER TABLE job_queue ADD COLUMN workflow_step_id uuid;

UPDATE job_queue jq
SET workflow_step_id = ws.id,
    updated_at = now()
FROM workflow_steps ws
WHERE ws.job_id = jq.id;

ALTER TABLE job_queue ADD CONSTRAINT fk_job_queue_workflow_step
    FOREIGN KEY (workflow_step_id)
    REFERENCES workflow_steps (id) ON DELETE SET NULL;

ALTER TABLE job_queue ADD CONSTRAINT uq_job_queue_workflow_step_id
    UNIQUE (workflow_step_id);

CREATE FUNCTION enforce_workflow_job_linkage_symmetry()
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
