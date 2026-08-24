-- Contract phase. Apply only after every application instance uses
-- workflow_steps.job_id for reads and writes and the expand-phase anti-joins
-- remain empty. This migration is compatibility-fenced below so older
-- Runledger startup guards reject the destructive schema.
LOCK TABLE job_queue, workflow_steps IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    inconsistent_link_count bigint;
BEGIN
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
            'workflow job linkage contract audit found % inconsistent reciprocal relationships',
            inconsistent_link_count
            USING ERRCODE = '23514',
                  CONSTRAINT = 'os_workflow_job_linkage_contract_audit';
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_workflow_steps_job_linkage_compatibility ON workflow_steps;
DROP FUNCTION IF EXISTS project_workflow_step_job_linkage_compatibility();
DROP TRIGGER IF EXISTS trg_job_queue_workflow_step_linkage_symmetry ON job_queue;
DROP TRIGGER IF EXISTS trg_workflow_steps_job_linkage_symmetry ON workflow_steps;
DROP FUNCTION IF EXISTS enforce_workflow_job_linkage_symmetry();

ALTER TABLE job_queue DROP CONSTRAINT fk_job_queue_workflow_step;
ALTER TABLE job_queue DROP CONSTRAINT uq_job_queue_workflow_step_id;
ALTER TABLE job_queue DROP COLUMN workflow_step_id;

INSERT INTO runledger_migration_history (version)
VALUES (202608240002)
ON CONFLICT (version) DO NOTHING;
