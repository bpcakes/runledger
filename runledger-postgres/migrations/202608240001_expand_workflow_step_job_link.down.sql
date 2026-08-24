LOCK TABLE job_queue, workflow_steps IN SHARE ROW EXCLUSIVE MODE;

DROP TRIGGER IF EXISTS trg_workflow_steps_job_linkage_compatibility ON workflow_steps;
DROP FUNCTION IF EXISTS project_workflow_step_job_linkage_compatibility();
