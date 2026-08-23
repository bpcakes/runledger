SELECT
    step.id,
    step.workflow_run_id,
    step.step_key,
    step.execution_kind::text AS "execution_kind!",
    step.job_type,
    step.organization_id,
    step.priority,
    step.max_attempts,
    step.timeout_seconds,
    step.stage,
    step.allow_handler_continuation,
    step.status::text AS "status!",
    step.job_id,
    step.released_at,
    step.started_at,
    step.finished_at,
    step.dependency_count_total AS "visible_dependency_count_total!",
    step.dependency_count_pending AS "visible_dependency_count_pending!",
    step.dependency_count_unsatisfied AS "visible_dependency_count_unsatisfied!",
    false AS "has_hidden_prerequisites!",
    step.last_error_code,
    step.created_at,
    step.updated_at
FROM workflow_steps step
WHERE step.workflow_run_id = $1
ORDER BY step.created_at ASC, step.id ASC
LIMIT $2 OFFSET $3
