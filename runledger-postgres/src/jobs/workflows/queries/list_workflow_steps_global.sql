SELECT
    ws.id,
    ws.workflow_run_id,
    ws.step_key,
    ws.execution_kind::text AS "execution_kind!",
    ws.job_type,
    ws.organization_id,
    ws.payload,
    ws.priority,
    ws.max_attempts,
    ws.timeout_seconds,
    ws.stage,
    ws.allow_handler_continuation,
    ws.execution_resource_key,
    ws.status::text AS "status!",
    ws.job_id,
    ws.released_at,
    ws.started_at,
    ws.finished_at,
    ws.dependency_count_total,
    ws.dependency_count_pending,
    ws.dependency_count_unsatisfied,
    ws.status_reason,
    ws.last_error_code,
    ws.last_error_message,
    ws.output,
    ws.created_at,
    ws.updated_at
FROM workflow_steps ws
WHERE ws.workflow_run_id = $1
ORDER BY ws.created_at ASC, ws.id ASC
LIMIT $2 OFFSET $3
