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
    COALESCE(visible_dependencies.total, 0)::int4 AS "dependency_count_total!",
    COALESCE(visible_dependencies.pending, 0)::int4 AS "dependency_count_pending!",
    COALESCE(visible_dependencies.unsatisfied, 0)::int4 AS "dependency_count_unsatisfied!",
    ws.status_reason,
    ws.last_error_code,
    ws.last_error_message,
    ws.output,
    ws.created_at,
    ws.updated_at
FROM workflow_steps ws
JOIN workflow_runs wr
  ON wr.id = ws.workflow_run_id
 AND wr.organization_id = $2
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::int4 AS total,
        COUNT(*) FILTER (
            WHERE prerequisite.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        )::int4 AS pending,
        COUNT(*) FILTER (
            WHERE dependency.release_mode = 'ON_SUCCESS'
              AND prerequisite.status IN ('FAILED', 'CANCELED')
        )::int4 AS unsatisfied
    FROM workflow_step_dependencies dependency
    JOIN workflow_steps prerequisite
      ON prerequisite.id = dependency.prerequisite_step_id
     AND prerequisite.organization_id = $2
    WHERE dependency.dependent_step_id = ws.id
) visible_dependencies ON true
WHERE ws.workflow_run_id = $1
  AND ws.organization_id = $2
ORDER BY ws.created_at ASC, ws.id ASC
LIMIT $3 OFFSET $4
