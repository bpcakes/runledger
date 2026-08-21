SELECT
    step.id,
    step.workflow_run_id,
    step.step_key,
    step.execution_kind::text AS "execution_kind!",
    step.job_type,
    step.organization_id,
    step.payload,
    step.priority,
    step.max_attempts,
    step.timeout_seconds,
    step.stage,
    step.allow_handler_continuation,
    step.execution_resource_key,
    step.status::text AS "status!",
    step.job_id,
    step.released_at,
    step.started_at,
    step.finished_at,
    COALESCE(prerequisites.visible_total, 0)::int4
        AS "visible_dependency_count_total!",
    COALESCE(prerequisites.visible_pending, 0)::int4
        AS "visible_dependency_count_pending!",
    COALESCE(prerequisites.visible_unsatisfied, 0)::int4
        AS "visible_dependency_count_unsatisfied!",
    COALESCE(prerequisites.has_hidden, false)
        AS "has_hidden_prerequisites!",
    step.status_reason,
    step.last_error_code,
    step.last_error_message,
    step.output,
    step.created_at,
    step.updated_at
FROM workflow_steps step
JOIN workflow_runs workflow
  ON workflow.id = step.workflow_run_id
 AND workflow.organization_id = $2
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (
            WHERE prerequisite.organization_id = $2
        )::int4 AS visible_total,
        COUNT(*) FILTER (
            WHERE prerequisite.organization_id = $2
              AND prerequisite.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        )::int4 AS visible_pending,
        COUNT(*) FILTER (
            WHERE prerequisite.organization_id = $2
              AND dependency.release_mode = 'ON_SUCCESS'
              AND prerequisite.status IN ('FAILED', 'CANCELED')
        )::int4 AS visible_unsatisfied,
        BOOL_OR(prerequisite.organization_id IS DISTINCT FROM $2) AS has_hidden
    FROM workflow_step_dependencies dependency
    JOIN workflow_steps prerequisite
      ON prerequisite.id = dependency.prerequisite_step_id
    WHERE dependency.dependent_step_id = step.id
) prerequisites ON true
WHERE step.workflow_run_id = $1
  AND step.organization_id = $2
ORDER BY step.created_at ASC, step.id ASC
LIMIT $3 OFFSET $4
