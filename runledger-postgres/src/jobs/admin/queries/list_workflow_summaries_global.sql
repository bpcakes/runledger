SELECT
    id,
    workflow_type,
    organization_id,
    status::text AS "status!",
    result_step_key,
    started_at,
    finished_at,
    created_at,
    updated_at
FROM workflow_runs
WHERE ($1::text IS NULL OR status = $1::text::workflow_run_status)
  AND ($2::text IS NULL OR workflow_type ILIKE '%' || $2 || '%')
ORDER BY created_at DESC, id DESC
LIMIT $3
OFFSET $4
