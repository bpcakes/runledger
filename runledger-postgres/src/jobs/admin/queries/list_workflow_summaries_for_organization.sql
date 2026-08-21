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
WHERE organization_id = $1
  AND ($2::text IS NULL OR status = $2::text::workflow_run_status)
  AND ($3::text IS NULL OR workflow_type ILIKE '%' || $3 || '%')
ORDER BY created_at DESC, id DESC
LIMIT $4
OFFSET $5
