SELECT
    id,
    job_type,
    organization_id,
    status::text AS "status!",
    priority,
    run_number,
    attempt,
    max_attempts,
    timeout_seconds,
    next_run_at,
    lease_expires_at,
    last_heartbeat_at,
    started_at,
    finished_at,
    stage,
    progress_done,
    progress_total,
    progress_pct::float8 AS progress_pct,
    last_error_code,
    created_at,
    updated_at
FROM job_queue
WHERE organization_id = $1
  AND ($2::text::job_status IS NULL OR status = $2::text::job_status)
  AND ($3::text IS NULL OR job_type ILIKE '%' || $3 || '%')
ORDER BY created_at DESC, id DESC
LIMIT $4
OFFSET $5
