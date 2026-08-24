WITH eligible_resource_jobs AS MATERIALIZED (
    SELECT
        jq.id,
        jq.execution_resource_key,
        jq.priority,
        jq.next_run_at,
        jq.created_at
    FROM job_queue jq
    WHERE jq.status = 'PENDING'
      AND jq.next_run_at <= now()
      AND ($4::text[] IS NULL OR jq.job_type = ANY($4::text[]))
      AND jq.execution_resource_key IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM job_execution_resource_claims rc
          WHERE rc.resource_key = jq.execution_resource_key
      )
    ORDER BY
        jq.priority DESC,
        jq.next_run_at ASC,
        jq.created_at ASC,
        jq.id ASC
    LIMIT $5
),
resource_heads AS MATERIALIZED (
    SELECT DISTINCT ON (eligible.execution_resource_key)
        eligible.id
    FROM eligible_resource_jobs eligible
    ORDER BY
        eligible.execution_resource_key,
        eligible.priority DESC,
        eligible.next_run_at ASC,
        eligible.created_at ASC,
        eligible.id ASC
),
candidates AS MATERIALIZED (
    SELECT
        jq.id,
        jq.execution_resource_key,
        jq.run_number,
        jq.attempt,
        jq.priority,
        jq.next_run_at,
        jq.created_at
    FROM job_queue jq
    WHERE jq.status = 'PENDING'
      AND jq.next_run_at <= now()
      AND ($4::text[] IS NULL OR jq.job_type = ANY($4::text[]))
      AND (
          jq.execution_resource_key IS NULL
          OR (
              NOT EXISTS (
                  SELECT 1
                  FROM job_execution_resource_claims rc
                  WHERE rc.resource_key = jq.execution_resource_key
              )
              AND jq.id IN (SELECT id FROM resource_heads)
          )
      )
    ORDER BY
        jq.priority DESC,
        jq.next_run_at ASC,
        jq.created_at ASC,
        jq.id ASC
    FOR UPDATE OF jq SKIP LOCKED
    LIMIT $1
),
acquired AS (
    INSERT INTO job_execution_resource_claims (
        resource_key,
        job_id,
        run_number,
        attempt,
        worker_id,
        lease_expires_at
    )
    SELECT
        execution_resource_key,
        id,
        run_number,
        attempt + 1,
        $2,
        now() + make_interval(secs => $3::int4)
    FROM candidates
    WHERE execution_resource_key IS NOT NULL
    ORDER BY execution_resource_key
    ON CONFLICT DO NOTHING
    RETURNING job_id
)
SELECT c.id AS "id!"
FROM candidates c
LEFT JOIN acquired a ON a.job_id = c.id
WHERE c.execution_resource_key IS NULL OR a.job_id IS NOT NULL
ORDER BY
    c.priority DESC,
    c.next_run_at ASC,
    c.created_at ASC,
    c.id ASC
