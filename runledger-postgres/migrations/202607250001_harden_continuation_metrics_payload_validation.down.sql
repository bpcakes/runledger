CREATE OR REPLACE VIEW job_continuation_metrics_rollup AS
SELECT
    continuation_metrics.organization_id,
    continuation_metrics.job_type,
    SUM(continuation_metrics.continued_24h)::bigint AS continued_24h,
    SUM(continuation_metrics.active_continued_count)::bigint
        AS active_continued_count,
    MAX(continuation_metrics.max_active_run_number)::int4
        AS max_active_run_number
FROM (
    SELECT
        jq.organization_id,
        jq.job_type,
        COUNT(*)::bigint AS continued_24h,
        0::bigint AS active_continued_count,
        0::int4 AS max_active_run_number
    FROM job_events je
    JOIN job_queue jq ON jq.id = je.job_id
    WHERE je.event_type = 'REQUEUED'
      AND je.occurred_at >= now() - interval '24 hours'
      AND je.payload ?& ARRAY[
          'next_run_number',
          'next_run_at',
          'delay_microseconds'
      ]
      AND (
          je.payload ->> 'requeue_kind' = 'HANDLER_CONTINUATION'
          OR (
              NOT (je.payload ? 'requeue_kind')
              AND je.payload ->> 'reason' = 'HANDLER_CONTINUATION'
          )
      )
    GROUP BY jq.organization_id, jq.job_type

    UNION ALL

    SELECT
        jq.organization_id,
        jq.job_type,
        0::bigint AS continued_24h,
        COUNT(*)::bigint AS active_continued_count,
        MAX(jq.run_number)::int4 AS max_active_run_number
    FROM job_queue jq
    WHERE jq.status IN ('PENDING', 'LEASED')
      AND EXISTS (
          SELECT 1
          FROM job_events je
          WHERE je.job_id = jq.id
            AND je.run_number = jq.run_number - 1
            AND je.event_type = 'REQUEUED'
            AND je.payload ?& ARRAY[
                'next_run_number',
                'next_run_at',
                'delay_microseconds'
            ]
            AND (
                je.payload ->> 'requeue_kind' = 'HANDLER_CONTINUATION'
                OR (
                    NOT (je.payload ? 'requeue_kind')
                    AND je.payload ->> 'reason' = 'HANDLER_CONTINUATION'
                )
            )
            AND je.payload -> 'next_run_number' = to_jsonb(jq.run_number)
      )
    GROUP BY jq.organization_id, jq.job_type
) continuation_metrics
GROUP BY
    continuation_metrics.organization_id,
    continuation_metrics.job_type;
