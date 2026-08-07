use std::collections::BTreeSet;

use runledger_core::jobs::WorkflowRunStatus;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::super::row_decode::parse_workflow_run_status;
use super::super::active_claims::release_or_defer_workflow_active_claim_tx;

pub(crate) const WORKFLOW_RUN_TERMINAL_CHANNEL: &str = "runledger_workflow_run_terminal";

pub(crate) async fn notify_workflow_run_terminal_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    status: WorkflowRunStatus,
) -> Result<()> {
    sqlx::query!(
        "SELECT pg_notify(
            $1,
            json_build_object(
                'workflow_run_id', $2::uuid::text,
                'status', $3::text
            )::text
         )",
        WORKFLOW_RUN_TERMINAL_CHANNEL,
        workflow_run_id,
        status.as_db_value(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("notify workflow run terminal", error))?;

    Ok(())
}

pub(crate) async fn recompute_workflow_run_statuses_tx(
    tx: &mut DbTx<'_>,
    touched_run_ids: &BTreeSet<Uuid>,
) -> Result<()> {
    for workflow_run_id in touched_run_ids {
        // Serialize status recomputation per run so concurrent step completions do not
        // leave the run stuck in RUNNING due to snapshot races.
        let run_status = sqlx::query!(
            "SELECT status::text AS \"status!\"
             FROM workflow_runs
             WHERE id = $1
             FOR UPDATE",
            *workflow_run_id
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock workflow run for status recompute", error)
        })?;
        let Some(run_status) = run_status else {
            continue;
        };
        let previous_status = parse_workflow_run_status(run_status.status)?;

        let row = sqlx::query!(
            "SELECT
                COUNT(*) FILTER (
                    WHERE status IN ('BLOCKED', 'ENQUEUED', 'RUNNING')
                )::bigint AS \"active_steps!\",
                COUNT(*) FILTER (
                    WHERE status = 'WAITING_FOR_EXTERNAL'
                )::bigint AS \"waiting_steps!\",
                COUNT(*) FILTER (
                    WHERE status IN ('FAILED', 'CANCELED')
                )::bigint AS \"errored_steps!\"
             FROM workflow_steps
             WHERE workflow_run_id = $1",
            *workflow_run_id,
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("recompute workflow run status counters", error)
        })?;

        let active_steps: i64 = row.active_steps;
        let waiting_steps: i64 = row.waiting_steps;
        let errored_steps: i64 = row.errored_steps;

        let next_status = if active_steps > 0 {
            WorkflowRunStatus::Running
        } else if waiting_steps > 0 {
            WorkflowRunStatus::WaitingForExternal
        } else if errored_steps > 0 {
            WorkflowRunStatus::CompletedWithErrors
        } else {
            WorkflowRunStatus::Succeeded
        };

        if previous_status == next_status && next_status.is_terminal() {
            continue;
        }

        let updated = sqlx::query!(
            "UPDATE workflow_runs
             SET status = $2::text::workflow_run_status,
                 finished_at = CASE
                    WHEN $2::text::workflow_run_status IN ('RUNNING', 'WAITING_FOR_EXTERNAL')
                        THEN NULL
                    ELSE COALESCE(finished_at, now())
                 END,
                 result = CASE
                    WHEN $2::text::workflow_run_status = 'SUCCEEDED' THEN (
                        SELECT ws.output
                        FROM workflow_steps ws
                        WHERE ws.workflow_run_id = workflow_runs.id
                          AND ws.step_key = workflow_runs.result_step_key
                          AND ws.status = 'SUCCEEDED'
                        LIMIT 1
                    )
                    WHEN $2::text::workflow_run_status IN ('COMPLETED_WITH_ERRORS', 'CANCELED')
                        THEN NULL
                    ELSE result
                 END,
                 updated_at = now()
             WHERE id = $1
               AND status <> 'CANCELED'",
            *workflow_run_id,
            next_status.as_db_value(),
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("update workflow run recomputed status", error)
        })?;

        if updated.rows_affected() > 0
            && next_status.is_terminal()
            && !previous_status.is_terminal()
        {
            release_or_defer_workflow_active_claim_tx(tx, *workflow_run_id).await?;
            notify_workflow_run_terminal_tx(tx, *workflow_run_id, next_status).await?;
        }
    }

    Ok(())
}
