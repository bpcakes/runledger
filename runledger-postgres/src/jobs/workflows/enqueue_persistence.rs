use runledger_core::jobs::WorkflowRunEnqueue;
use serde_json::Value as JsonValue;

use super::super::rows::WorkflowRunEnqueueRow;
use super::errors::{
    workflow_enqueue_conflicting_retry_error, workflow_internal_state_error,
    workflow_legacy_idempotency_snapshot_missing_error,
};
use crate::{DbTx, Error, Result};

pub(super) struct WorkflowRunInsertOutcome {
    pub(super) record: super::super::workflow_types::WorkflowRunDbRecord,
    pub(super) inserted: bool,
}

pub(super) async fn insert_workflow_run_record_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    enqueue_request: &JsonValue,
) -> Result<WorkflowRunInsertOutcome> {
    // Every new workflow stores its canonical request so a future recovery can
    // reconstruct the original DAG safely. Idempotency still only applies when
    // an idempotency key is present.
    // The conflict clause is selected from static literals only; all request
    // data remains bound below. This dynamic SQL is not SQLx macro-checked, so
    // keep the returned columns and bind order aligned with WorkflowRunEnqueueRow
    // and the workflow_runs insert list.
    let insert_sql = format!(
        "INSERT INTO workflow_runs (
            workflow_type,
            organization_id,
            status,
            idempotency_key,
            result_step_key,
            metadata,
            enqueue_request,
            started_at
         )
         VALUES ($1, $2, 'RUNNING', $3, $4, $5::jsonb, $6::jsonb, now())
         {}
         RETURNING
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            idempotency_key,
            result_step_key,
            metadata,
            NULL::boolean AS enqueue_request_matches,
            started_at,
            finished_at,
            created_at,
            updated_at",
        enqueue_workflow_run_idempotency_conflict_clause(payload),
    );
    let run_row = sqlx::query_as::<_, WorkflowRunEnqueueRow>(&insert_sql)
        .bind(payload.workflow_type())
        .bind(payload.organization_id())
        .bind(payload.idempotency_key())
        .bind(payload.result_step_key().map(|step_key| step_key.as_str()))
        .bind(payload.metadata())
        .bind(enqueue_request)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("enqueue workflow run", error))?;

    if let Some(run_row) = run_row {
        return Ok(WorkflowRunInsertOutcome {
            record: run_row.into_record()?,
            inserted: true,
        });
    }

    let Some(idempotency_key) = payload.idempotency_key() else {
        return Err(workflow_internal_state_error(
            "workflow run insert returned no row without an idempotency key conflict",
        ));
    };

    let existing =
        load_existing_idempotent_workflow_run_tx(tx, payload, idempotency_key, enqueue_request)
            .await?;
    validate_existing_idempotent_workflow_run(&existing)?;

    Ok(WorkflowRunInsertOutcome {
        record: existing.into_record()?,
        inserted: false,
    })
}

async fn load_existing_idempotent_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &JsonValue,
) -> Result<WorkflowRunEnqueueRow> {
    try_load_existing_idempotent_workflow_run_tx(tx, payload, idempotency_key, enqueue_request)
        .await?
        .ok_or_else(|| {
            workflow_internal_state_error(
                "workflow run insert conflicted but matching idempotent workflow run was not found",
            )
        })
}

pub(super) async fn try_load_existing_idempotent_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &JsonValue,
) -> Result<Option<WorkflowRunEnqueueRow>> {
    // Lock a matched committed row while the enqueue transaction compares and
    // returns the idempotent result.
    let run = if let Some(organization_id) = payload.organization_id() {
        sqlx::query_as!(
            WorkflowRunEnqueueRow,
            r#"SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS "status!",
                idempotency_key,
                result_step_key,
                metadata,
                enqueue_request = $4::jsonb AS "enqueue_request_matches?",
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR SHARE"#,
            payload.workflow_type() as _,
            organization_id,
            idempotency_key,
            enqueue_request,
        )
        .fetch_optional(&mut **tx)
        .await
    } else {
        sqlx::query_as!(
            WorkflowRunEnqueueRow,
            r#"SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS "status!",
                idempotency_key,
                result_step_key,
                metadata,
                enqueue_request = $3::jsonb AS "enqueue_request_matches?",
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR SHARE"#,
            payload.workflow_type() as _,
            idempotency_key,
            enqueue_request,
        )
        .fetch_optional(&mut **tx)
        .await
    };

    run.map_err(|error| {
        Error::from_query_sqlx_with_context("load idempotent workflow enqueue", error)
    })
}

fn enqueue_workflow_run_idempotency_conflict_clause(
    payload: &WorkflowRunEnqueue<'_>,
) -> &'static str {
    // Keep these predicates aligned with the partial unique indexes
    // uq_workflow_runs_type_idempotency_org and uq_workflow_runs_type_idempotency_global.
    match (payload.idempotency_key(), payload.organization_id()) {
        (Some(_), Some(_)) => {
            "ON CONFLICT (workflow_type, organization_id, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NOT NULL
             DO NOTHING"
        }
        (Some(_), None) => {
            "ON CONFLICT (workflow_type, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NULL
             DO NOTHING"
        }
        (None, _) => "",
    }
}

pub(super) fn validate_existing_idempotent_workflow_run(
    existing: &WorkflowRunEnqueueRow,
) -> Result<()> {
    match existing.enqueue_request_matches() {
        Some(true) => Ok(()),
        Some(false) => Err(workflow_enqueue_conflicting_retry_error("request")),
        None => Err(workflow_legacy_idempotency_snapshot_missing_error(
            existing.workflow_type.as_str(),
            existing.id,
        )),
    }
}
