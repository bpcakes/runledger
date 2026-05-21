use runledger_core::jobs::{StepKeyName, WorkflowStepExecutionKind, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::jobs::row_decode::{
    parse_step_key_name, parse_workflow_run_status, parse_workflow_step_execution_kind,
    parse_workflow_step_status,
};
use crate::jobs::workflow_types::WorkflowRunDbRecord;
use crate::{DbTx, Error, Result};

use super::errors::{workflow_internal_state_error, workflow_run_not_found_error};
use super::read::load_workflow_run_by_id_tx;

// Reserved for workflow-run release, cancellation, and terminal-completion
// advisory lock coordination. This separates this lock class from other
// advisory-lock families; UUID folding still determines the collision
// probability between workflow runs.
const WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE: u64 = 0x7275_6e6c_0000_0000;

#[derive(Clone, Debug)]
pub(in crate::jobs::workflows) struct LockedWorkflowStepState {
    pub(in crate::jobs::workflows) id: Uuid,
    pub(in crate::jobs::workflows) step_key: StepKeyName,
    pub(in crate::jobs::workflows) execution_kind: WorkflowStepExecutionKind,
    pub(in crate::jobs::workflows) organization_id: Option<Uuid>,
    pub(in crate::jobs::workflows) status: WorkflowStepStatus,
    pub(in crate::jobs::workflows) job_id: Option<Uuid>,
}

impl LockedWorkflowStepState {
    pub(in crate::jobs::workflows) fn decode(
        id: Uuid,
        step_key: String,
        execution_kind: String,
        organization_id: Option<Uuid>,
        status: String,
        job_id: Option<Uuid>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            step_key: parse_step_key_name(step_key)?,
            execution_kind: parse_workflow_step_execution_kind(execution_kind)?,
            organization_id,
            status: parse_workflow_step_status(status)?,
            job_id,
        })
    }
}

fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
    let value = workflow_run_id.as_u128();
    // Advisory locks are 64-bit; collisions only over-serialize unrelated runs.
    let folded = (value >> 64) as u64 ^ value as u64 ^ WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE;
    folded as i64
}

// Exclusive release/cancel lock.
//
// Callers must lock every workflow-managed job_queue row for the workflow run
// before taking this lock. Job terminal completion also holds the relevant job
// row before it takes the blocking shared form, so preserving job-row-first
// ordering is what keeps cancellation and terminal completion deadlock-free.
pub(in crate::jobs::workflows) async fn lock_workflow_run_release_exclusive_after_jobs_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<()> {
    sqlx::query!(
        // Keep the marker in the SQL text; workflow_cancel_lock_order.rs
        // observes this exact wait in pg_stat_activity.
        "SELECT pg_advisory_xact_lock($1) /* runledger:lock_workflow_run_release */",
        workflow_run_release_lock_key(workflow_run_id)
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("lock workflow run release", error))?;

    Ok(())
}

// Blocks until no cancellation owns the exclusive release lock. Terminal job
// completion uses this after it has already persisted the terminal step state
// so dependency release observes cancellation as an ordered transaction boundary
// instead of surfacing a transient release-conflict error to the worker.
pub(in crate::jobs::workflows) async fn lock_workflow_run_release_shared_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<()> {
    // DO NOT REMOVE the SQL comment marker: lock-order integration tests use it
    // to observe a backend waiting on this advisory lock.
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared($1) /* runledger:lock_workflow_run_release_shared */",
    )
    .bind(workflow_run_release_lock_key(workflow_run_id))
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock shared workflow run release", error)
    })?;

    Ok(())
}

// Releases take a shared advisory lock so independent releases on the same run
// can proceed together; cancellation takes the exclusive form above.
pub(in crate::jobs::workflows) async fn try_lock_workflow_run_release_shared_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<bool> {
    sqlx::query_scalar!(
        "SELECT pg_try_advisory_xact_lock_shared($1) AS \"acquired!\"",
        workflow_run_release_lock_key(workflow_run_id),
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("try shared lock workflow run release", error)
    })
}

pub(in crate::jobs::workflows) async fn lock_workflow_step_jobs_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<()> {
    sqlx::query!(
        // Keep the marker in the SQL text; the lock-order regression test
        // uses it to observe this exact wait in pg_stat_activity.
        //
        // Lock in UUIDv7 creation order. Terminal completion holds a prerequisite
        // job row before releasing dependent jobs, and dependent job rows are
        // inserted later, so cancel must block on older prerequisite rows before
        // it can lock newer dependent rows.
        "SELECT jq.id /* runledger:lock_workflow_step_jobs_for_update */
         FROM job_queue jq
         JOIN workflow_steps ws ON ws.job_id = jq.id
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY jq.id ASC
         FOR UPDATE OF jq",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow step jobs for mutation", error)
    })?;

    Ok(())
}

pub(in crate::jobs::workflows) async fn lock_workflow_step_rows_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<()> {
    // Keep the inline marker below in sync with lock-order regression tests
    // that identify this wait in pg_stat_activity.
    sqlx::query(
        "SELECT ws.id /* runledger:lock_workflow_step_rows_for_update */
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY ws.id ASC
         FOR UPDATE OF ws",
    )
    .bind(workflow_run_id)
    .bind(organization_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow step rows for mutation", error)
    })?;

    Ok(())
}

pub(in crate::jobs::workflows) async fn lock_workflow_steps_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<Vec<LockedWorkflowStepState>> {
    let rows = sqlx::query!(
        "SELECT
            ws.id,
            ws.step_key,
            ws.execution_kind::text AS \"execution_kind!\",
            ws.organization_id,
            ws.status::text AS \"status!\",
            ws.job_id
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY ws.id ASC
         FOR UPDATE OF ws",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow steps for mutation", error)
    })?;

    rows.into_iter()
        .map(|row| {
            LockedWorkflowStepState::decode(
                row.id,
                row.step_key,
                row.execution_kind,
                row.organization_id,
                row.status,
                row.job_id,
            )
        })
        .collect()
}

pub(in crate::jobs::workflows) async fn lock_workflow_run_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<WorkflowRunDbRecord> {
    let row = sqlx::query!(
        "SELECT id, status::text AS \"status!\"
         FROM workflow_runs
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
         FOR UPDATE",
        workflow_run_id,
        organization_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("lock workflow run for mutation", error))?
    .ok_or_else(workflow_run_not_found_error)?;

    let workflow_run =
        load_workflow_run_by_id_tx(tx, row.id, "load workflow run after mutation lock").await?;
    let status = parse_workflow_run_status(row.status)?;
    if workflow_run.status != status {
        return Err(workflow_internal_state_error(format!(
            "workflow run {} status changed during mutation lock reload",
            workflow_run.id
        )));
    }
    Ok(workflow_run)
}

#[cfg(feature = "test-support")]
pub mod test_support {
    use sqlx::types::Uuid;

    #[must_use]
    pub fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
        super::workflow_run_release_lock_key(workflow_run_id)
    }
}
