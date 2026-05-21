use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

// Reserved for workflow-run release, cancellation, and terminal-completion
// advisory lock coordination. This separates this lock class from other
// advisory-lock families; UUID folding still determines the collision
// probability between workflow runs.
const WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE: u64 = 0x7275_6e6c_0000_0000;

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
pub(crate) async fn lock_workflow_run_release_exclusive_after_jobs_tx(
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
pub(crate) async fn lock_workflow_run_release_shared_tx(
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
pub(crate) async fn try_lock_workflow_run_release_shared_tx(
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

#[cfg(feature = "test-support")]
pub mod test_support {
    use sqlx::types::Uuid;

    #[must_use]
    pub fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
        super::workflow_run_release_lock_key(workflow_run_id)
    }
}
