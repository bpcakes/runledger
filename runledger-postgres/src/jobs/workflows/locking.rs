use sqlx::types::Uuid;

// Reserved for workflow-run release/cancel advisory lock coordination. This
// separates this lock class from other advisory-lock families; UUID folding
// still determines the collision probability between workflow runs.
const WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE: u64 = 0x7275_6e6c_0000_0000;

fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
    let value = workflow_run_id.as_u128();
    // Advisory locks are 64-bit; collisions only over-serialize unrelated runs.
    let folded = (value >> 64) as u64 ^ value as u64 ^ WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE;
    folded as i64
}

pub(crate) async fn lock_workflow_run_release_tx(
    tx: &mut crate::DbTx<'_>,
    workflow_run_id: Uuid,
) -> crate::Result<()> {
    sqlx::query!(
        // Keep the marker in the SQL text; workflow_cancel_lock_order.rs
        // observes this exact wait in pg_stat_activity.
        "SELECT pg_advisory_xact_lock($1) /* runledger:lock_workflow_run_release */",
        workflow_run_release_lock_key(workflow_run_id)
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("lock workflow run release", error)
    })?;

    Ok(())
}

// Releases take a shared advisory lock so independent releases on the same run
// can proceed together; cancellation takes the exclusive form above.
pub(crate) async fn try_lock_workflow_run_release_shared_tx(
    tx: &mut crate::DbTx<'_>,
    workflow_run_id: Uuid,
) -> crate::Result<bool> {
    sqlx::query_scalar!(
        "SELECT pg_try_advisory_xact_lock_shared($1) AS \"acquired!\"",
        workflow_run_release_lock_key(workflow_run_id),
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("try shared lock workflow run release", error)
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
