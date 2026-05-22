use runledger_core::jobs::{StepKeyName, WorkflowStepExecutionKind, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::jobs::row_decode::{
    parse_step_key_name, parse_workflow_run_status, parse_workflow_step_execution_kind,
    parse_workflow_step_status,
};
use crate::jobs::workflow_types::WorkflowRunDbRecord;
use crate::{DbTx, Error, Result};

use super::errors::{
    workflow_internal_state_error, workflow_release_conflict_timeout_error,
    workflow_run_not_found_error,
};
use super::read::load_workflow_run_by_id_tx;

// Reserved for workflow-run release, cancellation, and terminal-completion
// advisory lock coordination. This separates this lock class from other
// advisory-lock families; UUID folding still determines the collision
// probability between workflow runs.
const WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE: u64 = 0x7275_6e6c_0000_0000;
// Bound terminal completion waits behind an in-flight cancel/release without
// making normal short cancellation transactions surface as conflicts. Existing
// nonzero lock_timeout values shorter than this cap remain in force. This maps
// only PostgreSQL lock_timeout (55P03); a shorter statement_timeout remains a
// caller or deployment-level cancellation.
const WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS: i64 = 5_000;
const WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT: &str = "5s";

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
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT,
        WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS,
        "set workflow release shared lock timeout",
    )
    .await?;

    // DO NOT REMOVE the SQL comment marker: lock-order integration tests use it
    // to observe a backend waiting on this advisory lock.
    let lock_result = sqlx::query(
        "SELECT pg_advisory_xact_lock_shared($1) /* runledger:lock_workflow_run_release_shared */",
    )
    .bind(workflow_run_release_lock_key(workflow_run_id))
    .execute(&mut **tx)
    .await;

    match lock_result {
        Ok(_) => {
            // Restore defensively so release/recompute work that follows in this
            // transaction uses the caller's original lock-wait policy.
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore workflow release shared lock timeout",
            )
            .await?;
            Ok(())
        }
        // PostgreSQL errors leave the transaction unusable until rollback, and
        // callers return/rollback on this path, so there is no useful timeout
        // value to restore here.
        Err(error) if is_lock_timeout_error(&error) => Err(
            workflow_release_conflict_timeout_error(workflow_run_id, error),
        ),
        Err(error) => Err(Error::from_query_sqlx_with_context(
            "lock shared workflow run release",
            error,
        )),
    }
}

async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // Preserve PostgreSQL's own GUC text so restore keeps units and special
    // values such as "0" exactly as the connection reported them.
    // MATERIALIZED is load-bearing: it forces current_setting to be captured
    // before set_config mutates the transaction-local value. pg_settings.setting
    // stores lock_timeout in its base unit of milliseconds.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('lock_timeout') AS lock_timeout,
                setting::bigint AS lock_timeout_ms
             FROM pg_settings
             WHERE name = 'lock_timeout'
         )
         SELECT previous.lock_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'lock_timeout',
                    CASE
                        WHEN previous.lock_timeout_ms = 0 THEN $1
                        WHEN previous.lock_timeout_ms <= $2 THEN previous.lock_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(lock_timeout)
    .bind(lock_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(lock_timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}

fn is_lock_timeout_error(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code.as_ref() == "55P03")
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
            /* runledger:lock_workflow_steps_for_update */
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

#[cfg(test)]
mod tests {
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    use super::*;

    async fn current_lock_timeout(tx: &mut DbTx<'_>) -> String {
        sqlx::query_scalar::<_, String>("SELECT current_setting('lock_timeout')")
            .fetch_one(&mut **tx)
            .await
            .expect("read lock_timeout")
    }

    #[tokio::test]
    async fn cap_local_lock_timeout_preserves_stricter_existing_timeout() {
        let (pool, database) = setup_ephemeral_pool("workflow_lock_timeout_cap_strict", 1).await;

        let mut tx = pool.begin().await.expect("begin tx");
        set_local_lock_timeout_tx(&mut tx, "100ms", "set test lock_timeout")
            .await
            .expect("set strict lock_timeout");

        let previous = cap_local_lock_timeout_tx(
            &mut tx,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS,
            "cap test lock_timeout",
        )
        .await
        .expect("cap lock_timeout");
        assert_eq!(previous, "100ms");
        assert_eq!(current_lock_timeout(&mut tx).await, "100ms");

        tx.rollback().await.expect("rollback tx");
        teardown_ephemeral_pool(pool, database).await;
    }

    #[tokio::test]
    async fn cap_local_lock_timeout_caps_unlimited_timeout_and_allows_restore() {
        let (pool, database) = setup_ephemeral_pool("workflow_lock_timeout_cap_unlimited", 1).await;

        let mut tx = pool.begin().await.expect("begin tx");
        let previous = cap_local_lock_timeout_tx(
            &mut tx,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS,
            "cap test lock_timeout",
        )
        .await
        .expect("cap lock_timeout");
        assert_eq!(previous, "0");
        assert_eq!(current_lock_timeout(&mut tx).await, "5s");

        set_local_lock_timeout_tx(&mut tx, &previous, "restore test lock_timeout")
            .await
            .expect("restore lock_timeout");
        assert_eq!(current_lock_timeout(&mut tx).await, "0");

        tx.rollback().await.expect("rollback tx");
        teardown_ephemeral_pool(pool, database).await;
    }

    #[tokio::test]
    async fn cap_local_lock_timeout_preserves_equal_cap_and_clamps_longer_timeout() {
        let (pool, database) = setup_ephemeral_pool("workflow_lock_timeout_cap_matrix", 1).await;

        let mut tx = pool.begin().await.expect("begin tx");
        set_local_lock_timeout_tx(&mut tx, "5s", "set equal lock_timeout")
            .await
            .expect("set equal lock_timeout");
        let previous = cap_local_lock_timeout_tx(
            &mut tx,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS,
            "cap equal lock_timeout",
        )
        .await
        .expect("cap equal lock_timeout");
        assert_eq!(previous, "5s");
        assert_eq!(current_lock_timeout(&mut tx).await, "5s");

        set_local_lock_timeout_tx(&mut tx, "10s", "set longer lock_timeout")
            .await
            .expect("set longer lock_timeout");
        let previous = cap_local_lock_timeout_tx(
            &mut tx,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT,
            WORKFLOW_RUN_RELEASE_SHARED_LOCK_ACQUIRE_TIMEOUT_MS,
            "cap longer lock_timeout",
        )
        .await
        .expect("cap longer lock_timeout");
        assert_eq!(previous, "10s");
        assert_eq!(current_lock_timeout(&mut tx).await, "5s");

        tx.rollback().await.expect("rollback tx");
        teardown_ephemeral_pool(pool, database).await;
    }
}

#[cfg(feature = "test-support")]
pub mod test_support {
    use sqlx::types::Uuid;

    #[must_use]
    pub fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
        super::workflow_run_release_lock_key(workflow_run_id)
    }
}
