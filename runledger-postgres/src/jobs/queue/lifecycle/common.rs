use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::super::errors::{lease_owner_mismatch_error, validate_completion_progress};
use super::super::super::transaction_settings::{
    PostgresTimeout, cap_local_lock_and_transaction_timeouts_duration_tx,
};
use super::super::super::types::JobLeaseIdentity;

// Lifecycle mutations are expected to be short. The lock cap prevents one
// worker connection from waiting indefinitely behind an abandoned job-row
// transaction, while the transaction cap also covers a connection left idle
// after acquiring that row lock. Stricter consumer settings remain in force.
const JOB_LIFECYCLE_LOCK_TIMEOUT: PostgresTimeout = PostgresTimeout::new(Duration::from_secs(5));
const JOB_LIFECYCLE_TRANSACTION_TIMEOUT: PostgresTimeout =
    PostgresTimeout::new(Duration::from_secs(30));
const _: () = assert!(
    JOB_LIFECYCLE_TRANSACTION_TIMEOUT.milliseconds() > JOB_LIFECYCLE_LOCK_TIMEOUT.milliseconds()
);

pub(super) const HEARTBEAT_LEASE_MISMATCH_CONTEXT: &str =
    "heartbeat job transaction lease mismatch";
pub(super) const UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT: &str =
    "update job progress transaction lease mismatch";
pub(super) const COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT: &str =
    "complete job success transaction lease mismatch";
pub(super) const COMPLETE_CONTINUATION_LEASE_MISMATCH_CONTEXT: &str =
    "complete job continuation transaction lease mismatch";
pub(super) const COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT: &str =
    "complete job failure transaction missing leased row";

pub(super) async fn cap_owned_job_lifecycle_timeouts_tx(
    tx: &mut DbTx<'_>,
    context: &'static str,
) -> Result<()> {
    // These are owned transactions, so both transaction-local values
    // intentionally remain active until the operation commits or rolls back.
    cap_local_lock_and_transaction_timeouts_duration_tx(
        tx,
        JOB_LIFECYCLE_LOCK_TIMEOUT,
        JOB_LIFECYCLE_TRANSACTION_TIMEOUT,
        context,
    )
    .await?;
    Ok(())
}

pub(super) struct CompletionLeaseRow {
    pub(super) job_type: String,
    pub(super) organization_id: Option<Uuid>,
    pub(super) max_attempts: i32,
    pub(super) progress_done: Option<i64>,
    pub(super) progress_total: Option<i64>,
    pub(super) completion_base_at: DateTime<Utc>,
}

pub(super) async fn lock_live_completion_lease_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    error_context: &'static str,
) -> Result<Option<CompletionLeaseRow>> {
    sqlx::query_as!(
        CompletionLeaseRow,
        r#"SELECT
            job_type AS "job_type!",
            organization_id,
            max_attempts,
            progress_done,
            progress_total,
            clock_timestamp() AS "completion_base_at!"
         FROM job_queue
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at > clock_timestamp()
         FOR UPDATE"#,
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))
}

pub(super) fn coalesce_completion_progress(
    progress_done: &mut Option<i64>,
    progress_total: &mut Option<i64>,
    existing: &CompletionLeaseRow,
) -> Result<()> {
    *progress_done = progress_done.or(existing.progress_done);
    *progress_total = progress_total.or(existing.progress_total);
    validate_completion_progress(*progress_done, *progress_total)
}

pub(super) async fn finish_successful_attempt_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = NULL,
             error_code = NULL,
             error_message = NULL,
             retry_delay_ms = NULL
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

pub(super) async fn rollback_and_return_lease_mismatch<T>(
    tx: DbTx<'_>,
    context: &'static str,
) -> Result<T> {
    if let Err(error) = tx.rollback().await {
        tracing::warn!(
            error = %error,
            lease_mismatch_context = context,
            "failed to rollback transaction due to lease mismatch"
        );
    }
    Err(lease_owner_mismatch_error())
}

#[cfg(test)]
mod tests {
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    use super::*;

    #[tokio::test]
    async fn owned_lifecycle_transactions_cap_and_preserve_database_timeouts() {
        let (pool, database) = setup_ephemeral_pool("postgres_owned_lifecycle_timeouts", 1).await;
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version");
        let server_version_num =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
                .fetch_one(&pool)
                .await
                .expect("read PostgreSQL server_version_num");
        eprintln!(
            "owned lifecycle timeout regression PostgreSQL server_version={server_version}, \
             server_version_num={server_version_num}"
        );

        let mut tx = pool.begin().await.expect("begin lifecycle timeout tx");
        cap_owned_job_lifecycle_timeouts_tx(&mut tx, "cap lifecycle timeouts in regression test")
            .await
            .expect("cap lifecycle transaction timeouts");

        let (lock_timeout, transaction_timeout) = sqlx::query_as::<_, (String, String)>(
            "SELECT current_setting('lock_timeout'), current_setting('transaction_timeout')",
        )
        .fetch_one(&mut *tx)
        .await
        .expect("read capped lifecycle timeouts");
        assert_eq!(lock_timeout, "5s");
        assert_eq!(transaction_timeout, "30s");

        tx.rollback().await.expect("roll back lifecycle timeout tx");
        let (lock_timeout, transaction_timeout) = sqlx::query_as::<_, (String, String)>(
            "SELECT current_setting('lock_timeout'), current_setting('transaction_timeout')",
        )
        .fetch_one(&pool)
        .await
        .expect("read restored session timeouts");
        assert_eq!(lock_timeout, "0");
        assert_eq!(transaction_timeout, "0");

        let mut strict_tx = pool
            .begin()
            .await
            .expect("begin strict lifecycle timeout tx");
        sqlx::query(
            "SELECT
                set_config('lock_timeout', '100ms', true),
                set_config('transaction_timeout', '10s', true)",
        )
        .execute(&mut *strict_tx)
        .await
        .expect("set stricter lifecycle timeouts");
        cap_owned_job_lifecycle_timeouts_tx(
            &mut strict_tx,
            "preserve strict lifecycle timeouts in regression test",
        )
        .await
        .expect("preserve strict lifecycle timeouts");
        let (lock_timeout, transaction_timeout) = sqlx::query_as::<_, (String, String)>(
            "SELECT current_setting('lock_timeout'), current_setting('transaction_timeout')",
        )
        .fetch_one(&mut *strict_tx)
        .await
        .expect("read strict lifecycle timeouts");
        assert_eq!(lock_timeout, "100ms");
        assert_eq!(transaction_timeout, "10s");
        strict_tx
            .rollback()
            .await
            .expect("roll back strict lifecycle timeout tx");

        teardown_ephemeral_pool(pool, database).await;
    }
}
