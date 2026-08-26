use std::time::Duration;

use crate::{DbTx, Result};

use super::super::transaction_settings::{
    PostgresTimeout, cap_local_lock_and_transaction_timeouts_duration_tx,
    cap_local_lock_timeout_duration_tx, set_local_lock_timeout_tx,
};

// Heartbeat and progress mutations are bounded lifecycle operations. Their lock
// cap prevents a worker connection from waiting indefinitely behind an
// abandoned job-row transaction, while the transaction cap also covers a
// connection left idle after acquiring that row lock. Completion transactions
// can include unbounded workflow propagation and therefore apply only the lock
// cap, scoped to their initial job-row acquisition. Stricter consumer settings
// remain in force in both cases.
const JOB_LIFECYCLE_LOCK_TIMEOUT: PostgresTimeout = PostgresTimeout::new(Duration::from_secs(5));
const JOB_LIFECYCLE_TRANSACTION_TIMEOUT: PostgresTimeout =
    PostgresTimeout::new(Duration::from_secs(30));
const _: () = assert!(
    JOB_LIFECYCLE_TRANSACTION_TIMEOUT.milliseconds() > JOB_LIFECYCLE_LOCK_TIMEOUT.milliseconds()
);

pub(super) async fn cap_bounded_job_lifecycle_timeouts_tx(
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

pub(super) async fn cap_job_row_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    context: &'static str,
) -> Result<String> {
    cap_local_lock_timeout_duration_tx(tx, JOB_LIFECYCLE_LOCK_TIMEOUT, context).await
}

pub(super) async fn restore_job_row_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    previous_lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    set_local_lock_timeout_tx(tx, previous_lock_timeout, context).await
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
        cap_bounded_job_lifecycle_timeouts_tx(&mut tx, "cap lifecycle timeouts in regression test")
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
        cap_bounded_job_lifecycle_timeouts_tx(
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
