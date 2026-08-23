use std::time::Duration;

use crate::{DbTx, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::jobs) struct PostgresTimeout(Duration);

impl PostgresTimeout {
    pub(in crate::jobs) const fn new(duration: Duration) -> Self {
        assert!(
            duration.subsec_nanos() % 1_000_000 == 0,
            "PostgreSQL timeout duration must use whole milliseconds"
        );
        assert!(
            duration.as_millis() <= i64::MAX as u128,
            "PostgreSQL timeout milliseconds must fit in i64"
        );
        Self(duration)
    }

    pub(in crate::jobs) const fn milliseconds(self) -> i64 {
        self.0.as_millis() as i64
    }

    fn setting_value(self) -> String {
        format!("{}ms", self.milliseconds())
    }
}

#[derive(Clone, Copy)]
enum OperationTimeoutSetting {
    Lock,
    Statement,
}

impl OperationTimeoutSetting {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Lock => "lock_timeout",
            Self::Statement => "statement_timeout",
        }
    }
}

/// Caps the transaction-local PostgreSQL `lock_timeout` while preserving any
/// stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    cap_local_operation_timeout_tx(
        tx,
        OperationTimeoutSetting::Lock,
        lock_timeout,
        lock_timeout_ms,
        context,
    )
    .await
}

pub(in crate::jobs) async fn cap_local_lock_timeout_duration_tx(
    tx: &mut DbTx<'_>,
    timeout: PostgresTimeout,
    context: &'static str,
) -> Result<String> {
    let setting_value = timeout.setting_value();
    cap_local_lock_timeout_tx(tx, &setting_value, timeout.milliseconds(), context).await
}

/// Caps the transaction-local PostgreSQL `statement_timeout` while preserving
/// any stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    cap_local_operation_timeout_tx(
        tx,
        OperationTimeoutSetting::Statement,
        statement_timeout,
        statement_timeout_ms,
        context,
    )
    .await
}

pub(in crate::jobs) async fn cap_local_statement_timeout_duration_tx(
    tx: &mut DbTx<'_>,
    timeout: PostgresTimeout,
    context: &'static str,
) -> Result<String> {
    let setting_value = timeout.setting_value();
    cap_local_statement_timeout_tx(tx, &setting_value, timeout.milliseconds(), context).await
}

/// Caps the transaction-local PostgreSQL `transaction_timeout` while preserving
/// any stricter caller setting and returns the exact prior value for restoration.
pub(in crate::jobs) async fn cap_local_transaction_timeout_tx(
    tx: &mut DbTx<'_>,
    transaction_timeout: &str,
    transaction_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // PostgreSQL arms transaction_timeout when a transaction begins. Merely
    // lowering its GUC while that timer is active changes current_setting()
    // without shortening the already-armed deadline. Disarm a looser timer
    // before applying the cap so the assignment hook starts a fresh deadline.
    // The dependent MATERIALIZED CTEs keep both assignments in one statement,
    // avoiding a cancellation point while the timeout is disabled.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('transaction_timeout') AS timeout,
                setting::bigint AS timeout_ms
             FROM pg_settings
             WHERE name = 'transaction_timeout'
         ),
         disarmed AS MATERIALIZED (
             SELECT set_config(
                 'transaction_timeout',
                 CASE
                     WHEN previous.timeout_ms > $2 THEN '0'
                     ELSE previous.timeout
                 END,
                 true
             ) AS timeout
             FROM previous
         ),
         applied AS MATERIALIZED (
             SELECT set_config(
                 'transaction_timeout',
                 CASE
                     WHEN previous.timeout_ms = 0 OR previous.timeout_ms > $2 THEN $1
                     ELSE previous.timeout
                 END,
                 true
             ) AS timeout
             FROM previous
             CROSS JOIN disarmed
         )
         SELECT previous.timeout
         FROM previous
         CROSS JOIN applied",
    )
    .bind(transaction_timeout)
    .bind(transaction_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

pub(in crate::jobs) async fn cap_local_transaction_timeout_duration_tx(
    tx: &mut DbTx<'_>,
    timeout: PostgresTimeout,
    context: &'static str,
) -> Result<String> {
    let setting_value = timeout.setting_value();
    cap_local_transaction_timeout_tx(tx, &setting_value, timeout.milliseconds(), context).await
}

/// Caps timeouts whose effective deadline is evaluated when a statement or
/// lock wait starts. Transaction-lifecycle timeouts must use their dedicated
/// helper above so their already-armed timer is handled correctly.
async fn cap_local_operation_timeout_tx(
    tx: &mut DbTx<'_>,
    setting: OperationTimeoutSetting,
    timeout: &str,
    timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    let setting_name = setting.as_str();
    // Preserve PostgreSQL's own GUC text so restore keeps units and special
    // values such as "0" exactly as the connection reported them.
    // MATERIALIZED is load-bearing: it forces current_setting to be captured
    // before set_config mutates the transaction-local value. pg_settings.setting
    // stores timeout GUCs in their base unit of milliseconds.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting($1) AS timeout,
                setting::bigint AS timeout_ms
             FROM pg_settings
             WHERE name = $1
         )
         SELECT previous.timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    $1,
                    CASE
                        WHEN previous.timeout_ms = 0 THEN $2
                        WHEN previous.timeout_ms <= $3 THEN previous.timeout
                        ELSE $2
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(setting_name)
    .bind(timeout)
    .bind(timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

/// Restores a transaction-local PostgreSQL `lock_timeout` value returned by
/// [`cap_local_lock_timeout_tx`].
pub(in crate::jobs) async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    set_local_operation_timeout_tx(tx, OperationTimeoutSetting::Lock, lock_timeout, context).await
}

/// Restores a transaction-local PostgreSQL `statement_timeout` value returned
/// by [`cap_local_statement_timeout_tx`].
pub(in crate::jobs) async fn set_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    context: &'static str,
) -> Result<()> {
    set_local_operation_timeout_tx(
        tx,
        OperationTimeoutSetting::Statement,
        statement_timeout,
        context,
    )
    .await
}

async fn set_local_operation_timeout_tx(
    tx: &mut DbTx<'_>,
    setting: OperationTimeoutSetting,
    timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config($1, $2, true)")
        .bind(setting.as_str())
        .bind(timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
    use tokio::time::timeout;

    use super::{
        PostgresTimeout, cap_local_lock_timeout_duration_tx,
        cap_local_transaction_timeout_duration_tx,
    };

    #[test]
    fn postgres_timeout_derives_text_and_milliseconds_from_one_duration() {
        let timeout = PostgresTimeout::new(Duration::from_millis(1_250));
        let maximum = PostgresTimeout::new(Duration::from_millis(i64::MAX as u64));

        assert_eq!(timeout.setting_value(), "1250ms");
        assert_eq!(timeout.milliseconds(), 1_250);
        assert_eq!(maximum.milliseconds(), i64::MAX);
    }

    #[test]
    #[should_panic(expected = "PostgreSQL timeout milliseconds must fit in i64")]
    fn postgres_timeout_rejects_milliseconds_above_postgresql_comparison_bound() {
        PostgresTimeout::new(Duration::from_millis(i64::MAX as u64 + 1));
    }

    #[test]
    #[should_panic(expected = "PostgreSQL timeout duration must use whole milliseconds")]
    fn postgres_timeout_rejects_fractional_milliseconds() {
        PostgresTimeout::new(Duration::from_micros(1_500));
    }

    #[tokio::test]
    async fn lock_timeout_cap_preserves_ordering_at_millisecond_boundaries() {
        let (pool, database) = setup_ephemeral_pool("postgres_lock_timeout_boundaries", 1).await;
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
            "lock timeout boundary PostgreSQL server_version={server_version}, \
             server_version_num={server_version_num}"
        );

        let cap = PostgresTimeout::new(Duration::from_millis(1_000));
        let mut tx = pool.begin().await.expect("begin timeout boundary tx");

        for (caller_timeout, expected_timeout) in
            [("999ms", "999ms"), ("1000ms", "1s"), ("1001ms", "1s")]
        {
            sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
                .bind(caller_timeout)
                .fetch_one(&mut *tx)
                .await
                .expect("set caller lock timeout");

            cap_local_lock_timeout_duration_tx(
                &mut tx,
                cap,
                "cap lock timeout at ordering boundary",
            )
            .await
            .expect("cap lock timeout");

            assert_eq!(
                sqlx::query_scalar::<_, String>("SHOW lock_timeout")
                    .fetch_one(&mut *tx)
                    .await
                    .expect("read capped lock timeout"),
                expected_timeout
            );
        }

        tx.rollback().await.expect("roll back boundary tx");
        teardown_ephemeral_pool(pool, database).await;
    }

    #[tokio::test]
    async fn transaction_timeout_cap_rearms_a_looser_active_timer() {
        let (pool, database) = setup_ephemeral_pool("postgres_transaction_timeout_rearm", 1).await;
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
            "transaction timeout rearm PostgreSQL server_version={server_version}, \
             server_version_num={server_version_num}"
        );

        sqlx::query("SET SESSION transaction_timeout = '5s'")
            .execute(&pool)
            .await
            .expect("set loose session transaction timeout");
        let mut tx = pool.begin().await.expect("begin capped transaction");
        let previous = cap_local_transaction_timeout_duration_tx(
            &mut tx,
            PostgresTimeout::new(Duration::from_millis(300)),
            "cap transaction timeout in regression test",
        )
        .await
        .expect("cap active transaction timeout");
        assert_eq!(previous, "5s");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SHOW transaction_timeout")
                .fetch_one(&mut *tx)
                .await
                .expect("read capped transaction timeout"),
            "300ms"
        );

        let started = Instant::now();
        let _error = timeout(
            Duration::from_millis(1_500),
            sqlx::query("SELECT pg_sleep(2)").execute(&mut *tx),
        )
        .await
        .expect("PostgreSQL transaction timeout must beat the test guard")
        .expect_err("over-budget transaction must terminate its connection");
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "transaction timeout fired too early: {:?}",
            started.elapsed()
        );
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "transaction timeout did not replace the looser active timer: {:?}",
            started.elapsed()
        );
        drop(tx);
        teardown_ephemeral_pool(pool, database).await;
    }
}
