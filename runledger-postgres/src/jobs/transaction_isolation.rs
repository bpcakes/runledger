use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

fn transaction_error_context(action: &str, operation: &str) -> String {
    format!("{action} {operation} transaction")
}

async fn rollback_owned_transaction(tx: DbTx<'_>, operation: &str, operation_error: &Error) {
    if let Err(rollback_error) = tx.rollback().await {
        tracing::warn!(
            operation,
            error = %rollback_error,
            original_error = %operation_error,
            "failed to roll back owned transaction"
        );
    }
}

/// Opens an internally owned transaction with an explicit `READ COMMITTED`
/// isolation level.
///
/// An internal pool-owning wrapper may call its operation body directly after
/// this succeeds. Caller-owned transaction APIs must still use
/// [`ensure_read_committed_tx`] because they did not establish the isolation.
pub(crate) async fn begin_owned_read_committed_tx<'a>(
    pool: &'a DbPool,
    operation: &str,
) -> Result<DbTx<'a>> {
    let begin_context = transaction_error_context("begin", operation);
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(&begin_context, error))?;

    if let Err(error) = sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await
    {
        let isolation_context = transaction_error_context("set isolation for", operation);
        let error = Error::from_query_sqlx_with_context(&isolation_context, error);
        rollback_owned_transaction(tx, operation, &error).await;
        return Err(error);
    }

    Ok(tx)
}

/// Commits a successful internally owned operation or rolls back its error.
pub(crate) async fn finish_owned_transaction<T>(
    tx: DbTx<'_>,
    operation: &str,
    operation_result: Result<T>,
) -> Result<T> {
    match operation_result {
        Ok(value) => {
            let commit_context = transaction_error_context("commit", operation);
            tx.commit()
                .await
                .map_err(|error| Error::from_query_sqlx_with_context(&commit_context, error))?;
            Ok(value)
        }
        Err(error) => {
            rollback_owned_transaction(tx, operation, &error).await;
            Err(error)
        }
    }
}

pub(crate) async fn ensure_read_committed_tx(
    tx: &mut DbTx<'_>,
    operation: &'static str,
    code: &'static str,
    client_message: &'static str,
) -> Result<()> {
    // SHOW reports the effective isolation for the current transaction, so a
    // caller's SET TRANSACTION change is visible here. PostgreSQL treats READ
    // UNCOMMITTED as READ COMMITTED.
    //
    // Call this at the start of each transaction-sensitive operation. The guard
    // is not meant to defend against callers changing isolation again later in
    // the same transaction.
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("inspect transaction isolation", error)
        })?;

    let normalized = isolation.to_ascii_lowercase();
    if matches!(normalized.as_str(), "read committed" | "read uncommitted") {
        return Ok(());
    }

    Err(Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        format!("{operation} requires READ COMMITTED isolation; got {isolation}"),
    )))
}

#[cfg(test)]
mod tests {
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    use super::{begin_owned_read_committed_tx, finish_owned_transaction};
    use crate::{Error, Result};

    #[tokio::test]
    async fn owned_transaction_commits_success_and_rolls_back_operation_error() {
        let (pool, database) =
            setup_ephemeral_pool("postgres_owned_read_committed_transaction", 1).await;
        sqlx::query(
            "CREATE TABLE owned_transaction_test_values (
                value integer PRIMARY KEY
             )",
        )
        .execute(&pool)
        .await
        .expect("create owned transaction test table");

        let mut connection = pool.acquire().await.expect("acquire test connection");
        sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *connection)
            .await
            .expect("set non-default session isolation");
        drop(connection);

        let mut commit_tx = begin_owned_read_committed_tx(&pool, "test commit")
            .await
            .expect("begin owned commit transaction");
        let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *commit_tx)
            .await
            .expect("inspect owned transaction isolation");
        assert_eq!(isolation, "read committed");
        sqlx::query("INSERT INTO owned_transaction_test_values (value) VALUES (1)")
            .execute(&mut *commit_tx)
            .await
            .expect("insert committed sentinel");
        finish_owned_transaction(commit_tx, "test commit", Ok(()))
            .await
            .expect("commit successful owned operation");

        let mut rollback_tx = begin_owned_read_committed_tx(&pool, "test rollback")
            .await
            .expect("begin owned rollback transaction");
        sqlx::query("INSERT INTO owned_transaction_test_values (value) VALUES (2)")
            .execute(&mut *rollback_tx)
            .await
            .expect("insert rolled-back sentinel");
        let operation_result: Result<()> = Err(Error::ConfigError(
            "intentional operation failure".to_owned(),
        ));
        let error = finish_owned_transaction(rollback_tx, "test rollback", operation_result)
            .await
            .expect_err("operation error should be returned after rollback");
        assert!(
            matches!(error, Error::ConfigError(ref message) if message == "intentional operation failure")
        );

        let values = sqlx::query_scalar::<_, i32>(
            "SELECT value FROM owned_transaction_test_values ORDER BY value",
        )
        .fetch_all(&pool)
        .await
        .expect("load owned transaction test values");
        assert_eq!(values, vec![1]);

        teardown_ephemeral_pool(pool, database).await;
    }
}
