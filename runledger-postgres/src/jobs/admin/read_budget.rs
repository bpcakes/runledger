use crate::{DbPool, DbTx, Error, Result};

use super::super::transaction_settings::cap_local_statement_timeout_tx;

const ADMIN_READ_STATEMENT_TIMEOUT: &str = "30s";
const ADMIN_READ_STATEMENT_TIMEOUT_MS: i64 = 30_000;

/// A read-only transaction with a server-enforced execution budget.
///
/// Pagination bounds response size, not PostgreSQL work. Admin queries that can
/// aggregate or substring-search large relations use this boundary so their
/// worst-case execution time is capped without leaking session settings back
/// into the connection pool.
pub(super) struct AdminReadTransaction<'a> {
    tx: DbTx<'a>,
}

impl<'a> AdminReadTransaction<'a> {
    pub(super) async fn begin(pool: &'a DbPool) -> Result<Self> {
        Self::begin_with_statement_timeout(
            pool,
            ADMIN_READ_STATEMENT_TIMEOUT,
            ADMIN_READ_STATEMENT_TIMEOUT_MS,
        )
        .await
    }

    async fn begin_with_statement_timeout(
        pool: &'a DbPool,
        statement_timeout: &str,
        statement_timeout_ms: i64,
    ) -> Result<Self> {
        let mut tx = pool.begin().await.map_err(|error| {
            Error::from_query_sqlx_with_context("begin bounded admin read", error)
        })?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("set admin transaction read only", error)
            })?;
        cap_local_statement_timeout_tx(
            &mut tx,
            statement_timeout,
            statement_timeout_ms,
            "cap admin read statement timeout",
        )
        .await?;
        Ok(Self { tx })
    }

    pub(super) fn as_tx(&mut self) -> &mut DbTx<'a> {
        &mut self.tx
    }

    pub(super) async fn commit(self) -> Result<()> {
        self.tx.commit().await.map_err(|error| {
            Error::from_query_sqlx_with_context("commit bounded admin read", error)
        })
    }
}

#[cfg(test)]
mod tests {
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    use super::AdminReadTransaction;

    #[tokio::test]
    async fn transaction_is_read_only_local_and_server_timed() {
        let (pool, database) = setup_ephemeral_pool("postgres_admin_read_budget", 1).await;
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version");
        eprintln!("admin read budget PostgreSQL server_version={server_version}");

        let mut read = AdminReadTransaction::begin_with_statement_timeout(&pool, "25ms", 25)
            .await
            .expect("begin bounded admin read");
        let settings = sqlx::query_as::<_, (bool, String)>(
            "SELECT
                current_setting('transaction_read_only')::bool,
                current_setting('statement_timeout')",
        )
        .fetch_one(&mut **read.as_tx())
        .await
        .expect("read bounded admin transaction settings");
        assert_eq!(settings, (true, "25ms".to_owned()));

        let timeout_error = sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut **read.as_tx())
            .await
            .expect_err("PostgreSQL must cancel an admin read that exceeds its budget");
        assert_eq!(
            timeout_error
                .as_database_error()
                .and_then(|error| error.code()),
            Some(std::borrow::Cow::Borrowed("57014"))
        );
        drop(read);

        let session_timeout =
            sqlx::query_scalar::<_, String>("SELECT current_setting('statement_timeout')")
                .fetch_one(&pool)
                .await
                .expect("reuse connection after timed-out local transaction");
        assert_eq!(session_timeout, "0");

        teardown_ephemeral_pool(pool, database).await;
    }
}
