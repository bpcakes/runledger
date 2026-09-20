use sqlx::{Executor, PgConnection, Postgres};

/// Internal query dispatch only. Transaction invariants belong to the owner.
pub(crate) trait PgTransactionExecutor: Send {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres>;
}
impl PgTransactionExecutor for crate::DbTx<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        &mut **self
    }
}
impl PgTransactionExecutor for batter_sqlx::PgScopedSql<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        self.executor()
    }
}
impl PgTransactionExecutor for PgConnection {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        self
    }
}
