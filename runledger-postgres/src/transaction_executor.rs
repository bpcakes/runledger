use sqlx::{Executor, PgConnection, Postgres};

/// Internal query dispatch only. Transaction invariants belong to the owner.
pub(crate) trait PgQueryExecutor: Send {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres>;
}
impl PgQueryExecutor for crate::DbTx<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        &mut **self
    }
}
impl PgQueryExecutor for batter_sqlx::PgScopedSql<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        self.executor()
    }
}
impl PgQueryExecutor for PgConnection {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        self
    }
}

impl PgQueryExecutor for batter_sqlx::PgReadOnlySql<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        self.executor()
    }
}
