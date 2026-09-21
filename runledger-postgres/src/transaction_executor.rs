use sqlx::{Executor, PgConnection, Postgres};

/// Internal query dispatch only. Transaction invariants belong to the owner.
pub(crate) trait PgQueryExecutor: Send {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres>;
}

/// Private write-transaction dispatch. Idle connections and read-only snapshot
/// capabilities deliberately do not implement this marker. Native DbTx callers
/// still own their transaction-control obligations; this is not XID evidence.
pub(crate) trait PgTransactionalExecutor: PgQueryExecutor {}

impl PgTransactionalExecutor for crate::DbTx<'_> {}
impl PgTransactionalExecutor for batter_sqlx::PgScopedSql<'_> {}

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

#[cfg(test)]
mod tests {
    use super::PgTransactionalExecutor;
    static_assertions::assert_impl_all!(crate::DbTx<'static>: PgTransactionalExecutor);
    static_assertions::assert_impl_all!(batter_sqlx::PgScopedSql<'static>: PgTransactionalExecutor);
    static_assertions::assert_not_impl_any!(sqlx::PgConnection: PgTransactionalExecutor);
    static_assertions::assert_not_impl_any!(batter_sqlx::PgReadOnlySql<'static>: PgTransactionalExecutor);
}
