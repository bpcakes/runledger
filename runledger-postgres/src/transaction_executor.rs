use sqlx::{Executor, Postgres};

use crate::DbTx;

/// Execution access to one caller-owned, explicit PostgreSQL transaction.
///
/// This capability lets an adapter keep its transaction representation opaque:
/// Runledger receives a fresh SQLx executor view for each query, but it cannot
/// extract, replace, commit, or roll back the underlying connection. The
/// implementer must return views of the same live explicit transaction for the
/// duration of a Runledger operation. Implement this only for types whose
/// construction establishes that transaction ownership invariant.
///
/// A native SQLx [`DbTx`] implements this trait. Adapters can implement it for
/// their own opaque transaction types without exposing `sqlx::PgConnection` or
/// `sqlx::Transaction` to application code.
///
/// ```rust,no_run
/// use runledger_postgres::PgTransactionExecutor;
/// use sqlx::{Executor, Postgres};
///
/// struct OpaqueTransaction<'a> {
///     inner: sqlx::Transaction<'a, Postgres>,
/// }
///
/// impl PgTransactionExecutor for OpaqueTransaction<'_> {
///     fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
///         &mut *self.inner
///     }
/// }
/// ```
pub trait PgTransactionExecutor: Send {
    /// Borrow an executor view of the same live transaction.
    ///
    /// The opaque return type deliberately exposes only SQL execution. It must
    /// not provide an independent connection or a view whose transaction can
    /// differ between calls.
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres>;
}

impl PgTransactionExecutor for DbTx<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        &mut **self
    }
}
