use sqlx::{Executor, PgConnection, Postgres};

use crate::DbTx;

mod sealed {
    pub trait Transaction {}
}

/// Execution access to one borrowed native PostgreSQL transaction.
///
/// This trait is sealed: only native SQLx transactions and [`PgTransactionView`]
/// implement it. An adapter exposes a view constructed from its private native
/// transaction, rather than promising that arbitrary executors are transactional.
/// The exclusive borrow pins that transaction for the complete operation.
///
/// This preserves the native transaction boundary, not arbitrary SQL semantics:
/// application SQL can still issue transaction-control statements before a call.
/// Commit/rollback acknowledgement and cancellation disposition remain owner policy.
///
/// A downstream connection wrapper cannot masquerade as a transaction:
/// ```compile_fail
/// use runledger_postgres::PgTransactionExecutor;
/// use sqlx::{Executor, PgConnection, Postgres};
/// struct NotATransaction(PgConnection);
/// impl PgTransactionExecutor for NotATransaction {
///     fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
///         &mut self.0
///     }
/// }
/// ```
/// Nor can a pool-backed wrapper supply the transaction API:
/// ```compile_fail
/// use runledger_postgres::PgTransactionExecutor;
/// use sqlx::{Executor, PgPool, Postgres};
/// struct RoutingTransaction(PgPool);
/// impl PgTransactionExecutor for RoutingTransaction {
///     fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
///         &self.0
///     }
/// }
/// ```
pub trait PgTransactionExecutor: sealed::Transaction + Send {
    /// Borrow the retained transaction for one query.
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres>;
}

impl sealed::Transaction for DbTx<'_> {}
impl PgTransactionExecutor for DbTx<'_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        &mut **self
    }
}

/// An opaque exclusive borrow of an actual native SQLx transaction.
///
/// Construct this inside the adapter that owns the native transaction. The view
/// exposes no replaceable resource reference or consuming completion method.
/// Runledger retains its borrow through isolation validation and every write.
/// As with native SQLx, application SQL remains a low-level boundary and can
/// issue transaction-control statements; this view does not validate SQL text.
///
/// ```no_run
/// use runledger_postgres::{DbTx, PgTransactionView};
/// struct Adapter<'a> { transaction: DbTx<'a> }
/// impl<'a> Adapter<'a> {
///     fn view(&mut self) -> PgTransactionView<'_, 'a> {
///         PgTransactionView::new(&mut self.transaction)
///     }
/// }
/// ```
/// An idle connection is not enough, including inside a downstream newtype:
/// ```compile_fail
/// use runledger_postgres::PgTransactionView;
/// struct Idle(sqlx::PgConnection);
/// impl Idle {
///     fn view(&mut self) -> PgTransactionView<'_, '_> {
///         PgTransactionView::new(&mut self.0)
///     }
/// }
/// ```
/// Its representation cannot be forged:
/// ```compile_fail
/// # fn forge(tx: &mut runledger_postgres::DbTx<'_>) {
/// let view = runledger_postgres::PgTransactionView { transaction: tx };
/// # }
/// ```
/// The borrowed resource cannot be replaced while the view is retained:
/// ```compile_fail
/// use runledger_postgres::{DbTx, PgTransactionView, PgTransactionExecutor};
/// fn replace<'a>(tx: &mut DbTx<'a>, other: DbTx<'a>) {
///     let mut view = PgTransactionView::new(tx);
///     let _old = std::mem::replace(tx, other);
///     let _executor = view.executor();
/// }
/// ```
#[must_use]
pub struct PgTransactionView<'borrow, 'transaction> {
    transaction: &'borrow mut DbTx<'transaction>,
}

impl<'borrow, 'transaction> PgTransactionView<'borrow, 'transaction> {
    /// Exclusively borrow one native transaction; no executor provider is accepted.
    pub fn new(transaction: &'borrow mut DbTx<'transaction>) -> Self {
        Self { transaction }
    }
}

impl sealed::Transaction for PgTransactionView<'_, '_> {}
impl PgTransactionExecutor for PgTransactionView<'_, '_> {
    fn executor(&mut self) -> impl Executor<'_, Database = Postgres> {
        &mut **self.transaction
    }
}

/// An opaque exclusive borrow of one native PostgreSQL connection.
///
/// Schema verification consumes this view and retains the same connection for
/// every query. This value exposes neither a routing executor nor native identity.
/// Owners retain responsibility for cancellation and disposition.
///
/// A pool-backed downstream wrapper cannot create a session view:
/// ```compile_fail
/// use runledger_postgres::PgSessionView;
/// struct PoolSession(sqlx::PgPool);
/// impl PoolSession {
///     fn view(&mut self) -> PgSessionView<'_> {
///         PgSessionView::new(&mut self.0)
///     }
/// }
/// ```
/// A routing executor cannot create one either:
/// ```compile_fail
/// use runledger_postgres::PgSessionView;
/// struct RoutingSession(sqlx::PgConnection, sqlx::PgConnection);
/// impl RoutingSession {
///     fn view(&mut self) -> PgSessionView<'_> {
///         PgSessionView::new(self)
///     }
/// }
/// ```
/// No external code can forge the representation:
/// ```compile_fail
/// # fn forge(connection: &mut sqlx::PgConnection) {
/// let view = runledger_postgres::PgSessionView { connection };
/// # }
/// ```
/// Session replacement is excluded until verification releases the borrow:
/// ```compile_fail
/// use runledger_postgres::{PgSessionView, ensure_schema_compatible_after_idempotency_cutover_with_session};
/// async fn replace(conn: &mut sqlx::PgConnection, other: sqlx::PgConnection) {
///     let check = ensure_schema_compatible_after_idempotency_cutover_with_session(PgSessionView::new(conn));
///     let _old = std::mem::replace(conn, other);
///     check.await.unwrap();
/// }
/// ```
#[must_use]
pub struct PgSessionView<'connection> {
    connection: &'connection mut PgConnection,
}

impl<'connection> PgSessionView<'connection> {
    /// Exclusively borrow one connection; pools and executor providers are rejected.
    pub fn new(connection: &'connection mut PgConnection) -> Self {
        Self { connection }
    }

    pub(crate) fn into_connection(self) -> &'connection mut PgConnection {
        self.connection
    }
}
