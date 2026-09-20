//! Application writes and Runledger operations under one consuming owner.
use crate::{
    DbPool, Error,
    jobs::{JobEnqueue, JobEnqueueIntent, JobEnqueueIntentOutcome, JobEnqueueOutcome},
};
pub use batter_sqlx::{
    CommitUnconfirmed as AtomicCommitUnconfirmed, PgCommitConfirmed, PgRollbackConfirmed,
    PgScopeError, PgScopedSql, PgTransactionError,
};

/// Owned READ COMMITTED transaction with application and Runledger operations.
///
/// Each method consumes the owner. Cancellation retires it; Runledger operations
/// share native SQL implementations but own savepoint cleanup and continuity.
/// Outcomes remain provisional until commit is acknowledged.
/// Native `DbTx` APIs are low-level persistence boundaries, not substitutes for
/// this continuity and cancellation contract.
///
/// ```compile_fail
/// let tx = runledger_postgres::PgAtomicTransaction { inner: todo!() };
/// ```
/// ```compile_fail
/// fn extract(tx: &mut runledger_postgres::PgAtomicTransaction) {
///     let _executor = tx.executor();
/// }
/// ```
///
/// ```no_run
/// # async fn example(pool: &runledger_postgres::DbPool, intent: &runledger_postgres::jobs::JobEnqueueIntent<'_>) -> Result<(), Box<dyn std::error::Error>> {
/// let tx = runledger_postgres::PgAtomicTransaction::begin(pool).await?;
/// let (tx, ()) = tx.application(async |sql| {
///     sqlx::query("INSERT INTO application_audit(message) VALUES ('queued')")
///         .execute(sql.executor()).await?;
///     Ok::<_, sqlx::Error>(())
/// }).await?;
/// let (tx, outcome) = tx.record_job_enqueue_intent(intent).await?;
/// let confirmed = tx.commit().await?;
/// # let _ = (outcome, confirmed);
/// # Ok(()) }
/// ```
#[derive(Debug)]
#[must_use]
pub struct PgAtomicTransaction {
    inner: batter_sqlx::PgAtomicTransaction,
}

impl PgAtomicTransaction {
    /// Acquire and establish a transaction with identity assigned at birth.
    pub async fn begin(pool: &DbPool) -> Result<Self, PgTransactionError> {
        Ok(Self {
            inner: batter_sqlx::PgAtomicTransaction::begin(pool).await?,
        })
    }

    /// Consume the owner for application SQL; application failure is terminal.
    pub async fn application<T, E>(
        self,
        work: impl AsyncFnOnce(&mut PgScopedSql<'_>) -> Result<T, E>,
    ) -> Result<(Self, T), PgScopeError<E>> {
        let (inner, value) = self.inner.application(work).await?;
        Ok((Self { inner }, value))
    }

    /// An inner error returns a reusable owner only after savepoint rollback
    /// and continuity validation; an outer error consumes it permanently.
    pub async fn operation<T, E>(
        self,
        work: impl AsyncFnOnce(&mut PgScopedSql<'_>) -> Result<T, E>,
    ) -> Result<(Self, Result<T, E>), PgScopeError<E>> {
        let (inner, result) = self.inner.operation(work).await?;
        Ok((Self { inner }, result))
    }

    /// Record an intent before queue-row operations to preserve lock order.
    pub async fn record_job_enqueue_intent(
        self,
        intent: &JobEnqueueIntent<'_>,
    ) -> Result<(Self, JobEnqueueIntentOutcome), PgScopeError<Error>> {
        self.application(async |sql| {
            crate::jobs::record_job_enqueue_intent_in_transaction(sql, intent).await
        })
        .await
    }

    /// Enqueue with application writes under a library-owned savepoint.
    pub async fn enqueue_job(
        self,
        request: &JobEnqueue<'_>,
    ) -> Result<(Self, JobEnqueueOutcome), PgScopeError<Error>> {
        self.application(async |sql| {
            crate::jobs::enqueue_job_with_outcome_in_transaction(sql, request).await
        })
        .await
    }

    /// Enqueue with a resource under the same owned transaction.
    pub async fn enqueue_job_with_execution_resource(
        self,
        request: &JobEnqueue<'_>,
        resource: &str,
    ) -> Result<(Self, JobEnqueueOutcome), PgScopeError<Error>> {
        self.application(async |sql| {
            crate::jobs::enqueue_job_with_execution_resource_in_transaction(sql, request, resource)
                .await
        })
        .await
    }

    /// Consume the transaction; success is explicit commit acknowledgement.
    pub async fn commit(self) -> Result<PgCommitConfirmed, AtomicCommitUnconfirmed> {
        self.inner.commit().await
    }

    /// Consume the transaction; success is explicit rollback acknowledgement.
    pub async fn rollback(self) -> Result<PgRollbackConfirmed, PgTransactionError> {
        self.inner.rollback().await
    }
}
