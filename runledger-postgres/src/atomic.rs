//! Acknowledged atomic workflows with a one-way intent-to-queue phase.
mod intent;
use crate::{
    Error, RunledgerDatabase,
    jobs::{JobEnqueue, JobEnqueueIntent, JobEnqueueIntentOutcome, JobEnqueueOutcome},
};
use batter_sqlx::PgAtomicScope;
pub use batter_sqlx::{
    PgAtomicError, PgAtomicUncertainty, PgScopeError, PgScopeFailure, PgScopeLoss, PgScopedSql,
    PgTransactionError,
};
pub use intent::{AcceptedIntentOutcome, AcceptedIntentState, IntentConflict, RequiredIntentError};

/// Run application writes and Runledger operations in one owned transaction.
/// Outputs leave this runner only after acknowledged commit; rejections only
/// after acknowledged rollback. Uncertainty retains the provisional result.
/// Cancellation retires the connection and returns no result, not rollback proof.
///
/// Record intents in the initial phase, then consume it with [`PgIntentScope::queue`]
/// before enqueueing. Direct SQL against Runledger tables is a low-level escape
/// hatch: named operations enforce lock ordering, arbitrary SQL text cannot.
///
/// ```no_run
/// # async fn example(pool: &runledger_postgres::RunledgerDatabase, intent: &runledger_postgres::jobs::JobEnqueueIntent<'_>) -> Result<(), Box<dyn std::error::Error>> {
/// let outcome = runledger_postgres::run_atomic(pool, async |mut scope| {
///     scope.record_required_job_enqueue_intent(intent).await
/// }).await?;
/// // The intent is committed; no separately paired completion token is needed.
/// # let _ = outcome;
/// # Ok(()) }
/// ```
pub async fn run_atomic<T, E>(
    database: &RunledgerDatabase,
    work: impl AsyncFnOnce(PgIntentScope<'_>) -> Result<T, E>,
) -> Result<T, PgAtomicError<T, E>> {
    batter_sqlx::run_atomic_profiled(database.pool(), database.profile(), async |inner| {
        work(PgIntentScope { inner }).await
    })
    .await
}

/// Initial phase: intent recording is available, queue-row operations are not.
/// There is no public constructor or conversion back from the queue phase.
///
/// ```compile_fail,E0451
/// fn forge(inner: &mut batter_sqlx::PgAtomicScope) {
///     let _ = runledger_postgres::PgIntentScope { inner };
/// }
/// ```
/// ```compile_fail,E0382
/// # async fn example(pool: &runledger_postgres::RunledgerDatabase, intent: &runledger_postgres::jobs::JobEnqueueIntent<'_>) {
/// runledger_postgres::run_atomic(pool, async |mut scope| {
///     let queue = scope.queue();
///     scope.record_required_job_enqueue_intent(intent).await
/// }).await;
/// # }
/// ```
pub struct PgIntentScope<'a> {
    inner: &'a mut PgAtomicScope,
}

impl<'a> PgIntentScope<'a> {
    /// Run application SQL inside a protected savepoint. Returned values remain
    /// provisional inside the runner. Raw SQL must not bypass queue ordering.
    pub async fn application<T, E>(
        &mut self,
        work: impl AsyncFnOnce(&mut PgScopedSql<'_>) -> Result<T, E>,
    ) -> Result<T, PgScopeError<E>> {
        self.inner.application(work).await
    }

    /// Require an accepted handoff before any named queue-row operation.
    /// A known conflict is a rejection, never an ordinary successful observation.
    pub async fn record_required_job_enqueue_intent(
        &mut self,
        intent: &JobEnqueueIntent<'_>,
    ) -> Result<AcceptedIntentOutcome, PgScopeError<RequiredIntentError>> {
        self.inner
            .application(async |sql| {
                let outcome = crate::jobs::record_job_enqueue_intent_in_transaction(sql, intent)
                    .await
                    .map_err(RequiredIntentError::Storage)?;
                AcceptedIntentOutcome::require(outcome)
            })
            .await
    }

    /// Low-level observation that deliberately permits a conflicted outcome.
    /// Use only when committing despite that observation is application policy;
    /// required durable handoffs must use record_required_job_enqueue_intent.
    pub async fn observe_job_enqueue_intent(
        &mut self,
        intent: &JobEnqueueIntent<'_>,
    ) -> Result<JobEnqueueIntentOutcome, PgScopeError<Error>> {
        self.inner
            .application(async |sql| {
                crate::jobs::record_job_enqueue_intent_in_transaction(sql, intent).await
            })
            .await
    }

    /// Irreversibly end intent recording for this transaction.
    pub fn queue(self) -> PgQueueScope<'a> {
        PgQueueScope { inner: self.inner }
    }
}

/// Queue phase: enqueue and application SQL, but no further intent recording.
///
/// Enqueue-then-record cannot be expressed through named operations:
/// ```compile_fail,E0599
/// # async fn example(pool: &runledger_postgres::RunledgerDatabase, request: &runledger_postgres::jobs::JobEnqueue<'_>, intent: &runledger_postgres::jobs::JobEnqueueIntent<'_>) {
/// runledger_postgres::run_atomic(pool, async |scope| {
///     let mut queue = scope.queue();
///     queue.enqueue_job(request).await?;
///     queue.record_required_job_enqueue_intent(intent).await
/// }).await;
/// # }
/// ```
/// A scope cannot escape the runner or be committed independently:
/// ```compile_fail
/// # async fn example(pool: &runledger_postgres::RunledgerDatabase) {
/// runledger_postgres::run_atomic(pool, async |scope| Ok::<_, ()>(scope.queue())).await;
/// # }
/// ```
pub struct PgQueueScope<'a> {
    inner: &'a mut PgAtomicScope,
}

impl PgQueueScope<'_> {
    /// Run application SQL inside its own protected savepoint.
    pub async fn application<T, E>(
        &mut self,
        work: impl AsyncFnOnce(&mut PgScopedSql<'_>) -> Result<T, E>,
    ) -> Result<T, PgScopeError<E>> {
        self.inner.application(work).await
    }

    /// Enqueue after leaving the intent phase. Output is provisional in the body.
    pub async fn enqueue_job(
        &mut self,
        request: &JobEnqueue<'_>,
    ) -> Result<JobEnqueueOutcome, PgScopeError<Error>> {
        self.inner
            .application(async |sql| {
                crate::jobs::enqueue_job_with_outcome_in_transaction(sql, request).await
            })
            .await
    }

    /// Enqueue with a resource after leaving the intent phase.
    pub async fn enqueue_job_with_execution_resource(
        &mut self,
        request: &JobEnqueue<'_>,
        resource: &str,
    ) -> Result<JobEnqueueOutcome, PgScopeError<Error>> {
        self.inner
            .application(async |sql| {
                crate::jobs::enqueue_job_with_execution_resource_in_transaction(
                    sql, request, resource,
                )
                .await
            })
            .await
    }
}
