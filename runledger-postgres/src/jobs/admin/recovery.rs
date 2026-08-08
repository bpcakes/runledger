use std::future::Future;

use runledger_core::jobs::WorkflowStepStatus;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::errors::{
    cancellation_not_quiesced_error, ensure_rejection_rollback_succeeded, invalid_job_state_error,
    job_not_found_error, workflow_requeue_not_supported_error,
};
use super::super::queue::advance::{
    AdvanceJobToNextRun, JOB_QUEUE_COLUMNS_SQL, advance_locked_job_to_next_run_tx,
};
use super::super::queue::events::{
    RequeuedEventPayload, RequeuedJobEvent, insert_requeued_event_tx,
};
use super::super::rows::JobQueueRow;
use super::super::transaction_isolation::{
    ReadCommittedTx, begin_owned_read_committed_tx, ensure_read_committed_tx,
    finish_owned_transaction,
};
use super::super::types::{CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobQueueRecord};
use super::super::workflows::on_terminal;
use super::read::get_job_by_id;

async fn rollback_and_classify_missing_job_mutation(
    tx: DbTx<'_>,
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Error> {
    if let Err(error) = tx.rollback().await {
        tracing::warn!(error = %error, "failed to rollback missing job mutation transaction");
    }
    let exists = get_job_by_id(pool, organization_id, job_id).await?;
    Ok(if exists.is_none() {
        job_not_found_error()
    } else {
        invalid_job_state_error()
    })
}

async fn workflow_managed_job_exists_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar!(
        "SELECT EXISTS (
            SELECT 1
            FROM job_queue jq
            WHERE jq.id = $1
              AND jq.workflow_step_id IS NOT NULL
              AND ($2::uuid IS NULL OR jq.organization_id = $2)
         ) AS \"exists!\"",
        job_id,
        organization_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("requeue workflow-managed job check", error)
    })?;

    Ok(exists)
}

pub async fn cancel_job(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<JobQueueRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let Some(record) = cancel_job_tx(&mut tx, organization_id, job_id, reason).await? else {
        return Err(
            rollback_and_classify_missing_job_mutation(tx, pool, organization_id, job_id).await?,
        );
    };

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(record)
}

pub(crate) async fn cancel_job_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<Option<JobQueueRecord>> {
    // Preserve a live lease's original expiry as a cancellation-quiescence
    // marker. Status fencing rejects every subsequent worker write immediately,
    // while compare-and-requeue waits until this marker has passed before it
    // can start a new run. Pending jobs already have a NULL marker.
    let row = sqlx::query_as!(
        JobQueueRow,
        "UPDATE job_queue
         SET status = 'CANCELED',
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             output = NULL,
             status_reason = COALESCE($3, 'CANCELED'),
             updated_at = now()
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
           AND status IN ('PENDING', 'LEASED')
         RETURNING
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at",
        job_id,
        organization_id,
        reason,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("cancel job", error))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let record = row.into_record()?;

    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now()
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3
           AND finished_at IS NULL",
        record.id,
        record.run_number,
        record.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("close canceled attempt", error))?;

    let event_attempt = (record.attempt > 0).then_some(record.attempt);
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            payload
         )
         VALUES (
            $1,
            $2,
            $3,
            'CANCELED',
            jsonb_strip_nulls(jsonb_build_object(
                'reason', $4::text,
                'lease_quiesces_at', $5::timestamptz
            ))
         )",
        record.id,
        record.run_number,
        event_attempt,
        record.status_reason.as_deref(),
        record.lease_expires_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert canceled event", error))?;

    on_terminal(
        tx,
        record.id,
        WorkflowStepStatus::Canceled,
        record.status_reason.as_deref(),
        None,
        None,
        None,
    )
    .await?;

    Ok(Some(record))
}

#[derive(sqlx::FromRow)]
struct CompareAndRequeueCandidateRow {
    #[sqlx(flatten)]
    job: JobQueueRow,
    workflow_step_id: Option<Uuid>,
    canceled_lease_still_active: bool,
}

struct CompareAndRequeueCandidate {
    job: JobQueueRecord,
    workflow_managed: bool,
    canceled_lease_still_active: bool,
}

fn compare_and_requeue_candidate_from_row(
    row: CompareAndRequeueCandidateRow,
) -> Result<CompareAndRequeueCandidate> {
    let job = row.job.into_record()?;
    let workflow_managed = row.workflow_step_id.is_some();
    Ok(CompareAndRequeueCandidate {
        job,
        workflow_managed,
        canceled_lease_still_active: row.canceled_lease_still_active,
    })
}

async fn lock_compare_and_requeue_candidate_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<Option<JobQueueRecord>> {
    // Requeue never changes the job's identity, so NO KEY UPDATE is sufficient
    // and composes with the legacy keyed-enqueue path's KEY SHARE lock.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS canceled_lease_still_active
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid
           AND status::text = $3::text
           AND run_number = $4::int4
           AND workflow_step_id IS NULL
           AND NOT (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
           )
         FOR NO KEY UPDATE"
    );
    let row = sqlx::query_as::<_, CompareAndRequeueCandidateRow>(&sql)
        .bind(request.job_id)
        .bind(request.scope.organization_id())
        .bind(request.expected_status.as_db_value())
        .bind(request.expected_run_number)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock compare-and-requeue job", error)
        })?;

    let Some(candidate) = row
        .map(compare_and_requeue_candidate_from_row)
        .transpose()?
    else {
        return Ok(None);
    };
    debug_assert!(!candidate.workflow_managed);
    debug_assert!(!candidate.canceled_lease_still_active);
    Ok(Some(candidate.job))
}

async fn load_compare_and_requeue_candidate_for_classification_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<Option<CompareAndRequeueCandidate>> {
    // A mismatch/no-mutation read deliberately omits row locking so a
    // caller-owned transaction cannot stall a live worker or an operator
    // acting on a rejected row.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS canceled_lease_still_active
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid"
    );
    let row = sqlx::query_as::<_, CompareAndRequeueCandidateRow>(&sql)
        .bind(request.job_id)
        .bind(request.scope.organization_id())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("read compare-and-requeue mismatch", error)
        })?;

    row.map(compare_and_requeue_candidate_from_row).transpose()
}

async fn update_compare_and_requeue_candidate_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<JobQueueRecord> {
    advance_locked_job_to_next_run_tx(
        tx,
        &AdvanceJobToNextRun {
            job_id: request.job_id,
            preserve_missing_resume_state: request.state_policy.preserves_progress_and_checkpoint(),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            next_run_at: None,
            status_reason: Some(request.reason),
        },
        "compare and requeue job",
    )
    .await
}

/// Atomically requeues an exactly observed canceled or dead-lettered job in an
/// internally owned `READ COMMITTED` transaction.
///
/// Prefer this convenience API when recovery does not need to compose with
/// other database changes. Use [`compare_and_requeue_job_tx`] when the mutation
/// must be part of a caller-owned transaction.
///
/// Every normal [`CompareAndRequeueJobOutcome`] is committed before it is
/// returned. Database errors are rolled back before returning to the caller.
pub async fn compare_and_requeue_job(
    pool: &DbPool,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    const OPERATION: &str = "compare-and-requeue";

    let mut tx = begin_owned_read_committed_tx(pool, OPERATION).await?;
    let result = {
        let mut read_committed_tx = tx.as_read_committed_tx();
        compare_and_requeue_job_read_committed_tx(&mut read_committed_tx, request).await
    };
    finish_owned_transaction(tx, OPERATION, result).await
}

/// Atomically requeues an exactly scoped canceled or dead-lettered job only if
/// its terminal status and run number still match the caller's observation.
/// `state_policy` explicitly controls whether committed progress/checkpoint
/// state is carried into the new run or cleared.
///
/// The caller transaction must use `READ COMMITTED`. A mismatch against a live
/// row is read without taking a row lock. If cancellation fenced a leased
/// handler whose original lease window is still active, this returns
/// [`CompareAndRequeueJobOutcome::CancellationNotQuiesced`] instead of starting
/// an overlapping run.
///
/// The caller owns `tx`; this function neither commits nor rolls it back.
/// Missing rows, stale expectations, active cancellation fences, and workflow
/// rejections do not leave the job row locked in the caller transaction.
pub async fn compare_and_requeue_job_tx(
    tx: &mut DbTx<'_>,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    let mut read_committed_tx = ensure_read_committed_tx(
        tx,
        "job compare-and-requeue",
        "job.compare_and_requeue_unsupported_isolation",
        "Job compare-and-requeue requires READ COMMITTED transaction isolation.",
    )
    .await?;

    compare_and_requeue_job_read_committed_tx(&mut read_committed_tx, request).await
}

async fn compare_and_requeue_job_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    compare_and_requeue_job_read_committed_tx_inner(tx, request, || {
        std::future::ready(Ok::<(), Error>(()))
    })
    .await
}

async fn compare_and_requeue_job_read_committed_tx_inner<AfterLockMiss, AfterLockMissFuture>(
    tx: &mut ReadCommittedTx<'_, '_>,
    request: CompareAndRequeueJob<'_>,
    mut after_lock_miss: AfterLockMiss,
) -> Result<CompareAndRequeueJobOutcome>
where
    AfterLockMiss: FnMut() -> AfterLockMissFuture,
    AfterLockMissFuture: Future<Output = Result<()>>,
{
    let tx = tx.as_tx();
    let before = loop {
        if let Some(before) = lock_compare_and_requeue_candidate_tx(tx, &request).await? {
            break before;
        }

        after_lock_miss().await?;
        let Some(actual) =
            load_compare_and_requeue_candidate_for_classification_tx(tx, &request).await?
        else {
            return Ok(CompareAndRequeueJobOutcome::NotFound);
        };
        if actual.job.status == request.expected_status.as_job_status()
            && actual.job.run_number == request.expected_run_number
        {
            if actual.workflow_managed {
                return Err(workflow_requeue_not_supported_error());
            }

            if let (true, Some(retry_after)) = (
                actual.canceled_lease_still_active,
                actual.job.lease_expires_at,
            ) {
                return Ok(CompareAndRequeueJobOutcome::CancellationNotQuiesced {
                    actual: Box::new(actual.job),
                    retry_after,
                });
            }

            // READ COMMITTED gives each statement a fresh snapshot. The row may
            // have become mutation-eligible after the locking read missed it,
            // so retry instead of returning a contradictory mismatch whose
            // actual state equals the caller's expectation.
            continue;
        }

        return Ok(CompareAndRequeueJobOutcome::ExpectationMismatch {
            actual: Box::new(actual.job),
        });
    };
    let after = update_compare_and_requeue_candidate_tx(tx, &request).await?;

    let event_attempt = (before.attempt > 0).then_some(before.attempt);
    let event_id = insert_requeued_event_tx(
        tx,
        RequeuedJobEvent {
            job_id: before.id,
            completed_run_number: before.run_number,
            attempt: event_attempt,
            stage: None,
            progress_done: None,
            progress_total: None,
            payload: RequeuedEventPayload::CompareAndRequeue {
                reason: request.reason,
                state_policy: request.state_policy,
            },
        },
        "insert compare-and-requeue event",
    )
    .await?;

    Ok(CompareAndRequeueJobOutcome::Requeued {
        before: Box::new(before),
        after: Box::new(after),
        event_id,
    })
}

#[deprecated(
    since = "0.6.0",
    note = "use compare_and_requeue_job (or compare_and_requeue_job_tx for caller-owned transactions) with exact JobScope and RequeueableJobStatus expectations"
)]
pub async fn requeue_job(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<JobQueueRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let workflow_managed = workflow_managed_job_exists_tx(&mut tx, job_id, organization_id).await?;
    if workflow_managed {
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(workflow_requeue_not_supported_error());
    }

    let previous_run = sqlx::query!(
        "SELECT
            run_number,
            attempt,
            lease_expires_at,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS \"canceled_lease_still_active!\"
         FROM job_queue
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
           AND status IN ('DEAD_LETTERED', 'CANCELED', 'SUCCEEDED')
         FOR UPDATE",
        job_id,
        organization_id,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("requeue job prefetch attempt", error))?;

    let Some(previous_run) = previous_run else {
        return Err(
            rollback_and_classify_missing_job_mutation(tx, pool, organization_id, job_id).await?,
        );
    };
    let previous_run_number: i32 = previous_run.run_number;
    let previous_attempt: i32 = previous_run.attempt;
    if let (true, Some(retry_after)) = (
        previous_run.canceled_lease_still_active,
        previous_run.lease_expires_at,
    ) {
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(cancellation_not_quiesced_error(retry_after));
    }

    let record = advance_locked_job_to_next_run_tx(
        &mut tx,
        &AdvanceJobToNextRun {
            job_id,
            preserve_missing_resume_state: false,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            next_run_at: None,
            status_reason: reason,
        },
        "requeue job",
    )
    .await?;

    let event_attempt = (previous_attempt > 0).then_some(previous_attempt);
    insert_requeued_event_tx(
        &mut tx,
        RequeuedJobEvent {
            job_id: record.id,
            completed_run_number: previous_run_number,
            attempt: event_attempt,
            stage: None,
            progress_done: None,
            progress_total: None,
            payload: RequeuedEventPayload::Basic {
                reason: record.status_reason.as_deref().unwrap_or("REQUEUED"),
            },
        },
        "insert requeued event",
    )
    .await?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(record)
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{JobFailureKind, JobStatus, JobType};
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
    use serde_json::json;

    use super::compare_and_requeue_job_read_committed_tx_inner;
    use crate::jobs::transaction_isolation::ensure_read_committed_tx;
    use crate::jobs::{
        CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobDefinitionUpsert, JobEnqueue,
        JobFailureUpdate, JobRequeueStatePolicy, JobScope, RequeueableJobStatus, claim_jobs,
        complete_job_failure, enqueue_job, upsert_job_definition_tx,
    };

    #[tokio::test]
    async fn compare_and_requeue_retries_lock_when_later_snapshot_matches_expectation() {
        const JOB_TYPE: &str = "jobs.test.compare_requeue_snapshot_race";

        let (pool, database) =
            setup_ephemeral_pool("postgres_compare_requeue_snapshot_race", 4).await;
        let mut definition_tx = pool.begin().await.expect("begin definition transaction");
        upsert_job_definition_tx(
            &mut definition_tx,
            &JobDefinitionUpsert {
                job_type: JobType::new(JOB_TYPE),
                version: 1,
                max_attempts: 3,
                default_timeout_seconds: 60,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert job definition");
        definition_tx.commit().await.expect("commit job definition");

        let payload = json!({ "case": "snapshot-race" });
        let job_id = enqueue_job(
            &pool,
            &JobEnqueue {
                job_type: JobType::new(JOB_TYPE),
                organization_id: None,
                payload: &payload,
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            },
        )
        .await
        .expect("enqueue job");
        let claim = claim_jobs(&pool, "worker-snapshot-race", 30, 1)
            .await
            .expect("claim job")
            .pop()
            .expect("one job should be claimed");
        let worker_id = claim
            .worker_id
            .clone()
            .expect("claimed job should have a worker id");

        let mut transition = Some((claim.id, claim.run_number, claim.attempt, worker_id));
        let transition_pool = pool.clone();
        let mut recovery_tx = pool.begin().await.expect("begin recovery transaction");
        let outcome = {
            let mut read_committed_tx = ensure_read_committed_tx(
                &mut recovery_tx,
                "compare-and-requeue snapshot race test",
                "test.compare_and_requeue_unsupported_isolation",
                "Test requires READ COMMITTED transaction isolation.",
            )
            .await
            .expect("validate recovery transaction isolation");
            compare_and_requeue_job_read_committed_tx_inner(
                &mut read_committed_tx,
                CompareAndRequeueJob {
                    scope: JobScope::Global,
                    job_id,
                    expected_status: RequeueableJobStatus::DeadLettered,
                    expected_run_number: claim.run_number,
                    state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                    reason: "recover terminal transition observed by later snapshot",
                },
                || {
                    let transition = transition.take();
                    let transition_pool = transition_pool.clone();
                    async move {
                        if let Some((job_id, run_number, attempt, worker_id)) = transition {
                            complete_job_failure(
                                &transition_pool,
                                job_id,
                                run_number,
                                attempt,
                                &worker_id,
                                &JobFailureUpdate::new(
                                    JobFailureKind::Terminal,
                                    "job.test.snapshot_race",
                                    "terminal transition between recovery reads",
                                    None,
                                ),
                            )
                            .await?;
                        }
                        Ok(())
                    }
                },
            )
            .await
            .expect("recovery should retry the exact lock")
        };

        let CompareAndRequeueJobOutcome::Requeued { before, after, .. } = outcome else {
            panic!("later exact snapshot must requeue instead of returning mismatch");
        };
        assert_eq!(before.status, JobStatus::DeadLettered);
        assert_eq!(before.run_number, claim.run_number);
        assert_eq!(after.status, JobStatus::Pending);
        assert_eq!(after.run_number, claim.run_number + 1);
        recovery_tx.commit().await.expect("commit recovery");

        teardown_ephemeral_pool(pool, database).await;
    }
}
