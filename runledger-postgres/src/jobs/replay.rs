use runledger_core::jobs::{JobStage, JobStatus};
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::errors::{
    job_replay_idempotency_conflict_error, validate_job_replay_request,
    workflow_requeue_not_supported_error,
};
use super::queue::advance::JOB_QUEUE_COLUMNS_SQL;
use super::queue::enqueue_replayed_job_with_outcome_tx;
use super::queue::events::EnqueuedEventPayload;
use super::row_decode::parse_job_status;
use super::rows::JobQueueRow;
use super::transaction_isolation::{
    begin_owned_read_committed_tx, ensure_read_committed_tx, finish_owned_transaction,
};
use super::types::{
    JobEnqueue, JobEnqueueDisposition, JobEnqueueOutcome, JobQueueRecord, JobScope,
};

/// An idempotent request to execute a successful direct job again as a fresh
/// logical job.
///
/// This operation does not mutate the source row. Replay creates a new job ID
/// and copies only the source payload and effective execution settings;
/// progress, checkpoint, output, terminal timestamps, workflow ownership, and
/// the original idempotency key are never copied.
#[derive(Debug, Clone)]
pub struct CompareAndReplaySucceededJob<'a> {
    pub scope: JobScope,
    pub source_job_id: Uuid,
    pub expected_run_number: i32,
    /// Stable identity for one replay action. Retrying the same action must use
    /// the same key; another intentional replay must use a different key.
    pub replay_request_key: &'a str,
    pub reason: &'a str,
}

/// Result of compare-and-replay.
#[derive(Debug, Clone)]
#[must_use = "callers must inspect whether the expected successful job was replayed"]
#[non_exhaustive]
pub enum CompareAndReplaySucceededJobOutcome {
    /// The replay exists. `replay.disposition` distinguishes a newly inserted
    /// replay from an idempotent retry that resolved to the existing replay.
    Replayed {
        source_job_id: Uuid,
        source_run_number: i32,
        replay: JobEnqueueOutcome,
    },
    /// The exactly scoped source exists but no longer matches the successful
    /// run observed by the caller.
    ExpectationMismatch { actual: Box<JobQueueRecord> },
    /// No source job exists in the exact requested scope.
    NotFound,
}

#[derive(sqlx::FromRow)]
struct ReplayCandidateRow {
    #[sqlx(flatten)]
    job: JobQueueRow,
    workflow_step_id: Option<Uuid>,
    execution_resource_key: Option<String>,
}

struct ReplayCandidate {
    job: JobQueueRecord,
    workflow_managed: bool,
    execution_resource_key: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ExistingReplayRow {
    replay_job_id: Uuid,
    replay_status: String,
    replay_run_number: i32,
}

async fn load_matching_existing_replay_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<Option<ExistingReplayRow>> {
    sqlx::query_as::<_, ExistingReplayRow>(
        "SELECT
            replay.id AS replay_job_id,
            replay.status::text AS replay_status,
            replay.run_number AS replay_run_number
         FROM job_replays jr
         JOIN job_queue source ON source.id = jr.source_job_id
         JOIN job_queue replay ON replay.id = jr.replay_job_id
         WHERE jr.source_job_id = $1
           AND jr.source_run_number = $2
           AND jr.replay_request_key = $3
           AND source.organization_id IS NOT DISTINCT FROM $4::uuid
           AND jr.reason = $5
         FOR NO KEY UPDATE OF replay",
    )
    .bind(request.source_job_id)
    .bind(request.expected_run_number)
    .bind(request.replay_request_key)
    .bind(request.scope.organization_id())
    .bind(request.reason)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("load existing job replay", error))
}

async fn conflicting_replay_request_exists_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM job_replays jr
            JOIN job_queue source ON source.id = jr.source_job_id
            WHERE jr.source_job_id = $1
              AND jr.source_run_number = $2
              AND jr.replay_request_key = $3
              AND source.organization_id IS NOT DISTINCT FROM $4::uuid
         )",
    )
    .bind(request.source_job_id)
    .bind(request.expected_run_number)
    .bind(request.replay_request_key)
    .bind(request.scope.organization_id())
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("classify existing job replay request", error)
    })
}

fn existing_replay_outcome(
    request: &CompareAndReplaySucceededJob<'_>,
    existing: ExistingReplayRow,
) -> Result<CompareAndReplaySucceededJobOutcome> {
    Ok(CompareAndReplaySucceededJobOutcome::Replayed {
        source_job_id: request.source_job_id,
        source_run_number: request.expected_run_number,
        replay: JobEnqueueOutcome {
            job_id: existing.replay_job_id,
            status: parse_job_status(existing.replay_status)?,
            run_number: existing.replay_run_number,
            disposition: JobEnqueueDisposition::Existing,
        },
    })
}

async fn load_or_classify_existing_replay_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<Option<CompareAndReplaySucceededJobOutcome>> {
    if let Some(existing) = load_matching_existing_replay_tx(tx, request).await? {
        return existing_replay_outcome(request, existing).map(Some);
    }
    if conflicting_replay_request_exists_tx(tx, request).await? {
        return Err(job_replay_idempotency_conflict_error());
    }
    Ok(None)
}

fn replay_candidate_from_row(row: ReplayCandidateRow) -> Result<ReplayCandidate> {
    let job = row.job.into_record()?;
    let workflow_managed = row.workflow_step_id.is_some();
    Ok(ReplayCandidate {
        job,
        workflow_managed,
        execution_resource_key: row.execution_resource_key,
    })
}

async fn lock_eligible_replay_source_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<Option<ReplayCandidate>> {
    // Replay does not change the source's key, so NO KEY UPDATE composes with
    // the replay-lineage foreign-key insert.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            execution_resource_key
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid
           AND status = 'SUCCEEDED'
           AND run_number = $3::int4
           AND workflow_step_id IS NULL
         FOR NO KEY UPDATE"
    );
    let row = sqlx::query_as::<_, ReplayCandidateRow>(&sql)
        .bind(request.source_job_id)
        .bind(request.scope.organization_id())
        .bind(request.expected_run_number)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock successful job replay source", error)
        })?;

    row.map(replay_candidate_from_row).transpose()
}

async fn load_replay_source_for_classification_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<Option<ReplayCandidate>> {
    // A rejected or stale observation is read without retaining a row lock in
    // the caller transaction.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            execution_resource_key
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid"
    );
    let row = sqlx::query_as::<_, ReplayCandidateRow>(&sql)
        .bind(request.source_job_id)
        .bind(request.scope.organization_id())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("read successful job replay mismatch", error)
        })?;

    row.map(replay_candidate_from_row).transpose()
}

async fn lock_replay_source_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndReplaySucceededJob<'_>,
) -> Result<std::result::Result<ReplayCandidate, CompareAndReplaySucceededJobOutcome>> {
    loop {
        if let Some(candidate) = lock_eligible_replay_source_tx(tx, request).await? {
            debug_assert!(!candidate.workflow_managed);
            return Ok(Ok(candidate));
        }

        let Some(actual) = load_replay_source_for_classification_tx(tx, request).await? else {
            return Ok(Err(CompareAndReplaySucceededJobOutcome::NotFound));
        };
        if actual.job.status == JobStatus::Succeeded
            && actual.job.run_number == request.expected_run_number
        {
            if actual.workflow_managed {
                return Err(workflow_requeue_not_supported_error());
            }

            // READ COMMITTED gives the locking retry a fresh snapshot. If the
            // source became eligible between statements, retry rather than
            // report a contradictory mismatch.
            continue;
        }

        return Ok(Err(
            CompareAndReplaySucceededJobOutcome::ExpectationMismatch {
                actual: Box::new(actual.job),
            },
        ));
    }
}

/// Creates a fresh direct job from an exactly scoped successful source.
///
/// The caller transaction must use `READ COMMITTED`. This function neither
/// commits nor rolls back. Idempotent retries with the same source run,
/// `replay_request_key`, and reason return the existing replay job. Reusing a
/// replay key with a different reason returns
/// `job.replay_idempotency_conflict`.
pub async fn compare_and_replay_succeeded_job_tx(
    tx: &mut DbTx<'_>,
    request: CompareAndReplaySucceededJob<'_>,
) -> Result<CompareAndReplaySucceededJobOutcome> {
    validate_job_replay_request(request.replay_request_key, request.reason)?;
    ensure_read_committed_tx(
        tx,
        "successful job compare-and-replay",
        "job.compare_and_replay_unsupported_isolation",
        "Successful job replay requires READ COMMITTED transaction isolation.",
    )
    .await?;

    compare_and_replay_succeeded_job_read_committed_tx(tx, request).await
}

async fn compare_and_replay_succeeded_job_read_committed_tx(
    tx: &mut DbTx<'_>,
    request: CompareAndReplaySucceededJob<'_>,
) -> Result<CompareAndReplaySucceededJobOutcome> {
    if let Some(existing) = load_or_classify_existing_replay_tx(tx, &request).await? {
        return Ok(existing);
    }

    let source = match lock_replay_source_tx(tx, &request).await? {
        Ok(source) => source,
        Err(outcome) => return Ok(outcome),
    };

    // A concurrent replay with the same request key may have committed while
    // this transaction waited for the source lock.
    if let Some(existing) = load_or_classify_existing_replay_tx(tx, &request).await? {
        return Ok(existing);
    }

    let replay_payload = JobEnqueue {
        job_type: source.job.job_type.as_borrowed(),
        organization_id: source.job.organization_id,
        payload: &source.job.payload,
        priority: Some(source.job.priority),
        max_attempts: Some(source.job.max_attempts),
        timeout_seconds: Some(source.job.timeout_seconds),
        next_run_at: None,
        idempotency_key: None,
        stage: Some(JobStage::Queued),
    };
    let replay = enqueue_replayed_job_with_outcome_tx(
        tx,
        &replay_payload,
        source.execution_resource_key.as_deref(),
        EnqueuedEventPayload::SuccessfulReplay {
            replayed_from_job_id: source.job.id,
            replayed_from_run_number: source.job.run_number,
            replay_request_key: request.replay_request_key,
            reason: request.reason,
        },
    )
    .await?;
    debug_assert_eq!(replay.disposition, JobEnqueueDisposition::Inserted);

    sqlx::query(
        "INSERT INTO job_replays (
            source_job_id,
            source_run_number,
            replay_request_key,
            replay_job_id,
            reason
         )
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(source.job.id)
    .bind(source.job.run_number)
    .bind(request.replay_request_key)
    .bind(replay.job_id)
    .bind(request.reason)
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("record successful job replay", error))?;

    Ok(CompareAndReplaySucceededJobOutcome::Replayed {
        source_job_id: source.job.id,
        source_run_number: source.job.run_number,
        replay,
    })
}

/// Pool-owning convenience wrapper for [`compare_and_replay_succeeded_job_tx`].
///
/// Request identity is validated before this function acquires a connection or
/// begins a transaction. The caller-owned transaction API independently runs
/// the same validator so neither entry point can bypass the contract.
pub async fn compare_and_replay_succeeded_job(
    pool: &DbPool,
    request: CompareAndReplaySucceededJob<'_>,
) -> Result<CompareAndReplaySucceededJobOutcome> {
    const OPERATION: &str = "successful job replay";

    validate_job_replay_request(request.replay_request_key, request.reason)?;
    let mut tx = begin_owned_read_committed_tx(pool, OPERATION).await?;
    // `begin_owned_read_committed_tx` established the exact isolation required
    // by the operation body, so the pool-owned path need not query it again.
    let result = compare_and_replay_succeeded_job_read_committed_tx(&mut tx, request).await;
    finish_owned_transaction(tx, OPERATION, result).await
}
