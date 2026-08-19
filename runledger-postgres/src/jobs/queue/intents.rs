use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStage, JobType, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::error::SanitizedQueryErrorDiagnostics;
use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::super::errors::{validate_page_limit, validate_pagination};
use super::super::row_decode::{parse_job_stage, parse_job_type_name};
use super::super::rows::{
    JobEnqueueIntentOutcomeRow, JobEnqueueIntentRecordRow, SupportedJobEnqueueIntentPromotionRow,
};
use super::super::transaction_isolation::{
    ReadCommittedTx, begin_owned_read_committed_tx, ensure_read_committed_tx,
    finish_owned_transaction,
};
use super::super::transaction_settings::{
    cap_local_lock_timeout_tx, cap_local_statement_timeout_tx, cap_local_transaction_timeout_tx,
    set_local_lock_timeout_tx, set_local_statement_timeout_tx,
};
use super::super::types::{
    JobEnqueue, JobEnqueueDisposition, JobEnqueueIntent, JobEnqueueIntentDisposition,
    JobEnqueueIntentListFilter, JobEnqueueIntentMetricsFilter, JobEnqueueIntentMetricsRecord,
    JobEnqueueIntentOutcome, JobEnqueueIntentPromotionReport, JobEnqueueIntentRecord,
    JobEnqueueIntentStatus,
};
use super::enqueue::{
    IntentEnqueueResolution, JOB_ENQUEUE_REQUEST_VERSION, canonical_job_enqueue_request_v1,
    enqueue_job_from_intent_tx, validate_execution_resource_key,
};

const RECORD_OPERATION: &str = "record job enqueue intent";
const PROMOTE_OPERATION: &str = "promote job enqueue intents";
// The two-int advisory key space is distinct from the bigint key space used by
// workflow release locks. Advisory locks remain database-global, so a rare key
// collision with embedding-application locks only over-serializes work.
const RUNLEDGER_ADVISORY_LOCK_NAMESPACE: i32 = 0x7275_6e6c;
const JOB_ENQUEUE_INTENT_RETENTION_LOCK: i32 = 0x696e_7465;
const JOB_ENQUEUE_INTENT_PROMOTION_LOCK_TIMEOUT: &str = "5s";
const JOB_ENQUEUE_INTENT_PROMOTION_LOCK_TIMEOUT_MS: i64 = 5_000;
const JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT: &str = "25s";
const JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT_MS: i64 = 25_000;
const JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT: &str = "30s";
const JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT_MS: i64 = 30_000;
const JOB_ENQUEUE_INTENT_RETENTION_LOCK_TIMEOUT: &str = "5s";
const JOB_ENQUEUE_INTENT_RETENTION_LOCK_TIMEOUT_MS: i64 = 5_000;
const JOB_ENQUEUE_INTENT_RETENTION_STATEMENT_TIMEOUT: &str = "35s";
const JOB_ENQUEUE_INTENT_RETENTION_STATEMENT_TIMEOUT_MS: i64 = 35_000;
// These ordering constraints are the liveness contract between the shared and
// exclusive sides of the fence. Keep them compile-time checked when tuning.
const _: () = assert!(
    JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT_MS
        > JOB_ENQUEUE_INTENT_PROMOTION_LOCK_TIMEOUT_MS
);
const _: () = assert!(
    JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT_MS
        > JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT_MS
);
const _: () = assert!(
    JOB_ENQUEUE_INTENT_RETENTION_STATEMENT_TIMEOUT_MS
        > JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT_MS
);
const JOB_ENQUEUE_INTENT_RETENTION_BATCH_LIMIT_MAX: usize = 1_000;
// A failed row assigns two subtransaction IDs (the row savepoint and the
// rollback's replacement subtransaction). Capping at 24 leaves 16 IDs of
// headroom below PostgreSQL's 64 cached-subtransaction threshold. The enqueue
// path must not add nested savepoints without revisiting this bound. Larger
// batches also retain more intent, definition, and existing-job locks until
// the outer transaction commits.
const PROMOTION_BATCH_LIMIT_MAX: i64 = 24;

struct PreparedIntent<'a> {
    enqueue: JobEnqueue<'a>,
    execution_resource_key: Option<&'a str>,
    stage: &'static str,
    enqueue_request: Value,
}

struct IntentPromotionRequest {
    job_type: JobTypeName,
    organization_id: Option<Uuid>,
    payload: Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    next_run_at: Option<DateTime<Utc>>,
    idempotency_key: String,
    stage: JobStage,
    execution_resource_key: Option<String>,
}

impl IntentPromotionRequest {
    fn as_job_enqueue(&self) -> JobEnqueue<'_> {
        JobEnqueue {
            job_type: self.job_type.as_borrowed(),
            organization_id: self.organization_id,
            payload: &self.payload,
            priority: self.priority,
            max_attempts: self.max_attempts,
            timeout_seconds: self.timeout_seconds,
            next_run_at: self.next_run_at,
            idempotency_key: Some(&self.idempotency_key),
            stage: Some(self.stage),
        }
    }
}

struct PreparedIntentPromotion {
    id: Uuid,
    request: IntentPromotionRequest,
    current_enqueue_request: Value,
}

impl PreparedIntentPromotion {
    fn try_from_row(row: SupportedJobEnqueueIntentPromotionRow) -> Result<Self> {
        let request = IntentPromotionRequest {
            job_type: parse_job_type_name(row.job_type)?,
            organization_id: row.organization_id,
            payload: row.payload,
            priority: row.priority,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            next_run_at: row.next_run_at,
            idempotency_key: row.idempotency_key,
            stage: parse_job_stage(row.stage)?,
            execution_resource_key: row.execution_resource_key,
        };
        validate_execution_resource_key_if_present(request.execution_resource_key.as_deref())?;

        let enqueue = request.as_job_enqueue();
        let current_enqueue_request = canonical_job_enqueue_request_v1(
            &enqueue,
            request.stage.as_db_value(),
            request.execution_resource_key.as_deref(),
        )?;

        Ok(Self {
            id: row.id,
            request,
            current_enqueue_request,
        })
    }
}

enum IntentPromotionCandidate {
    Ready(PreparedIntentPromotion),
    Invalid { id: Uuid, error: Error },
}

impl IntentPromotionCandidate {
    fn from_row(row: SupportedJobEnqueueIntentPromotionRow) -> Self {
        let id = row.id;
        match PreparedIntentPromotion::try_from_row(row) {
            Ok(prepared) => Self::Ready(prepared),
            Err(error) => Self::Invalid { id, error },
        }
    }

    fn id(&self) -> Uuid {
        match self {
            Self::Ready(prepared) => prepared.id,
            Self::Invalid { id, .. } => *id,
        }
    }
}

struct JobEnqueueIntentMetricsRow {
    job_type: String,
    pending_count: i64,
    retrying_count: i64,
    max_promotion_attempts: i32,
    conflicted_24h: i64,
    promoted_24h: i64,
    oldest_pending_at: Option<DateTime<Utc>>,
}

enum IntentPromotionDisposition {
    Inserted,
    Existing,
    Conflicted,
    DefinitionBecameUnavailable,
    RetryDeferred,
}

impl JobEnqueueIntentPromotionReport {
    fn record(&mut self, disposition: IntentPromotionDisposition) {
        match disposition {
            IntentPromotionDisposition::Inserted => {
                self.inserted_jobs += 1;
                self.total_promoted += 1;
            }
            IntentPromotionDisposition::Existing => {
                self.existing_jobs += 1;
                self.total_promoted += 1;
            }
            IntentPromotionDisposition::Conflicted => self.conflicted += 1,
            IntentPromotionDisposition::DefinitionBecameUnavailable => {
                self.definition_became_unavailable += 1;
            }
            IntentPromotionDisposition::RetryDeferred => self.retry_deferred += 1,
        }
    }
}

/// Records a durable enqueue intent in a caller-owned transaction.
///
/// This operation never reads or locks `job_definitions`. It requires
/// PostgreSQL `READ COMMITTED` isolation because an idempotency conflict is
/// resolved by reading the row that won the unique-key race. Concurrent calls
/// for the same `(job_type, organization_id, idempotency_key)` may wait for the
/// transaction that first claimed that unique key to commit or roll back. The
/// caller must include that wait in its transaction lock ordering and timeout
/// budget.
///
/// The returned lifecycle status is a point-in-time observation. A shared key
/// lock protects an existing intent from deletion for the rest of the caller
/// transaction without blocking promotion's non-key status update, so the
/// status may change before the transaction ends. Use intent reads and metrics
/// to observe subsequent promotion or conflict.
pub async fn record_job_enqueue_intent_tx(
    tx: &mut DbTx<'_>,
    intent: &JobEnqueueIntent<'_>,
) -> Result<JobEnqueueIntentOutcome> {
    let prepared = prepare_intent(intent)?;
    let mut tx = ensure_read_committed_tx(
        tx,
        RECORD_OPERATION,
        "job.intent_idempotency_unsupported_isolation",
        "Job enqueue intent recording requires READ COMMITTED transaction isolation.",
    )
    .await?;
    record_job_enqueue_intent_read_committed_tx(&mut tx, &prepared).await
}

/// Records a durable enqueue intent in an owned transaction.
///
/// The returned lifecycle status is a point-in-time observation; promotion can
/// change it after this call returns.
pub async fn record_job_enqueue_intent(
    pool: &DbPool,
    intent: &JobEnqueueIntent<'_>,
) -> Result<JobEnqueueIntentOutcome> {
    let prepared = prepare_intent(intent)?;
    let mut tx = begin_owned_read_committed_tx(pool, RECORD_OPERATION).await?;
    let operation_result = {
        let mut read_committed_tx = tx.as_read_committed_tx();
        record_job_enqueue_intent_read_committed_tx(&mut read_committed_tx, &prepared).await
    };
    finish_owned_transaction(tx, RECORD_OPERATION, operation_result).await
}

async fn record_job_enqueue_intent_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    prepared: &PreparedIntent<'_>,
) -> Result<JobEnqueueIntentOutcome> {
    let enqueue = &prepared.enqueue;
    let idempotency_key = enqueue
        .idempotency_key
        .ok_or_else(intent_idempotency_key_error)?;
    for resolution_attempt in 0..2 {
        let row = if let Some(organization_id) = enqueue.organization_id {
            sqlx::query_as!(
                JobEnqueueIntentOutcomeRow,
                "INSERT INTO job_enqueue_intents (
                job_type,
                organization_id,
                payload,
                priority,
                max_attempts,
                timeout_seconds,
                next_run_at,
                idempotency_key,
                stage,
                enqueue_request_version,
                enqueue_request,
                execution_resource_key
             )
             VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
             ON CONFLICT (job_type, organization_id, idempotency_key)
             WHERE organization_id IS NOT NULL
             DO NOTHING
             RETURNING
                id,
                status,
                promoted_job_id,
                TRUE AS \"enqueue_request_matches!\"",
                enqueue.job_type as _,
                organization_id,
                enqueue.payload,
                enqueue.priority,
                enqueue.max_attempts,
                enqueue.timeout_seconds,
                enqueue.next_run_at,
                idempotency_key,
                prepared.stage,
                JOB_ENQUEUE_REQUEST_VERSION,
                &prepared.enqueue_request,
                prepared.execution_resource_key,
            )
            .fetch_optional(&mut **tx.as_tx())
            .await
        } else {
            sqlx::query_as!(
                JobEnqueueIntentOutcomeRow,
                "INSERT INTO job_enqueue_intents (
                job_type,
                organization_id,
                payload,
                priority,
                max_attempts,
                timeout_seconds,
                next_run_at,
                idempotency_key,
                stage,
                enqueue_request_version,
                enqueue_request,
                execution_resource_key
             )
             VALUES ($1, NULL, $2::jsonb, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
             ON CONFLICT (job_type, idempotency_key)
             WHERE organization_id IS NULL
             DO NOTHING
             RETURNING
                id,
                status,
                promoted_job_id,
                TRUE AS \"enqueue_request_matches!\"",
                enqueue.job_type as _,
                enqueue.payload,
                enqueue.priority,
                enqueue.max_attempts,
                enqueue.timeout_seconds,
                enqueue.next_run_at,
                idempotency_key,
                prepared.stage,
                JOB_ENQUEUE_REQUEST_VERSION,
                &prepared.enqueue_request,
                prepared.execution_resource_key,
            )
            .fetch_optional(&mut **tx.as_tx())
            .await
        }
        .map_err(|error| Error::from_query_sqlx_with_context(RECORD_OPERATION, error))?;

        if let Some(row) = row {
            return intent_outcome(&row, JobEnqueueIntentDisposition::Inserted);
        }

        let existing = load_existing_intent_with_key_share(tx, prepared).await?;
        let Some(existing) = existing else {
            if resolution_attempt == 0 {
                continue;
            }
            return Err(intent_conflict_missing_existing_error(
                enqueue.job_type.as_str(),
            ));
        };

        if !existing.enqueue_request_matches {
            return Err(intent_idempotency_conflict_error(enqueue.job_type.as_str()));
        }

        return intent_outcome(&existing, JobEnqueueIntentDisposition::Existing);
    }

    // The two bounded attempts above either return an outcome or a classified
    // error. Keep this defensive fallback in case that control flow changes.
    Err(intent_conflict_missing_existing_error(
        enqueue.job_type.as_str(),
    ))
}

async fn load_existing_intent_with_key_share(
    tx: &mut ReadCommittedTx<'_, '_>,
    prepared: &PreparedIntent<'_>,
) -> Result<Option<JobEnqueueIntentOutcomeRow>> {
    let enqueue = &prepared.enqueue;
    let Some(idempotency_key) = enqueue.idempotency_key else {
        return Err(intent_idempotency_key_error());
    };

    let result = if let Some(organization_id) = enqueue.organization_id {
        sqlx::query_as!(
            JobEnqueueIntentOutcomeRow,
            "SELECT
                id,
                status,
                promoted_job_id,
                enqueue_request = $4::jsonb AS \"enqueue_request_matches!\"
             FROM job_enqueue_intents
             WHERE job_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR KEY SHARE",
            enqueue.job_type as _,
            organization_id,
            idempotency_key,
            &prepared.enqueue_request,
        )
        .fetch_optional(&mut **tx.as_tx())
        .await
    } else {
        sqlx::query_as!(
            JobEnqueueIntentOutcomeRow,
            "SELECT
                id,
                status,
                promoted_job_id,
                enqueue_request = $3::jsonb AS \"enqueue_request_matches!\"
             FROM job_enqueue_intents
             WHERE job_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR KEY SHARE",
            enqueue.job_type as _,
            idempotency_key,
            &prepared.enqueue_request,
        )
        .fetch_optional(&mut **tx.as_tx())
        .await
    };

    result.map_err(|error| {
        Error::from_query_sqlx_with_context("load existing job enqueue intent", error)
    })
}

/// Loads one durable enqueue intent by ID.
///
/// Passing an organization ID filters to that tenant. Passing `None` performs
/// an administrator-wide lookup; authentication remains the caller's concern.
pub async fn get_job_enqueue_intent_by_id(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    intent_id: Uuid,
) -> Result<Option<JobEnqueueIntentRecord>> {
    let row = sqlx::query_as!(
        JobEnqueueIntentRecordRow,
        "SELECT
            id,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            idempotency_key,
            stage,
            enqueue_request_version,
            execution_resource_key,
            promotion_attempts,
            next_promotion_at,
            last_attempted_at,
            status,
            promoted_job_id,
            promoted_at,
            conflicted_at,
            last_error_code,
            last_error_message,
            created_at,
            updated_at
         FROM job_enqueue_intents
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
         LIMIT 1",
        intent_id,
        organization_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job enqueue intent by id", error))?;

    row.map(JobEnqueueIntentRecordRow::into_record).transpose()
}

/// Lists durable enqueue intents with bounded pagination.
pub async fn list_job_enqueue_intents(
    pool: &DbPool,
    filter: &JobEnqueueIntentListFilter<'_>,
) -> Result<Vec<JobEnqueueIntentRecord>> {
    validate_pagination(filter.limit, filter.offset)?;
    let status = filter.status.map(JobEnqueueIntentStatus::as_db_value);

    let rows = sqlx::query_as!(
        JobEnqueueIntentRecordRow,
        "SELECT
            id,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            idempotency_key,
            stage,
            enqueue_request_version,
            execution_resource_key,
            promotion_attempts,
            next_promotion_at,
            last_attempted_at,
            status,
            promoted_job_id,
            promoted_at,
            conflicted_at,
            last_error_code,
            last_error_message,
            created_at,
            updated_at
         FROM job_enqueue_intents
         WHERE ($1::uuid IS NULL OR organization_id = $1)
           AND ($2::text IS NULL OR status = $2)
           AND ($3::text IS NULL OR job_type ILIKE '%' || $3 || '%')
         ORDER BY created_at DESC, id DESC
         LIMIT $4
         OFFSET $5",
        filter.organization_id,
        status,
        filter.job_type_query,
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job enqueue intents", error))?;

    rows.into_iter()
        .map(JobEnqueueIntentRecordRow::into_record)
        .collect()
}

/// Returns durable intent backlog and promotion signals grouped by job type.
///
/// Each lifecycle population is aggregated behind its own selective predicate.
/// `pending_count`, `retrying_count`, `max_promotion_attempts`, and
/// `oldest_pending_at` describe only intents that are currently pending;
/// attempts made by terminal promoted or conflicted intents are not backlog.
/// `conflicted_24h` and `promoted_24h` are rolling operational signals; full
/// terminal evidence remains available through the intent read/list APIs. Job
/// types represented only by terminal intents older than 24 hours are omitted
/// because every returned signal for them would be zero. Results use stable
/// job-type ordering and the requested page must be in the shared `1..=1000`
/// limit range with a non-negative offset.
pub async fn get_job_enqueue_intent_metrics(
    pool: &DbPool,
    filter: &JobEnqueueIntentMetricsFilter<'_>,
) -> Result<Vec<JobEnqueueIntentMetricsRecord>> {
    validate_pagination(filter.limit, filter.offset)?;
    let job_type = filter.job_type.map(|job_type| job_type.as_str());
    let rows = sqlx::query_as!(
        JobEnqueueIntentMetricsRow,
        "WITH status_metrics AS (
            SELECT
                job_type,
                COUNT(*)::bigint AS pending_count,
                COUNT(*) FILTER (WHERE promotion_attempts > 0)::bigint AS retrying_count,
                MAX(promotion_attempts)::integer AS max_promotion_attempts,
                0::bigint AS conflicted_24h,
                0::bigint AS promoted_24h,
                MIN(created_at) AS oldest_pending_at
            FROM job_enqueue_intents
            WHERE status = 'PENDING'
              AND ($1::uuid IS NULL OR organization_id = $1)
              AND ($2::text IS NULL OR job_type = $2)
            GROUP BY job_type

            UNION ALL

            SELECT
                job_type,
                0::bigint AS pending_count,
                0::bigint AS retrying_count,
                0::integer AS max_promotion_attempts,
                COUNT(*)::bigint AS conflicted_24h,
                0::bigint AS promoted_24h,
                NULL::timestamptz AS oldest_pending_at
            FROM job_enqueue_intents
            WHERE status = 'CONFLICTED'
              AND conflicted_at >= now() - interval '24 hours'
              AND ($1::uuid IS NULL OR organization_id = $1)
              AND ($2::text IS NULL OR job_type = $2)
            GROUP BY job_type

            UNION ALL

            SELECT
                job_type,
                0::bigint AS pending_count,
                0::bigint AS retrying_count,
                0::integer AS max_promotion_attempts,
                0::bigint AS conflicted_24h,
                COUNT(*)::bigint AS promoted_24h,
                NULL::timestamptz AS oldest_pending_at
            FROM job_enqueue_intents
            WHERE status = 'PROMOTED'
              AND promoted_at >= now() - interval '24 hours'
              AND ($1::uuid IS NULL OR organization_id = $1)
              AND ($2::text IS NULL OR job_type = $2)
            GROUP BY job_type
         )
         SELECT
            job_type AS \"job_type!\",
            MAX(pending_count)::bigint AS \"pending_count!\",
            MAX(retrying_count)::bigint AS \"retrying_count!\",
            MAX(max_promotion_attempts)::integer AS \"max_promotion_attempts!\",
            MAX(conflicted_24h)::bigint AS \"conflicted_24h!\",
            MAX(promoted_24h)::bigint AS \"promoted_24h!\",
            MIN(oldest_pending_at) AS oldest_pending_at
         FROM status_metrics
         GROUP BY job_type
         ORDER BY job_type
         LIMIT $3
         OFFSET $4",
        filter.organization_id,
        job_type,
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job enqueue intent metrics", error)
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(JobEnqueueIntentMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                pending_count: row.pending_count,
                retrying_count: row.retrying_count,
                max_promotion_attempts: row.max_promotion_attempts,
                conflicted_24h: row.conflicted_24h,
                promoted_24h: row.promoted_24h,
                oldest_pending_at: row.oldest_pending_at,
            })
        })
        .collect()
}

/// Promotes a bounded batch of pending intents for enabled, allowed job types.
///
/// The requested limit must be in `1..=1000` and is then capped at 24 so one
/// transaction cannot create enough savepoint subtransactions to push
/// PostgreSQL into subtransaction overflow. Promotion waits at most five
/// seconds for every lock wait and at most twenty-five seconds for the complete
/// owned transaction while preserving any stricter session settings. The total
/// cap prevents repeated lock waits or non-locking statements from holding the
/// shared retention fence indefinitely. PostgreSQL terminates the promotion
/// session if that total cap expires; SQLx discards the disconnected pooled
/// session and the error is returned for retry. A fence-acquisition timeout
/// likewise aborts the owned transaction; row-level promotion failures are
/// deferred through the normal retry path. An idle pass performs one
/// non-locking eligibility query and returns without opening a transaction or
/// acquiring the retention fence. Work committed after that query is picked up
/// on the next configured pass.
pub async fn promote_job_enqueue_intents_for_types(
    pool: &DbPool,
    allowed_job_types: &[JobType<'_>],
    limit: i64,
) -> Result<JobEnqueueIntentPromotionReport> {
    validate_page_limit(limit)?;
    if allowed_job_types.is_empty() {
        return Ok(JobEnqueueIntentPromotionReport::default());
    }
    let limit = limit.min(PROMOTION_BATCH_LIMIT_MAX);

    let allowed_job_types = allowed_job_types
        .iter()
        .map(|job_type| job_type.as_str().to_owned())
        .collect::<Vec<_>>();
    if !has_eligible_job_enqueue_intents(pool, &allowed_job_types).await? {
        return Ok(JobEnqueueIntentPromotionReport::default());
    }

    let mut tx = begin_owned_read_committed_tx(pool, PROMOTE_OPERATION).await?;
    let operation_result = {
        let mut read_committed_tx = tx.as_read_committed_tx();
        promote_job_enqueue_intents_read_committed_tx(
            &mut read_committed_tx,
            &allowed_job_types,
            limit,
        )
        .await
    };
    finish_owned_transaction(tx, PROMOTE_OPERATION, operation_result).await
}

async fn has_eligible_job_enqueue_intents(
    pool: &DbPool,
    allowed_job_types: &[String],
) -> Result<bool> {
    // Keep this non-locking eligibility predicate aligned with the claiming
    // query below. A false negative delays work until the next pass; a false
    // positive only takes the transactional path unnecessarily.
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM job_enqueue_intents intent
            INNER JOIN job_definitions definition
               ON definition.job_type = intent.job_type
              AND definition.is_enabled = true
            WHERE intent.status = 'PENDING'
              AND intent.enqueue_request_version = $2
              AND intent.next_promotion_at <= now()
              AND intent.job_type = ANY($1::text[])
         )",
    )
    .bind(allowed_job_types)
    .bind(JOB_ENQUEUE_REQUEST_VERSION)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("check eligible job enqueue intents", error)
    })
}

async fn promote_job_enqueue_intents_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    allowed_job_types: &[String],
    limit: i64,
) -> Result<JobEnqueueIntentPromotionReport> {
    prepare_job_enqueue_intent_promotion_critical_section_tx(tx).await?;

    let rows = sqlx::query_as!(
        SupportedJobEnqueueIntentPromotionRow,
        "SELECT
            intent.id,
            intent.job_type,
            intent.organization_id,
            intent.payload,
            intent.priority,
            intent.max_attempts,
            intent.timeout_seconds,
            intent.next_run_at,
            intent.idempotency_key,
            intent.stage,
            intent.execution_resource_key
         FROM job_enqueue_intents intent
         INNER JOIN job_definitions definition
            ON definition.job_type = intent.job_type
           AND definition.is_enabled = true
         WHERE intent.status = 'PENDING'
           AND intent.enqueue_request_version = $3
           AND intent.next_promotion_at <= now()
           AND intent.job_type = ANY($1::text[])
         ORDER BY intent.next_promotion_at, intent.created_at, intent.id
         LIMIT $2
         FOR NO KEY UPDATE OF intent SKIP LOCKED",
        &allowed_job_types,
        limit,
        JOB_ENQUEUE_REQUEST_VERSION,
    )
    .fetch_all(&mut **tx.as_tx())
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("claim job enqueue intents", error))?;

    let candidates = rows
        .into_iter()
        .map(IntentPromotionCandidate::from_row)
        .collect::<Vec<_>>();

    let mut report = JobEnqueueIntentPromotionReport::default();
    report.mark_batch_size(candidates.len(), limit);
    for candidate in candidates {
        let intent_id = candidate.id();
        // Keep these control statements dynamic: compiling three fixed
        // SAVEPOINT statements would create SQLx cache entries without adding
        // result-shape safety.
        sqlx::query("SAVEPOINT promote_intent_row")
            .execute(&mut **tx.as_tx())
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("create intent promotion savepoint", error)
            })?;

        let promotion_result = promote_intent_candidate_tx(tx, candidate).await;

        let disposition = match promotion_result {
            Ok(disposition) => disposition,
            Err(error) => {
                rollback_intent_promotion_savepoint(tx).await?;
                if let Some((code, client_message)) = terminal_intent_failure(&error) {
                    mark_intent_conflicted_tx(tx, intent_id, code, client_message).await?;
                    log_query_intent_promotion_failure(intent_id, &error, "conflicted");
                    IntentPromotionDisposition::Conflicted
                } else if let Some((code, client_message)) = deferred_intent_failure(&error) {
                    mark_intent_retry_deferred_tx(tx, intent_id, code, client_message).await?;
                    log_query_intent_promotion_failure(intent_id, &error, "retry_deferred");
                    IntentPromotionDisposition::RetryDeferred
                } else {
                    return Err(error);
                }
            }
        };

        sqlx::query("RELEASE SAVEPOINT promote_intent_row")
            .execute(&mut **tx.as_tx())
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("release intent promotion savepoint", error)
            })?;
        report.record(disposition);
    }

    if report.total_promoted > 0
        || report.conflicted > 0
        || report.definition_became_unavailable > 0
        || report.retry_deferred > 0
    {
        tracing::info!(
            inserted_jobs = report.inserted_jobs,
            existing_jobs = report.existing_jobs,
            conflicted = report.conflicted,
            definition_became_unavailable = report.definition_became_unavailable,
            retry_deferred = report.retry_deferred,
            "processed durable job enqueue intents"
        );
    }

    Ok(report)
}

async fn promote_intent_candidate_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    candidate: IntentPromotionCandidate,
) -> Result<IntentPromotionDisposition> {
    match candidate {
        IntentPromotionCandidate::Ready(prepared) => {
            promote_prepared_intent_tx(tx, &prepared).await
        }
        IntentPromotionCandidate::Invalid { error, .. } => Err(error),
    }
}

async fn ensure_intent_snapshot_matches_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    prepared: &PreparedIntentPromotion,
) -> Result<()> {
    // Compare against the already-locked source rows so the persisted snapshots
    // never leave PostgreSQL. Rust `Value` equality does not model PostgreSQL
    // numeric normalization.
    let matches = sqlx::query_scalar::<_, bool>(
        "SELECT enqueue_request = $2::jsonb
         FROM job_enqueue_intents
         WHERE id = $1",
    )
    .bind(prepared.id)
    .bind(&prepared.current_enqueue_request)
    .fetch_one(&mut **tx.as_tx())
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("compare job enqueue intent snapshot", error)
    })?;

    if matches {
        Ok(())
    } else {
        Err(intent_snapshot_mismatch_error(prepared.id))
    }
}

async fn promote_prepared_intent_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    prepared: &PreparedIntentPromotion,
) -> Result<IntentPromotionDisposition> {
    ensure_intent_snapshot_matches_tx(tx, prepared).await?;
    let enqueue = prepared.request.as_job_enqueue();

    match enqueue_job_from_intent_tx(
        tx,
        &enqueue,
        prepared.request.execution_resource_key.as_deref(),
    )
    .await?
    {
        IntentEnqueueResolution::Enqueued(outcome) => {
            mark_intent_promoted_tx(tx, prepared.id, outcome.job_id).await?;
            Ok(match outcome.disposition {
                JobEnqueueDisposition::Inserted => IntentPromotionDisposition::Inserted,
                JobEnqueueDisposition::Existing => IntentPromotionDisposition::Existing,
            })
        }
        IntentEnqueueResolution::DefinitionUnavailable { code } => {
            let diagnostics = SanitizedQueryErrorDiagnostics::from_code(code);
            log_intent_promotion_failure(prepared.id, diagnostics, "definition_became_unavailable");
            Ok(IntentPromotionDisposition::DefinitionBecameUnavailable)
        }
        IntentEnqueueResolution::Conflict {
            code,
            client_message,
        } => {
            mark_intent_conflicted_tx(tx, prepared.id, code, client_message).await?;
            let diagnostics = SanitizedQueryErrorDiagnostics::from_code(code);
            log_intent_promotion_failure(prepared.id, diagnostics, "conflicted");
            Ok(IntentPromotionDisposition::Conflicted)
        }
    }
}

fn log_query_intent_promotion_failure(
    intent_id: Uuid,
    error: &Error,
    promotion_outcome: &'static str,
) {
    let Error::QueryError(error) = error else {
        return;
    };
    log_intent_promotion_failure(intent_id, error.sanitized_diagnostics(), promotion_outcome);
}

fn log_intent_promotion_failure(
    intent_id: Uuid,
    diagnostics: SanitizedQueryErrorDiagnostics<'_>,
    promotion_outcome: &'static str,
) {
    tracing::warn!(
        intent_id = %intent_id,
        error_code = diagnostics.code(),
        error_sqlstate = diagnostics.sqlstate().unwrap_or("none"),
        error_constraint = diagnostics.constraint().unwrap_or("none"),
        promotion_outcome,
        "durable job enqueue intent promotion did not complete"
    );
}

async fn rollback_intent_promotion_savepoint(tx: &mut ReadCommittedTx<'_, '_>) -> Result<()> {
    sqlx::query("ROLLBACK TO SAVEPOINT promote_intent_row")
        .execute(&mut **tx.as_tx())
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("rollback intent promotion savepoint", error)
        })
        .map(|_| ())
}

fn terminal_intent_failure(error: &Error) -> Option<(&'static str, &'static str)> {
    let Error::QueryError(error) = error else {
        return None;
    };
    // Maintenance invariant: only intrinsically invalid persisted requests are
    // terminal. Drift between redundant columns and the canonical snapshot is
    // repairable, so it follows the deferred path below. Unknown query failures
    // also remain retryable so an outage or repairable application policy
    // cannot discard work.
    matches!(
        error.code(),
        "job.intent_invalid_persisted_row"
            | "job.invalid_job_type"
            | "job.invalid_execution_resource_key"
            | "job.invalid_stage"
    )
    .then(|| (error.code(), error.client_message()))
}

fn deferred_intent_failure(error: &Error) -> Option<(&'static str, &'static str)> {
    let Error::QueryError(error) = error else {
        return None;
    };
    // The caller has already rolled back to the row savepoint. A successful
    // rollback proves the transaction can record backoff metadata; failures
    // that invalidate the connection or transaction return before this point.
    Some((error.code(), error.client_message()))
}

async fn mark_intent_promoted_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    intent_id: Uuid,
    job_id: Uuid,
) -> Result<()> {
    let result = sqlx::query!(
        "UPDATE job_enqueue_intents
         SET status = 'PROMOTED',
             promotion_attempts = promotion_attempts + 1,
             last_attempted_at = now(),
             promoted_job_id = $2,
             promoted_at = now(),
             conflicted_at = NULL,
             last_error_code = NULL,
             last_error_message = NULL
         WHERE id = $1
           AND status = 'PENDING'",
        intent_id,
        job_id,
    )
    .execute(&mut **tx.as_tx())
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark job enqueue intent promoted", error)
    })?;
    ensure_one_intent_updated(result.rows_affected(), intent_id, "promote")
}

async fn mark_intent_conflicted_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    intent_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> Result<()> {
    let result = sqlx::query!(
        "UPDATE job_enqueue_intents
         SET status = 'CONFLICTED',
             promotion_attempts = promotion_attempts + 1,
             last_attempted_at = now(),
             promoted_job_id = NULL,
             promoted_at = NULL,
             conflicted_at = now(),
             last_error_code = $2,
             last_error_message = $3
         WHERE id = $1
           AND status = 'PENDING'",
        intent_id,
        error_code,
        error_message,
    )
    .execute(&mut **tx.as_tx())
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark job enqueue intent conflicted", error)
    })?;
    ensure_one_intent_updated(result.rows_affected(), intent_id, "conflict")
}

async fn mark_intent_retry_deferred_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    intent_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> Result<()> {
    let result = sqlx::query!(
        "UPDATE job_enqueue_intents
         SET promotion_attempts = promotion_attempts + 1,
             last_attempted_at = now(),
             next_promotion_at = now()
                 + LEAST(
                     interval '4 minutes',
                     interval '1 second'
                         * power(2.0, LEAST(promotion_attempts, 9)::double precision)
                 )
                 + random() * LEAST(
                     interval '1 minute',
                     interval '0.25 seconds'
                         * power(2.0, LEAST(promotion_attempts, 9)::double precision)
                 ),
             last_error_code = $2,
             last_error_message = $3
         WHERE id = $1
           AND status = 'PENDING'",
        intent_id,
        error_code,
        error_message,
    )
    .execute(&mut **tx.as_tx())
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("defer failed job enqueue intent promotion", error)
    })?;
    ensure_one_intent_updated(result.rows_affected(), intent_id, "defer")
}

/// Deletes a bounded batch of old promoted intents.
///
/// Pending and conflicted rows are never selected. Runledger does not schedule
/// this cleanup automatically; the embedding application owns its retention
/// window. This cutoff cleanup removes the retention fence from every linked
/// job whose intent it deletes, but it is not coordinated with job retention.
/// Queue retention that needs atomic fence removal must instead call
/// [`delete_promoted_job_enqueue_intents_for_jobs_tx`] for its exact selected job
/// IDs before deleting those jobs in the same transaction.
pub async fn delete_promoted_job_enqueue_intents_before(
    pool: &DbPool,
    cutoff: DateTime<Utc>,
    limit: i64,
) -> Result<u64> {
    validate_page_limit(limit)?;
    let result = sqlx::query!(
        "WITH selected AS (
            SELECT id
            FROM job_enqueue_intents
            WHERE status = 'PROMOTED'
              AND promoted_at < $1
            ORDER BY promoted_at, id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
         )
         DELETE FROM job_enqueue_intents intent
         USING selected
         WHERE intent.id = selected.id",
        cutoff,
        limit,
    )
    .execute(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("delete promoted job enqueue intents", error)
    })?;
    Ok(result.rows_affected())
}

/// Deletes promoted intents linked to an exact set of jobs in a caller-owned
/// retention transaction.
///
/// After selecting candidate IDs without row locks, call this as the retention
/// transaction's first lock-taking operation, before locking or deleting the
/// same `job_queue` rows. The helper takes the exclusive side of the
/// enqueue-intent promotion fence, deletes existing promoted intent links, then
/// locks existing queue rows in UUID order. Concurrent promotions finish before
/// those intent or job row locks are taken, and new promotions wait until the
/// retention transaction ends. Taking intent locks before job locks matches the
/// duplicate-recorder order and prevents a recorder/retention lock-order cycle.
/// The fence prevents a pending intent from becoming linked between the intent
/// cleanup and the caller's queue delete.
/// Exact IDs are required because an intent may have converged on an older
/// existing job, so matching retention cutoffs alone cannot reliably order the
/// two deletes. Pending and conflicted intents are never deleted. At most 1,000
/// job IDs may be supplied per call; an empty slice is a no-op. Keep the
/// transaction short and commit promptly to release the fence. Because exact
/// cleanup cannot skip requested IDs, the helper may wait for a concurrent
/// promotion, a matching job-row lock, or an intent-row lock held by a duplicate
/// recorder. Runledger keeps its timeout caps active from fence acquisition
/// through intent deletion. The exclusive fence may wait up to thirty seconds
/// for a promotion transaction, whose total lifetime is capped at twenty-five
/// seconds. After acquiring the fence, each job- or intent-row lock wait is
/// capped at five seconds. Each statement is capped at thirty-five seconds.
/// Stricter caller settings are preserved. A timeout or deadlock aborts the
/// transaction; callers must roll it back and may retry the complete retention
/// transaction.
pub async fn delete_promoted_job_enqueue_intents_for_jobs_tx(
    tx: &mut DbTx<'_>,
    job_ids: &[Uuid],
) -> Result<u64> {
    if job_ids.is_empty() {
        return Ok(0);
    }
    validate_job_enqueue_intent_retention_batch_size(job_ids.len())?;
    delete_promoted_job_enqueue_intents_in_retention_critical_section_tx(tx, job_ids).await
}

async fn delete_promoted_job_enqueue_intents_in_retention_critical_section_tx(
    tx: &mut DbTx<'_>,
    job_ids: &[Uuid],
) -> Result<u64> {
    let previous_statement_timeout = cap_local_statement_timeout_tx(
        tx,
        JOB_ENQUEUE_INTENT_RETENTION_STATEMENT_TIMEOUT,
        JOB_ENQUEUE_INTENT_RETENTION_STATEMENT_TIMEOUT_MS,
        "cap statement timeout for job enqueue intent retention",
    )
    .await?;
    let previous_lock_timeout = cap_job_enqueue_intent_retention_fence_lock_timeout_tx(tx).await?;

    lock_job_enqueue_intent_retention_exclusive_tx(tx).await?;
    // Once the exclusive fence is held, no promotion can extend this section.
    // Restore the shorter per-lock budget before touching intent or job rows.
    cap_job_enqueue_intent_retention_lock_timeout_tx(tx).await?;

    let result = sqlx::query!(
        "DELETE FROM job_enqueue_intents
         WHERE status = 'PROMOTED'
           AND promoted_job_id = ANY($1::uuid[])",
        job_ids,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "delete promoted job enqueue intents for retained jobs",
            error,
        )
    })?;

    // Intent rows precede job rows in every critical section that can hold both.
    // Keeping that order here lets a duplicate recorder lock its promoted job
    // without cycling against retention.
    lock_retained_jobs_tx(tx, job_ids).await?;

    restore_job_enqueue_intent_lock_timeout_tx(tx, &previous_lock_timeout).await?;
    set_local_statement_timeout_tx(
        tx,
        &previous_statement_timeout,
        "restore statement timeout after job enqueue intent retention",
    )
    .await?;

    Ok(result.rows_affected())
}

fn validate_job_enqueue_intent_retention_batch_size(batch_size: usize) -> Result<()> {
    if batch_size <= JOB_ENQUEUE_INTENT_RETENTION_BATCH_LIMIT_MAX {
        return Ok(());
    }

    Err(Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.intent_retention_batch_too_large",
        "Job enqueue intent retention batch must contain at most 1,000 job IDs.",
        format!(
            "job enqueue intent retention batch must contain at most \
             {JOB_ENQUEUE_INTENT_RETENTION_BATCH_LIMIT_MAX} job IDs, got {batch_size}"
        ),
    )))
}

async fn lock_retained_jobs_tx(tx: &mut DbTx<'_>, job_ids: &[Uuid]) -> Result<()> {
    sqlx::query(
        "SELECT id
         FROM job_queue
         WHERE id = ANY($1::uuid[])
         ORDER BY id
         FOR UPDATE",
    )
    .bind(job_ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "lock retained jobs before promoted intent cleanup",
            error,
        )
    })?;
    Ok(())
}

async fn cap_job_enqueue_intent_promotion_lock_timeout_tx(tx: &mut DbTx<'_>) -> Result<String> {
    cap_local_lock_timeout_tx(
        tx,
        JOB_ENQUEUE_INTENT_PROMOTION_LOCK_TIMEOUT,
        JOB_ENQUEUE_INTENT_PROMOTION_LOCK_TIMEOUT_MS,
        "cap lock timeout for job enqueue intent promotion",
    )
    .await
}

async fn cap_job_enqueue_intent_retention_fence_lock_timeout_tx(
    tx: &mut DbTx<'_>,
) -> Result<String> {
    cap_local_lock_timeout_tx(
        tx,
        JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT,
        JOB_ENQUEUE_INTENT_RETENTION_FENCE_LOCK_TIMEOUT_MS,
        "cap lock timeout for job enqueue intent retention fence",
    )
    .await
}

async fn cap_job_enqueue_intent_retention_lock_timeout_tx(tx: &mut DbTx<'_>) -> Result<String> {
    cap_local_lock_timeout_tx(
        tx,
        JOB_ENQUEUE_INTENT_RETENTION_LOCK_TIMEOUT,
        JOB_ENQUEUE_INTENT_RETENTION_LOCK_TIMEOUT_MS,
        "cap lock timeout for job enqueue intent retention critical section",
    )
    .await
}

async fn restore_job_enqueue_intent_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    previous_lock_timeout: &str,
) -> Result<()> {
    set_local_lock_timeout_tx(
        tx,
        previous_lock_timeout,
        "restore lock timeout after job enqueue intent retention critical section",
    )
    .await
}

async fn prepare_job_enqueue_intent_promotion_critical_section_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
) -> Result<()> {
    // Promotion owns this transaction, so leave both caps active until commit
    // or rollback. The per-lock cap bounds one acquisition; the transaction cap
    // bounds all work performed while the shared retention fence is held.
    cap_local_transaction_timeout_tx(
        tx.as_tx(),
        JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT,
        JOB_ENQUEUE_INTENT_PROMOTION_TRANSACTION_TIMEOUT_MS,
        "cap transaction timeout for job enqueue intent promotion",
    )
    .await?;
    cap_job_enqueue_intent_promotion_lock_timeout_tx(tx.as_tx()).await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared($1, $2)
         /* runledger:lock_job_enqueue_intent_promotion */",
    )
    .bind(RUNLEDGER_ADVISORY_LOCK_NAMESPACE)
    .bind(JOB_ENQUEUE_INTENT_RETENTION_LOCK)
    .execute(&mut **tx.as_tx())
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock job enqueue intent promotion", error)
    })?;
    Ok(())
}

async fn lock_job_enqueue_intent_retention_exclusive_tx(tx: &mut DbTx<'_>) -> Result<()> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock($1, $2)
         /* runledger:lock_job_enqueue_intent_retention */",
    )
    .bind(RUNLEDGER_ADVISORY_LOCK_NAMESPACE)
    .bind(JOB_ENQUEUE_INTENT_RETENTION_LOCK)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock job enqueue intent retention", error)
    })?;
    Ok(())
}

fn prepare_intent<'a>(intent: &JobEnqueueIntent<'a>) -> Result<PreparedIntent<'a>> {
    let enqueue = intent.as_job_enqueue();
    JobType::try_new(enqueue.job_type.as_str()).map_err(|_| invalid_intent_job_type_error())?;
    let Some(idempotency_key) = enqueue.idempotency_key else {
        return Err(intent_idempotency_key_error());
    };
    if idempotency_key.trim().is_empty() {
        return Err(intent_idempotency_key_error());
    }
    if enqueue.max_attempts.is_some_and(|value| value <= 0) {
        return Err(invalid_intent_max_attempts_error());
    }
    if enqueue.timeout_seconds.is_some_and(|value| value <= 0) {
        return Err(invalid_intent_timeout_error());
    }
    if let Some(execution_resource_key) = intent.execution_resource_key() {
        validate_execution_resource_key(execution_resource_key)?;
    }

    let stage = enqueue.stage.unwrap_or(JobStage::Queued).as_db_value();
    let enqueue_request =
        canonical_job_enqueue_request_v1(&enqueue, stage, intent.execution_resource_key())?;
    Ok(PreparedIntent {
        enqueue,
        execution_resource_key: intent.execution_resource_key(),
        stage,
        enqueue_request,
    })
}

fn invalid_intent_job_type_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_job_type",
        "Job type must not be blank.",
        "job enqueue intent job_type was blank",
    ))
}

fn validate_execution_resource_key_if_present(execution_resource_key: Option<&str>) -> Result<()> {
    if let Some(execution_resource_key) = execution_resource_key {
        validate_execution_resource_key(execution_resource_key)?;
    }
    Ok(())
}

fn intent_outcome(
    row: &JobEnqueueIntentOutcomeRow,
    disposition: JobEnqueueIntentDisposition,
) -> Result<JobEnqueueIntentOutcome> {
    Ok(JobEnqueueIntentOutcome {
        intent_id: row.id,
        status: parse_intent_status(&row.status)?,
        promoted_job_id: row.promoted_job_id,
        disposition,
    })
}

fn parse_intent_status(status: &str) -> Result<JobEnqueueIntentStatus> {
    status.parse().map_err(|()| invalid_intent_row_error())
}

fn ensure_one_intent_updated(rows_affected: u64, intent_id: Uuid, operation: &str) -> Result<()> {
    if rows_affected == 1 {
        return Ok(());
    }
    Err(Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.intent_transition_failed",
        "Job enqueue intent could not be updated.",
        format!(
            "job enqueue intent {operation} transition for {intent_id} affected {rows_affected} rows"
        ),
    )))
}

fn intent_idempotency_key_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.intent_invalid_idempotency_key",
        "Job enqueue intent idempotency key must not be blank.",
        "job enqueue intent idempotency_key was missing or blank",
    ))
}

fn invalid_intent_max_attempts_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.intent_invalid_max_attempts",
        "Job enqueue intent max attempts must be positive.",
        "job enqueue intent max_attempts was not positive",
    ))
}

fn invalid_intent_timeout_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.intent_invalid_timeout",
        "Job enqueue intent timeout must be positive.",
        "job enqueue intent timeout_seconds was not positive",
    ))
}

fn intent_idempotency_conflict_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "job.intent_idempotency_conflict",
        "Job enqueue intent retry conflicts with the existing idempotency key.",
        format!("job enqueue intent request differs for job_type={job_type}"),
    ))
}

fn intent_conflict_missing_existing_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.intent_idempotency_conflict_missing_existing",
        "Job enqueue intent retry could not be resolved.",
        format!(
            "job enqueue intent insert for job_type={job_type} conflicted but matching row was not found"
        ),
    ))
}

fn invalid_intent_row_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.intent_invalid_persisted_row",
        "Job enqueue intent contains invalid persisted state.",
        "job enqueue intent persisted row could not be decoded",
    ))
}

fn intent_snapshot_mismatch_error(intent_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.intent_snapshot_mismatch",
        "Job enqueue intent request snapshot is inconsistent.",
        format!("job enqueue intent {intent_id} does not match its canonical request snapshot"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_failure_diagnostics_expose_only_sanitized_fields() {
        let error = QueryError::from_sqlx(
            sqlx::Error::Protocol("test promotion protocol failure".to_owned()),
            Some("promote test intent"),
        );

        let diagnostics = error.sanitized_diagnostics();

        assert_eq!(diagnostics.code(), "db.query_failed");
        assert_eq!(diagnostics.sqlstate(), None);
        assert_eq!(diagnostics.constraint(), None);
        let debug = format!("{diagnostics:?}");
        assert!(!debug.contains("promote test intent"));
        assert!(!debug.contains("test promotion protocol failure"));
    }

    #[test]
    fn disappearing_queue_idempotency_winner_is_deferred_not_terminal() {
        let error = Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            "job.idempotency_conflict_missing_existing",
            "Job enqueue retry could not be resolved.",
            "test disappearing idempotency winner",
        ));

        assert!(deferred_intent_failure(&error).is_some());
        assert_eq!(terminal_intent_failure(&error), None);
    }

    #[test]
    fn unclassified_query_error_is_deferred_after_savepoint_recovery() {
        let error = Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            "job.future_row_error",
            "Job enqueue intent could not be promoted.",
            "test future row-level error",
        ));

        assert_eq!(terminal_intent_failure(&error), None);
        assert_eq!(
            deferred_intent_failure(&error),
            Some((
                "job.future_row_error",
                "Job enqueue intent could not be promoted."
            ))
        );
    }

    #[test]
    fn repairable_snapshot_mismatch_is_deferred_not_terminal() {
        let error = intent_snapshot_mismatch_error(Uuid::now_v7());

        assert_eq!(terminal_intent_failure(&error), None);
        assert_eq!(
            deferred_intent_failure(&error),
            Some((
                "job.intent_snapshot_mismatch",
                "Job enqueue intent request snapshot is inconsistent."
            ))
        );
    }

    #[test]
    fn invalid_persisted_job_type_is_terminal() {
        let error = Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            "job.invalid_job_type",
            "Job type in persisted row is invalid.",
            "test invalid persisted job type",
        ));

        assert_eq!(
            terminal_intent_failure(&error),
            Some((
                "job.invalid_job_type",
                "Job type in persisted row is invalid."
            ))
        );
    }

    #[test]
    fn non_query_errors_remain_batch_fatal() {
        let error = Error::ConnectionError("test connection failure".to_owned());

        assert_eq!(terminal_intent_failure(&error), None);
        assert_eq!(deferred_intent_failure(&error), None);
    }
}
