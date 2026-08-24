use runledger_core::jobs::{JobStatus, JobType};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::transaction_settings::{cap_local_lock_timeout_tx, set_local_lock_timeout_tx};

const JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT: &str = "1s";
const JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT_MS: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "callers must inspect Updated/NotFound/Rejected"]
#[non_exhaustive]
pub enum JobPayloadUuidArrayFieldUpdate {
    Updated,
    NotFound,
    Rejected {
        reason: JobPayloadUuidArrayFieldUpdateRejection,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobPayloadUuidArrayFieldUpdateRejection {
    WorkflowManaged,
    IdempotentRequestSnapshot,
    NotPendingOrClaimed,
}

#[derive(sqlx::FromRow)]
struct JobPayloadUuidArrayFieldUpdateCandidate {
    status: String,
    worker_id: Option<String>,
    lease_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    workflow_managed: bool,
    idempotency_key: Option<String>,
    enqueue_request: Option<Value>,
}

/// Updates one UUID-array payload field on a direct, unclaimed pending job.
///
/// Returns a classified rejection when the row is already claimed or terminal,
/// belongs to a workflow step, or has an idempotency request snapshot that this
/// API cannot keep consistent.
pub async fn update_job_payload_uuid_array_field(
    pool: &DbPool,
    organization_id: Uuid,
    job_id: Uuid,
    job_type: JobType<'_>,
    payload_field: &str,
    values: &[Uuid],
) -> Result<JobPayloadUuidArrayFieldUpdate> {
    let mut tx = pool.begin().await.map_err(|error| {
        Error::from_query_sqlx_with_context(
            "begin job payload uuid array update transaction",
            error,
        )
    })?;

    let previous_lock_timeout =
        cap_job_payload_uuid_array_field_update_lock_timeout_tx(&mut tx).await?;

    let row_result = sqlx::query_as::<_, JobPayloadUuidArrayFieldUpdateCandidate>(
        "SELECT
             status::text AS status,
             worker_id,
             lease_expires_at,
             EXISTS (
                 SELECT 1
                 FROM workflow_steps ws
                 WHERE ws.job_id = job_queue.id
             ) AS workflow_managed,
             idempotency_key,
             enqueue_request
           FROM job_queue
           WHERE id = $1
             AND organization_id = $2
             AND job_type = $3
           FOR UPDATE",
    )
    .bind(job_id)
    .bind(organization_id)
    .bind(job_type)
    .fetch_optional(&mut *tx)
    .await;

    let row = match row_result {
        Ok(row) => {
            set_local_lock_timeout_tx(
                &mut tx,
                &previous_lock_timeout,
                "restore job payload uuid array update lock timeout",
            )
            .await?;
            row
        }
        Err(error) => {
            return Err(Error::from_query_sqlx_with_context(
                "classify job payload uuid array update",
                error,
            ));
        }
    };

    let Some(row) = row else {
        tx.commit().await.map_err(|error| {
            Error::from_query_sqlx_with_context(
                "commit job payload uuid array update transaction",
                error,
            )
        })?;
        return Ok(JobPayloadUuidArrayFieldUpdate::NotFound);
    };

    // Order matters: workflow-managed jobs can also carry request snapshots, so
    // return the ownership rejection before the snapshot-consistency rejection.
    let rejection = if row.workflow_managed {
        Some(JobPayloadUuidArrayFieldUpdateRejection::WorkflowManaged)
    } else if row.idempotency_key.is_some() || row.enqueue_request.is_some() {
        Some(JobPayloadUuidArrayFieldUpdateRejection::IdempotentRequestSnapshot)
    } else if row.status != JobStatus::Pending.as_db_value()
        || row.worker_id.is_some()
        || row.lease_expires_at.is_some()
    {
        Some(JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed)
    } else {
        None
    };

    if let Some(reason) = rejection {
        tx.commit().await.map_err(|error| {
            Error::from_query_sqlx_with_context(
                "commit job payload uuid array update transaction",
                error,
            )
        })?;
        return Ok(JobPayloadUuidArrayFieldUpdate::Rejected { reason });
    }

    sqlx::query!(
        "UPDATE job_queue
         SET
             payload = jsonb_set(
                 payload,
                 ARRAY[$4::text],
                 to_jsonb($5::uuid[]),
                 true
             ),
             updated_at = now()
         WHERE id = $1
           AND organization_id = $2
           AND job_type = $3",
        job_id,
        organization_id,
        job_type as _,
        payload_field,
        values,
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("update job payload uuid array field", error)
    })?;

    tx.commit().await.map_err(|error| {
        Error::from_query_sqlx_with_context(
            "commit job payload uuid array update transaction",
            error,
        )
    })?;
    Ok(JobPayloadUuidArrayFieldUpdate::Updated)
}

async fn cap_job_payload_uuid_array_field_update_lock_timeout_tx(
    tx: &mut DbTx<'_>,
) -> Result<String> {
    cap_local_lock_timeout_tx(
        tx,
        JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT,
        JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT_MS,
        "set job payload uuid array update lock timeout",
    )
    .await
}
