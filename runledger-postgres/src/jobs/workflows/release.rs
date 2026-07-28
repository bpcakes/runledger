use runledger_core::jobs::{JobStage, JobTypeName, WorkflowStepExecutionKind};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::errors::{workflow_internal_state_error, workflow_release_conflict_error};
use super::locking::try_lock_workflow_run_release_shared_tx;

#[derive(Clone, Debug)]
pub(in crate::jobs::workflows) struct StepReleaseCandidate {
    id: Uuid,
    workflow_run_id: Uuid,
    execution_kind: WorkflowStepExecutionKind,
    job_type: Option<JobTypeName>,
    organization_id: Option<Uuid>,
    payload: serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<JobStage>,
    execution_resource_key: Option<String>,
}

impl StepReleaseCandidate {
    #[must_use]
    pub(in crate::jobs::workflows) fn from_init(init: StepReleaseCandidateInit) -> Self {
        Self {
            id: init.id,
            workflow_run_id: init.workflow_run_id,
            execution_kind: init.execution_kind,
            job_type: init.job_type,
            organization_id: init.organization_id,
            payload: init.payload,
            priority: init.priority,
            max_attempts: init.max_attempts,
            timeout_seconds: init.timeout_seconds,
            stage: init.stage,
            execution_resource_key: init.execution_resource_key,
        }
    }

    #[must_use]
    pub(in crate::jobs::workflows) fn id(&self) -> Uuid {
        self.id
    }

    #[must_use]
    pub(in crate::jobs::workflows) fn workflow_run_id(&self) -> Uuid {
        self.workflow_run_id
    }
}

pub(in crate::jobs::workflows) struct StepReleaseCandidateInit {
    pub(in crate::jobs::workflows) id: Uuid,
    pub(in crate::jobs::workflows) workflow_run_id: Uuid,
    pub(in crate::jobs::workflows) execution_kind: WorkflowStepExecutionKind,
    pub(in crate::jobs::workflows) job_type: Option<JobTypeName>,
    pub(in crate::jobs::workflows) organization_id: Option<Uuid>,
    pub(in crate::jobs::workflows) payload: serde_json::Value,
    pub(in crate::jobs::workflows) priority: Option<i32>,
    pub(in crate::jobs::workflows) max_attempts: Option<i32>,
    pub(in crate::jobs::workflows) timeout_seconds: Option<i32>,
    pub(in crate::jobs::workflows) stage: Option<JobStage>,
    pub(in crate::jobs::workflows) execution_resource_key: Option<String>,
}

fn job_release_fields(
    candidate: &StepReleaseCandidate,
) -> Result<(&JobTypeName, i32, i32, i32, JobStage)> {
    let Some(job_type) = candidate.job_type.as_ref() else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing job_type",
        ));
    };
    let Some(priority) = candidate.priority else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing priority",
        ));
    };
    let Some(max_attempts) = candidate.max_attempts else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing max_attempts",
        ));
    };
    let Some(timeout_seconds) = candidate.timeout_seconds else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing timeout_seconds",
        ));
    };
    let Some(stage) = candidate.stage else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing stage",
        ));
    };

    Ok((job_type, priority, max_attempts, timeout_seconds, stage))
}

pub(in crate::jobs::workflows) async fn release_candidate_step_tx(
    tx: &mut DbTx<'_>,
    candidate: &StepReleaseCandidate,
) -> Result<()> {
    // Callers reach step release with the candidate workflow-step rows locked
    // FOR UPDATE. append_workflow_steps_tx also holds the workflow-run row lock
    // before inserting and releasing appended steps. If cancel already owns the
    // exclusive advisory lock, append/external callers must roll back rather
    // than committing consumed dependency counters without a matching
    // release/cancel sweep. Job terminal completion waits on the blocking
    // shared lock before it gets here, so this try-lock succeeds reentrantly on
    // that connection. Root enqueue also reaches this path, but a just-inserted
    // workflow run is not externally visible before commit, so the lock is
    // expected to be uncontended there.
    if !try_lock_workflow_run_release_shared_tx(tx, candidate.workflow_run_id).await? {
        return Err(workflow_release_conflict_error(candidate.workflow_run_id));
    }

    if !workflow_run_allows_step_release_tx(tx, candidate.workflow_run_id).await? {
        // This also covers reentrant calls from cancellation: PostgreSQL
        // advisory locks are reentrant for a backend, then the canceled run
        // status rejects release here.
        return Ok(());
    }

    match candidate.execution_kind {
        WorkflowStepExecutionKind::Job => {
            let (job_type, priority, max_attempts, timeout_seconds, stage) =
                job_release_fields(candidate)?;
            let row = sqlx::query!(
                "INSERT INTO job_queue (
                    job_type,
                    organization_id,
                    payload,
                    priority,
                    max_attempts,
                    timeout_seconds,
                    next_run_at,
                    workflow_step_id,
                    stage,
                    execution_resource_key
                 )
                 VALUES ($1, $2, $3::jsonb, $4, $5, $6, now(), $7, $8, $9)
                 RETURNING id, run_number",
                job_type.as_str(),
                candidate.organization_id,
                &candidate.payload,
                priority,
                max_attempts,
                timeout_seconds,
                candidate.id,
                stage.as_db_value(),
                candidate.execution_resource_key,
            )
            .fetch_one(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("enqueue released workflow step job", error)
            })?;

            let job_id: Uuid = row.id;
            let run_number: i32 = row.run_number;

            let updated = sqlx::query!(
                "UPDATE workflow_steps
                 SET status = 'ENQUEUED',
                     job_id = $2,
                     released_at = COALESCE(released_at, now()),
                     status_reason = NULL,
                     last_error_code = NULL,
                     last_error_message = NULL,
                     updated_at = now()
                 WHERE id = $1
                   AND status = 'BLOCKED'
                   AND job_id IS NULL
                   AND dependency_count_pending = 0
                   AND dependency_count_unsatisfied = 0",
                candidate.id,
                job_id,
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "mark released workflow step as enqueued",
                    error,
                )
            })?
            .rows_affected();
            if updated != 1 {
                return Err(workflow_internal_state_error(
                    "workflow step release preconditions were not met",
                ));
            }

            sqlx::query!(
                "INSERT INTO job_events (
                    job_id,
                    run_number,
                    event_type,
                    stage,
                    payload
                 )
                 VALUES ($1, $2, 'ENQUEUED', $3, jsonb_build_object('job_type', $4::text))",
                job_id,
                run_number,
                stage.as_db_value(),
                job_type.as_str(),
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "insert released workflow step enqueue event",
                    error,
                )
            })?;

            Ok(())
        }
        WorkflowStepExecutionKind::External => {
            let updated = sqlx::query!(
                "UPDATE workflow_steps
                 SET status = 'WAITING_FOR_EXTERNAL',
                     job_id = NULL,
                     released_at = COALESCE(released_at, now()),
                     started_at = NULL,
                     finished_at = NULL,
                     status_reason = NULL,
                     last_error_code = NULL,
                     last_error_message = NULL,
                     updated_at = now()
                 WHERE id = $1
                   AND status = 'BLOCKED'
                   AND job_id IS NULL
                   AND dependency_count_pending = 0
                   AND dependency_count_unsatisfied = 0",
                candidate.id,
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "mark released workflow step as waiting for external completion",
                    error,
                )
            })?
            .rows_affected();
            if updated != 1 {
                return Err(workflow_internal_state_error(
                    "workflow step release preconditions were not met",
                ));
            }

            Ok(())
        }
    }
}

async fn workflow_run_allows_step_release_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<bool> {
    // The shared row lock makes the releasable-status check stable until this
    // transaction either releases the step or exits. Release callers already
    // hold workflow-step row locks before taking this workflow-run row lock;
    // cancel takes the release advisory lock before it takes workflow-step row
    // locks, and release never blocks on that advisory lock.
    sqlx::query_scalar!(
        "SELECT status IN (
            'RUNNING'::workflow_run_status,
            'WAITING_FOR_EXTERNAL'::workflow_run_status
         ) AS \"allows_release!\"
         FROM workflow_runs
         WHERE id = $1
         FOR SHARE",
        workflow_run_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("check workflow run allows step release", error)
    })
}
