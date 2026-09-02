use runledger_core::jobs::{JobStage, JobTypeName, WorkflowStepExecutionKind};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_step_execution_kind,
};
use super::errors::{workflow_internal_state_error, workflow_release_conflict_error};
use super::locking::try_lock_workflow_run_release_shared_tx;

#[derive(Clone, Debug)]
pub(in crate::jobs::workflows) struct StepReleaseCandidate {
    id: Uuid,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    payload: serde_json::Value,
    stored_execution: StoredStepReleaseExecution,
}

#[derive(Clone, Debug)]
enum StoredStepReleaseExecution {
    Job {
        job_type: Option<JobTypeName>,
        priority: Option<i32>,
        max_attempts: Option<i32>,
        timeout_seconds: Option<i32>,
        stage: Option<JobStage>,
        execution_resource_key: Option<String>,
    },
    External,
}

#[derive(Debug)]
enum ReleasableStepExecution<'candidate> {
    Job(JobReleaseSpec<'candidate>),
    External,
}

#[derive(Debug)]
struct JobReleaseSpec<'candidate> {
    job_type: &'candidate JobTypeName,
    priority: i32,
    max_attempts: i32,
    timeout_seconds: i32,
    stage: JobStage,
    execution_resource_key: Option<&'candidate str>,
}

impl StepReleaseCandidate {
    #[must_use]
    pub(in crate::jobs::workflows) fn from_decoded_fields(init: StepReleaseCandidateInit) -> Self {
        let StepReleaseCandidateInit {
            id,
            workflow_run_id,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            execution_resource_key,
        } = init;
        let stored_execution = match execution_kind {
            WorkflowStepExecutionKind::Job => StoredStepReleaseExecution::Job {
                job_type,
                priority,
                max_attempts,
                timeout_seconds,
                stage,
                execution_resource_key,
            },
            WorkflowStepExecutionKind::External => StoredStepReleaseExecution::External,
        };

        Self {
            id,
            workflow_run_id,
            organization_id,
            payload,
            stored_execution,
        }
    }

    #[must_use]
    pub(in crate::jobs::workflows) fn id(&self) -> Uuid {
        self.id
    }

    fn releasable_execution(&self) -> Result<ReleasableStepExecution<'_>> {
        match &self.stored_execution {
            StoredStepReleaseExecution::Job {
                job_type,
                priority,
                max_attempts,
                timeout_seconds,
                stage,
                execution_resource_key,
            } => Ok(ReleasableStepExecution::Job(
                JobReleaseSpec::from_nullable_fields(
                    job_type.as_ref(),
                    *priority,
                    *max_attempts,
                    *timeout_seconds,
                    *stage,
                    execution_resource_key.as_deref(),
                )?,
            )),
            StoredStepReleaseExecution::External => Ok(ReleasableStepExecution::External),
        }
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

impl<'candidate> JobReleaseSpec<'candidate> {
    fn from_nullable_fields(
        job_type: Option<&'candidate JobTypeName>,
        priority: Option<i32>,
        max_attempts: Option<i32>,
        timeout_seconds: Option<i32>,
        stage: Option<JobStage>,
        execution_resource_key: Option<&'candidate str>,
    ) -> Result<Self> {
        let Some(job_type) = job_type else {
            return Err(workflow_internal_state_error(
                "job workflow step release is missing job_type",
            ));
        };
        let Some(priority) = priority else {
            return Err(workflow_internal_state_error(
                "job workflow step release is missing priority",
            ));
        };
        let Some(max_attempts) = max_attempts else {
            return Err(workflow_internal_state_error(
                "job workflow step release is missing max_attempts",
            ));
        };
        let Some(timeout_seconds) = timeout_seconds else {
            return Err(workflow_internal_state_error(
                "job workflow step release is missing timeout_seconds",
            ));
        };
        let Some(stage) = stage else {
            return Err(workflow_internal_state_error(
                "job workflow step release is missing stage",
            ));
        };

        Ok(Self {
            job_type,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            execution_resource_key,
        })
    }
}

pub(crate) async fn enqueue_root_steps_tx(tx: &mut DbTx<'_>, workflow_run_id: Uuid) -> Result<()> {
    let rows = sqlx::query!(
        "SELECT
            id,
            workflow_run_id,
            execution_kind::text AS \"execution_kind!\",
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            execution_resource_key
         FROM workflow_steps
         WHERE workflow_run_id = $1
           AND status = 'BLOCKED'
           AND dependency_count_pending = 0
         ORDER BY created_at ASC
         FOR UPDATE",
        workflow_run_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup root workflow steps for enqueue", error)
    })?;

    for row in rows {
        let candidate = StepReleaseCandidate::from_decoded_fields(StepReleaseCandidateInit {
            id: row.id,
            workflow_run_id: row.workflow_run_id,
            execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
            job_type: row.job_type.map(parse_job_type_name).transpose()?,
            organization_id: row.organization_id,
            payload: row.payload,
            priority: row.priority,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            stage: row.stage.map(parse_job_stage).transpose()?,
            execution_resource_key: row.execution_resource_key,
        });
        release_candidate_step_tx(tx, &candidate).await?;
    }

    Ok(())
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

    // Preserve the release protocol's error precedence: malformed persisted
    // job shapes are reported only after the advisory-lock and run-state gates.
    match candidate.releasable_execution()? {
        ReleasableStepExecution::Job(job) => release_job_step_tx(tx, candidate, &job).await,
        ReleasableStepExecution::External => release_external_step_tx(tx, candidate).await,
    }
}

async fn release_job_step_tx(
    tx: &mut DbTx<'_>,
    candidate: &StepReleaseCandidate,
    job: &JobReleaseSpec<'_>,
) -> Result<()> {
    let row = sqlx::query!(
        "INSERT INTO job_queue (
                    job_type,
                    organization_id,
                    payload,
                    priority,
                    max_attempts,
                    timeout_seconds,
                    next_run_at,
                    stage,
                    execution_resource_key
                 )
                 VALUES ($1, $2, $3::jsonb, $4, $5, $6, now(), $7, $8)
                 RETURNING id, run_number",
        job.job_type.as_str(),
        candidate.organization_id,
        &candidate.payload,
        job.priority,
        job.max_attempts,
        job.timeout_seconds,
        job.stage.as_db_value(),
        job.execution_resource_key,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("enqueue released workflow step job", error)
    })?;

    let job_id: Uuid = row.id;
    let run_number: i32 = row.run_number;

    mark_released_job_step_enqueued(tx, candidate.id, job_id).await?;
    insert_released_job_enqueue_event(tx, job, job_id, run_number).await
}

async fn mark_released_job_step_enqueued(
    tx: &mut DbTx<'_>,
    step_id: Uuid,
    job_id: Uuid,
) -> Result<()> {
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
        step_id,
        job_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark released workflow step as enqueued", error)
    })?
    .rows_affected();
    if updated != 1 {
        return Err(workflow_internal_state_error(
            "workflow step release preconditions were not met",
        ));
    }
    Ok(())
}

async fn insert_released_job_enqueue_event(
    tx: &mut DbTx<'_>,
    job: &JobReleaseSpec<'_>,
    job_id: Uuid,
    run_number: i32,
) -> Result<()> {
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
        job.stage.as_db_value(),
        job.job_type.as_str(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert released workflow step enqueue event", error)
    })?;
    Ok(())
}

async fn release_external_step_tx(
    tx: &mut DbTx<'_>,
    candidate: &StepReleaseCandidate,
) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{JobStage, JobTypeName, WorkflowStepExecutionKind};
    use serde_json::json;
    use sqlx::types::Uuid;

    use crate::{Error, QueryErrorCategory};

    use super::{
        JobReleaseSpec, ReleasableStepExecution, StepReleaseCandidate, StepReleaseCandidateInit,
    };

    fn valid_job_candidate_init() -> StepReleaseCandidateInit {
        StepReleaseCandidateInit {
            id: Uuid::nil(),
            workflow_run_id: Uuid::now_v7(),
            execution_kind: WorkflowStepExecutionKind::Job,
            job_type: Some(JobTypeName::new("jobs.test.release").expect("valid job type")),
            organization_id: Some(Uuid::now_v7()),
            payload: json!({"release": "candidate"}),
            priority: Some(42),
            max_attempts: Some(3),
            timeout_seconds: Some(60),
            stage: Some(JobStage::Queued),
            execution_resource_key: Some("provider-account:release".to_owned()),
        }
    }

    fn assert_job_release_shape_error(init: StepReleaseCandidateInit, expected_message: &str) {
        let candidate = StepReleaseCandidate::from_decoded_fields(init);
        let result = candidate.releasable_execution();
        let Err(Error::QueryError(error)) = result else {
            panic!("expected malformed job release shape to return an internal state error");
        };
        assert_eq!(error.category(), QueryErrorCategory::Internal);
        assert_eq!(error.code(), "workflow.internal_state");
        assert_eq!(error.internal_message(), expected_message);
    }

    #[test]
    fn release_candidate_converts_complete_job_shape_into_release_spec() {
        let init = valid_job_candidate_init();
        let workflow_run_id = init.workflow_run_id;
        let candidate = StepReleaseCandidate::from_decoded_fields(init);
        let execution = candidate
            .releasable_execution()
            .expect("complete job shape should decode");

        assert_eq!(candidate.id(), Uuid::nil());
        assert_eq!(candidate.workflow_run_id, workflow_run_id);
        match execution {
            ReleasableStepExecution::Job(JobReleaseSpec {
                job_type,
                priority,
                max_attempts,
                timeout_seconds,
                stage,
                execution_resource_key,
            }) => {
                assert_eq!(job_type.as_str(), "jobs.test.release");
                assert_eq!(priority, 42);
                assert_eq!(max_attempts, 3);
                assert_eq!(timeout_seconds, 60);
                assert_eq!(stage, JobStage::Queued);
                assert_eq!(execution_resource_key, Some("provider-account:release"));
            }
            ReleasableStepExecution::External => {
                panic!("complete job shape must retain job release settings")
            }
        }
    }

    #[test]
    fn release_candidate_converts_external_shape_without_job_release_settings() {
        let mut init = valid_job_candidate_init();
        init.execution_kind = WorkflowStepExecutionKind::External;
        init.job_type = None;
        init.priority = None;
        init.max_attempts = None;
        init.timeout_seconds = None;
        init.stage = None;
        init.execution_resource_key = None;

        let candidate = StepReleaseCandidate::from_decoded_fields(init);
        assert!(matches!(
            candidate
                .releasable_execution()
                .expect("external shape should decode"),
            ReleasableStepExecution::External
        ));
    }

    #[test]
    fn release_candidate_preserves_job_shape_error_order() {
        let mut missing_job_type = valid_job_candidate_init();
        missing_job_type.job_type = None;
        assert_job_release_shape_error(
            missing_job_type,
            "job workflow step release is missing job_type",
        );

        let mut missing_priority = valid_job_candidate_init();
        missing_priority.priority = None;
        assert_job_release_shape_error(
            missing_priority,
            "job workflow step release is missing priority",
        );

        let mut missing_max_attempts = valid_job_candidate_init();
        missing_max_attempts.max_attempts = None;
        assert_job_release_shape_error(
            missing_max_attempts,
            "job workflow step release is missing max_attempts",
        );

        let mut missing_timeout_seconds = valid_job_candidate_init();
        missing_timeout_seconds.timeout_seconds = None;
        assert_job_release_shape_error(
            missing_timeout_seconds,
            "job workflow step release is missing timeout_seconds",
        );

        let mut missing_stage = valid_job_candidate_init();
        missing_stage.stage = None;
        assert_job_release_shape_error(missing_stage, "job workflow step release is missing stage");
    }
}
