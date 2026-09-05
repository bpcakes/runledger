//! Storage-agnostic job execution and workflow-enqueue contracts.
//!
//! Handler results use [`JobCompletion`] for terminal success or bounded
//! continuation and [`JobFailure`] for failures, including optional retry
//! not-before lower bounds. Workflow builders carry result-step declarations,
//! reusable active keys, execution-resource keys, and the explicit
//! handler-continuation opt-in consumed by `runledger-postgres`.

mod execution;
mod handler;
mod identifier_macros;
mod identifiers;
mod runtime_types;
mod spec;
mod status;
mod submission;
mod typed_handler;
mod workflow_enqueue;

pub use execution::{JobExecution, JobExecutionError, JobExecutionServices, JobExecutionUpdate};
pub use handler::{ExecutionHandlerAdapter, JobExecutionHandler, JobHandler, JobHandlerRegistry};
pub use identifiers::{
    IdentifierValidationError, JobType, JobTypeName, StepKey, StepKeyName, WorkflowType,
    WorkflowTypeName,
};
pub use runtime_types::{
    JobCompletion, JobCompletionDisposition, JobContext, JobDeadLetterInfo, JobDeadLetterReason,
    JobFailure, JobProgressValidationError, JobRetryTiming, validate_job_progress,
};
pub use status::{
    JobEventType, JobFailureKind, JobStage, JobStatus, WorkflowRunStatus, WorkflowStepStatus,
};
pub use workflow_enqueue::{
    WorkflowBuildError, WorkflowDagBuilder, WorkflowDagDependencyValidationInput,
    WorkflowDagStepValidationInput, WorkflowDagValidationError, WorkflowDependencyReleaseMode,
    WorkflowJobStepExecution, WorkflowRunEnqueue, WorkflowRunEnqueueBuilder,
    WorkflowStepDependencySpec, WorkflowStepEnqueue, WorkflowStepEnqueueBuilder,
    WorkflowStepExecution, WorkflowStepExecutionKind, validate_workflow_dag,
    validate_workflow_run_enqueue, validate_workflow_step_append,
};

pub use spec::{JobDefinitionSettings, JobSpec, JobSpecError, JobSpecs};
pub use submission::{JobContract, JobSubmission, JobSubmissionError};
pub use typed_handler::{TypedHandlerAdapter, TypedJobHandler, malformed_job_payload};
