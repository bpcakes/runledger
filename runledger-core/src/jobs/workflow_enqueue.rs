//! Workflow enqueue builders and DAG validation.
//!
//! Use this module when work is more than one independent retried job. A
//! workflow run gives dependent steps one durable identity, validates the graph
//! up front, and lets the PostgreSQL workflow runtime release downstream steps
//! as prerequisites finish.
//!
//! For ordinary dependent work, prefer [`WorkflowDagBuilder`] for simple DAGs, or
//! [`WorkflowRunEnqueueBuilder`] and [`WorkflowStepEnqueueBuilder`] for advanced
//! per-step settings. Direct jobs are best for one independent unit of work.

mod build_validation;
mod dag_builder;
mod dag_validation;
mod errors;
mod run_builder;
mod step_builder;
mod step_validation;
mod types;

pub use dag_builder::WorkflowDagBuilder;
pub use dag_validation::{
    WorkflowDagDependencyValidationInput, WorkflowDagStepValidationInput,
    WorkflowDagValidationError, validate_workflow_dag, validate_workflow_run_enqueue,
    validate_workflow_step_append,
};
pub use errors::WorkflowBuildError;
pub use run_builder::WorkflowRunEnqueueBuilder;
pub use step_builder::WorkflowStepEnqueueBuilder;
pub use types::{
    WorkflowDependencyReleaseMode, WorkflowJobStepExecution, WorkflowRunEnqueue,
    WorkflowStepDependencySpec, WorkflowStepEnqueue, WorkflowStepExecution,
    WorkflowStepExecutionKind,
};

#[cfg(test)]
mod tests;
