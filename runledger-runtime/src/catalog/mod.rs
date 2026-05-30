//! Catalog-backed startup API for keeping handlers, job definitions, schedules,
//! and workflow steps aligned.
//!
//! [`JobCatalog`] is the primary facade. Applications register handlers once,
//! sync the catalog's definition defaults to PostgreSQL, and use the catalog
//! helpers to build enqueue, schedule, and workflow inputs with job-type
//! validation at the call site.
//!
//! Catalog defaults apply to every registered entry unless a job is registered
//! with job-specific definition overrides.

mod error;
mod inputs;
mod registration;
mod sync;
mod types;
mod workflow;

pub use error::CatalogError;
pub use inputs::{CatalogJobEnqueueInput, CatalogJobScheduleInput};
pub use types::{
    JobCatalog, JobCatalogDefaults, JobCatalogDefinitionOverrides, JobCatalogExactSyncReport,
    JobCatalogSyncReport, JobCatalogSyncScope,
};
pub use workflow::CatalogWorkflowDagBuilder;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use runledger_core::jobs::{JobContext, JobFailure, JobHandler, JobType, WorkflowBuildError};
    use serde_json::Value;
    use serde_json::json;

    struct StaticHandler(&'static str);

    #[async_trait]
    impl JobHandler for StaticHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new(self.0)
        }

        async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
            Ok(())
        }
    }

    struct BlankHandler;

    #[async_trait]
    impl JobHandler for BlankHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("   ")
        }

        async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
            Ok(())
        }
    }

    struct MismatchHandler;

    #[async_trait]
    impl JobHandler for MismatchHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.other")
        }

        async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
            Ok(())
        }
    }

    #[test]
    fn rejects_blank_declared_job_type() {
        let error = JobCatalog::new()
            .try_job("   ", StaticHandler("jobs.test"))
            .expect_err("blank declared job type");
        assert!(matches!(
            error,
            CatalogError::InvalidJobType {
                job_type,
                source: runledger_core::jobs::IdentifierValidationError::BlankJobType,
            } if job_type == "   "
        ));
    }

    #[test]
    fn rejects_blank_handler_job_type() {
        let error = JobCatalog::new()
            .try_job("jobs.test", BlankHandler)
            .expect_err("blank handler job type");
        assert!(matches!(
            error,
            CatalogError::InvalidHandlerJobType {
                handler_job_type,
                source: runledger_core::jobs::IdentifierValidationError::BlankJobType,
            } if handler_job_type == "   "
        ));
    }

    #[test]
    fn rejects_handler_job_type_mismatch() {
        let error = JobCatalog::new()
            .try_job("jobs.catalog.expected", MismatchHandler)
            .expect_err("handler mismatch");
        assert!(matches!(error, CatalogError::HandlerJobTypeMismatch { .. }));
    }

    #[test]
    fn rejects_duplicate_job_type() {
        let error = JobCatalog::new()
            .try_job("jobs.dup", StaticHandler("jobs.dup"))
            .expect("first registration")
            .try_job("jobs.dup", StaticHandler("jobs.dup"))
            .expect_err("duplicate job type");
        assert!(matches!(error, CatalogError::DuplicateJobType { .. }));
    }

    #[test]
    fn to_registry_preserves_handlers_and_retry_overrides() {
        let catalog = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .try_retry_delay_override("jobs.test", "job.test.wait", 42)
            .expect("retry override");
        let registry = catalog.to_registry();
        assert!(registry.get(JobType::new("jobs.test")).is_some());
        assert_eq!(
            registry.retry_delay_override(JobType::new("jobs.test"), "job.test.wait"),
            Some(42)
        );
    }

    #[test]
    fn retry_override_rejects_unknown_job_type() {
        let error = JobCatalog::new()
            .try_retry_delay_override("jobs.missing", "job.test.wait", 42)
            .expect_err("unknown job type");
        assert!(matches!(error, CatalogError::UnknownJobType { .. }));
    }

    #[test]
    fn retry_override_validates_job_type_before_override_values() {
        let error = JobCatalog::new()
            .try_retry_delay_override("jobs.missing", "   ", 0)
            .expect_err("unknown job type");
        assert!(matches!(error, CatalogError::UnknownJobType { .. }));
    }

    #[test]
    fn retry_override_rejects_blank_failure_code() {
        let error = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .try_retry_delay_override("jobs.test", "   ", 42)
            .expect_err("blank failure code");
        assert!(matches!(error, CatalogError::InvalidFailureCode));
    }

    #[test]
    fn retry_override_rejects_non_positive_delay() {
        let error = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .try_retry_delay_override("jobs.test", "job.test.wait", 0)
            .expect_err("invalid retry delay");
        assert!(matches!(error, CatalogError::InvalidRetryDelay));
    }

    #[test]
    fn exact_sync_scope_rejects_blank_job_type() {
        let error = JobCatalogSyncScope::job_type("   ").expect_err("blank exact sync job type");
        assert!(matches!(
            error,
            CatalogError::InvalidExactSyncScopeJobType {
                job_type,
                source: runledger_core::jobs::IdentifierValidationError::BlankJobType,
            } if job_type == "   "
        ));
    }

    #[test]
    fn exact_sync_scope_rejects_empty_job_type_list() {
        let error = JobCatalogSyncScope::job_types(Vec::<String>::new())
            .expect_err("empty exact sync scope");
        assert!(matches!(error, CatalogError::InvalidExactSyncScope));
    }

    #[test]
    fn job_enqueue_rejects_unknown_job_type() {
        let catalog = JobCatalog::new();
        let error = catalog
            .job_enqueue(&CatalogJobEnqueueInput {
                job_type: "jobs.missing",
                organization_id: None,
                payload: &json!({}),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            })
            .expect_err("unknown job type");
        assert!(matches!(error, CatalogError::UnknownJobType { .. }));
    }

    #[test]
    fn job_enqueue_rejects_disabled_catalog_defaults() {
        let catalog = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .defaults(JobCatalogDefaults::new().enabled(false));
        let error = catalog
            .job_enqueue(&CatalogJobEnqueueInput {
                job_type: "jobs.test",
                organization_id: None,
                payload: &json!({}),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            })
            .expect_err("disabled job type");
        assert!(matches!(error, CatalogError::DisabledJobType { .. }));
    }

    #[test]
    fn job_enqueue_rejects_disabled_job_definition_overrides() {
        let catalog = JobCatalog::new().job_with_definition_overrides(
            "jobs.test",
            StaticHandler("jobs.test"),
            JobCatalogDefinitionOverrides::new().enabled(false),
        );
        let error = catalog
            .job_enqueue(&CatalogJobEnqueueInput {
                job_type: "jobs.test",
                organization_id: None,
                payload: &json!({}),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            })
            .expect_err("disabled job type");
        assert!(matches!(error, CatalogError::DisabledJobType { .. }));
    }

    #[test]
    fn job_definition_overrides_take_precedence_over_disabled_catalog_defaults() {
        let catalog = JobCatalog::new()
            .job_with_definition_overrides(
                "jobs.test",
                StaticHandler("jobs.test"),
                JobCatalogDefinitionOverrides::new().enabled(true),
            )
            .defaults(JobCatalogDefaults::new().enabled(false));
        let payload = json!({});
        let enqueue = catalog
            .job_enqueue(&CatalogJobEnqueueInput {
                job_type: "jobs.test",
                organization_id: None,
                payload: &payload,
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            })
            .expect("job-specific enabled override should win");
        assert_eq!(enqueue.job_type.as_str(), "jobs.test");
    }

    #[test]
    fn job_schedule_accepts_enabled_job_definition_override() {
        let catalog = JobCatalog::new()
            .job_with_definition_overrides(
                "jobs.test",
                StaticHandler("jobs.test"),
                JobCatalogDefinitionOverrides::new().enabled(true),
            )
            .defaults(JobCatalogDefaults::new().enabled(false));
        let payload = json!({});
        let schedule = catalog
            .job_schedule(&CatalogJobScheduleInput {
                name: "jobs.test.schedule",
                job_type: "jobs.test",
                organization_id: None,
                payload_template: &payload,
                cron_expr: "0 * * * * *",
                is_active: true,
                next_fire_at: chrono::Utc::now(),
                max_jitter_seconds: 0,
            })
            .expect("job-specific enabled override should win");
        assert_eq!(schedule.job_type.as_str(), "jobs.test");
    }

    #[test]
    fn definition_overrides_apply_after_job_registration() {
        let catalog = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .definition_overrides(
                "jobs.test",
                JobCatalogDefinitionOverrides::new().enabled(true),
            )
            .defaults(JobCatalogDefaults::new().enabled(false));
        let payload = json!({});
        let enqueue = catalog
            .job_enqueue(&CatalogJobEnqueueInput {
                job_type: "jobs.test",
                organization_id: None,
                payload: &payload,
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            })
            .expect("post-registration override should apply");
        assert_eq!(enqueue.job_type.as_str(), "jobs.test");
    }

    #[test]
    fn definition_overrides_reject_unknown_job_type() {
        let error = JobCatalog::new()
            .try_definition_overrides(
                "jobs.missing",
                JobCatalogDefinitionOverrides::new().enabled(true),
            )
            .expect_err("unknown job type");
        assert!(matches!(error, CatalogError::UnknownJobType { .. }));
    }

    #[test]
    fn definition_overrides_reject_invalid_job_type() {
        let error = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .try_definition_overrides("   ", JobCatalogDefinitionOverrides::new().enabled(true))
            .expect_err("invalid job type");
        assert!(matches!(error, CatalogError::InvalidJobType { .. }));
    }

    #[test]
    fn try_definition_overrides_reject_invalid_values() {
        let error = JobCatalog::new()
            .job("jobs.test", StaticHandler("jobs.test"))
            .try_definition_overrides(
                "jobs.test",
                JobCatalogDefinitionOverrides::new().max_attempts(0),
            )
            .expect_err("invalid override");
        assert!(matches!(
            error,
            CatalogError::InvalidJobDefinitionValue {
                job_type,
                field: "max_attempts",
            } if job_type == "jobs.test"
        ));
    }

    #[test]
    fn workflow_dag_propagates_blank_step_key_error() {
        let catalog = JobCatalog::new().job("jobs.test", StaticHandler("jobs.test"));
        let error = catalog
            .workflow_dag("workflow.test", &json!({}))
            .job("   ", "jobs.test", &json!({}))
            .expect_err("blank step key");
        assert!(matches!(
            error,
            CatalogError::WorkflowBuild(WorkflowBuildError::BlankStepKey { .. })
        ));
    }

    #[test]
    fn workflow_dag_propagates_unknown_dependency_error() {
        let catalog = JobCatalog::new().job("jobs.test", StaticHandler("jobs.test"));
        let error = catalog
            .workflow_dag("workflow.test", &json!({}))
            .job("first", "jobs.test", &json!({}))
            .expect("first step")
            .after_success("missing", ["first"])
            .expect_err("unknown dependency target");
        assert!(matches!(
            error,
            CatalogError::WorkflowBuild(WorkflowBuildError::UnknownStepKey { .. })
        ));
    }

    #[test]
    fn workflow_dag_propagates_empty_workflow_error() {
        let catalog = JobCatalog::new();
        let error = catalog
            .workflow_dag("workflow.test", &json!({}))
            .try_build()
            .expect_err("empty workflow");
        assert!(matches!(
            error,
            CatalogError::WorkflowBuild(WorkflowBuildError::EmptySteps)
        ));
    }
}
