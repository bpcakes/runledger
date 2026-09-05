use std::{fmt, sync::Arc};

use runledger_core::jobs::JobProgressValidationError;
use sqlx::error::ErrorKind;

mod classify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorCategory {
    Conflict,
    Validation,
    Forbidden,
    Internal,
}

/// Stable semantic kinds for database errors that drive runtime policy.
///
/// Human- and machine-readable error codes remain available through
/// [`QueryError::code`]. This enum is deliberately smaller: it covers errors
/// whose handling must stay compile-checked across Runledger crates instead of
/// depending on duplicated string literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorKind {
    JobLeaseOwnerMismatch,
    JobInvalidCompletionProgress,
    JobInvalidContinuationDelay,
    JobInvalidRetryTiming,
    JobUnstartedClaimReleaseNotApplicable,
    JobWorkflowHandlerContinuationNotEnabled,
    JobWorkflowRequeueNotSupported,
    PostgresLockNotAvailable,
    WorkflowReleaseConflict,
}

impl QueryErrorKind {
    const fn spec(self) -> QueryErrorSpec {
        match self {
            Self::JobLeaseOwnerMismatch => QueryErrorSpec::forbidden(
                "job.lease_owner_mismatch",
                "Job lease is not currently held by this worker.",
            ),
            Self::JobInvalidCompletionProgress => QueryErrorSpec::validation(
                "job.invalid_completion_progress",
                "Job completion progress is invalid.",
            ),
            Self::JobInvalidContinuationDelay => QueryErrorSpec::validation(
                "job.invalid_continuation_delay",
                "Job continuation delay is too large.",
            ),
            Self::JobInvalidRetryTiming => QueryErrorSpec::validation(
                "job.invalid_retry_timing",
                "Job retry timing is invalid.",
            ),
            Self::JobUnstartedClaimReleaseNotApplicable => QueryErrorSpec::validation(
                "job.unstarted_claim_release_not_applicable",
                "Job claim cannot be released as unstarted.",
            ),
            Self::JobWorkflowHandlerContinuationNotEnabled => QueryErrorSpec::validation(
                "job.workflow_handler_continuation_not_enabled",
                "Workflow step handler continuation is not enabled.",
            ),
            Self::JobWorkflowRequeueNotSupported => QueryErrorSpec::validation(
                "job.workflow_requeue_not_supported",
                "Workflow-managed jobs cannot be requeued directly.",
            ),
            Self::PostgresLockNotAvailable => QueryErrorSpec::internal(),
            Self::WorkflowReleaseConflict => QueryErrorSpec::conflict(
                "workflow.release_conflict",
                "Workflow step release conflicted with another workflow mutation.",
            ),
        }
    }

    fn from_sqlstate(sqlstate: Option<&str>) -> Option<Self> {
        match sqlstate {
            Some("55P03") => Some(Self::PostgresLockNotAvailable),
            _ => None,
        }
    }
}

/// Query-error fields that are safe to emit at application logging boundaries.
///
/// This deliberately excludes the internal message and SQLx source because
/// either may contain payload values, idempotency keys, or database policy
/// details. Keeping the safe projection as a distinct type makes it harder for
/// callers to accidentally widen structured logs with raw diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SanitizedQueryErrorDiagnostics<'a> {
    code: &'a str,
    sqlstate: Option<&'a str>,
    constraint: Option<&'a str>,
}

impl<'a> SanitizedQueryErrorDiagnostics<'a> {
    #[must_use]
    pub(crate) const fn from_code(code: &'a str) -> Self {
        Self {
            code,
            sqlstate: None,
            constraint: None,
        }
    }

    #[must_use]
    pub(crate) const fn code(self) -> &'a str {
        self.code
    }

    #[must_use]
    pub(crate) const fn sqlstate(self) -> Option<&'a str> {
        self.sqlstate
    }

    #[must_use]
    pub(crate) const fn constraint(self) -> Option<&'a str> {
        self.constraint
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameworkConstraintSpec {
    category: QueryErrorCategory,
    code: &'static str,
    client_message: &'static str,
}

impl FrameworkConstraintSpec {
    #[must_use]
    pub const fn new(
        category: QueryErrorCategory,
        code: &'static str,
        client_message: &'static str,
    ) -> Self {
        Self {
            category,
            code,
            client_message,
        }
    }

    #[must_use]
    pub const fn category(&self) -> QueryErrorCategory {
        self.category
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn client_message(&self) -> &'static str {
        self.client_message
    }
}

#[derive(Clone)]
enum QueryErrorClassification {
    Fixed(QueryErrorKind),
    Classified(QueryErrorSpec),
    InvalidProgress(JobProgressValidationError),
}

impl QueryErrorClassification {
    const fn spec(&self) -> QueryErrorSpec {
        match self {
            Self::Fixed(kind) => kind.spec(),
            Self::Classified(spec) => *spec,
            Self::InvalidProgress(_) => {
                QueryErrorSpec::validation("job.invalid_progress", "Job progress is invalid.")
            }
        }
    }

    const fn kind(&self) -> Option<QueryErrorKind> {
        match self {
            Self::Fixed(kind) => Some(*kind),
            Self::Classified(_) | Self::InvalidProgress(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct QueryError {
    classification: QueryErrorClassification,
    sqlstate: Option<String>,
    constraint: Option<String>,
    message: String,
    source: Option<Arc<sqlx::Error>>,
}

impl QueryError {
    pub(crate) fn from_invalid_progress(error: JobProgressValidationError) -> Self {
        Self {
            classification: QueryErrorClassification::InvalidProgress(error),
            sqlstate: None,
            constraint: None,
            message: error.to_string(),
            source: None,
        }
    }

    /// Returns the typed violation of the effective durable progress values.
    /// Runtime adapters can preserve this validation failure without parsing
    /// diagnostic strings or mistaking it for a transient database failure.
    #[must_use]
    pub const fn progress_validation_error(&self) -> Option<JobProgressValidationError> {
        match self.classification {
            QueryErrorClassification::InvalidProgress(error) => Some(error),
            _ => None,
        }
    }

    #[must_use]
    pub fn from_classified(
        category: QueryErrorCategory,
        code: &'static str,
        client_message: &'static str,
        internal_message: impl Into<String>,
    ) -> Self {
        Self {
            classification: QueryErrorClassification::Classified(QueryErrorSpec {
                category,
                code,
                client_message,
            }),
            sqlstate: None,
            constraint: None,
            message: internal_message.into(),
            source: None,
        }
    }

    #[must_use]
    pub(crate) fn from_kind(kind: QueryErrorKind, internal_message: impl Into<String>) -> Self {
        Self {
            classification: QueryErrorClassification::Fixed(kind),
            sqlstate: None,
            constraint: None,
            message: internal_message.into(),
            source: None,
        }
    }

    #[must_use]
    pub(crate) fn from_sqlx_with_kind(
        kind: QueryErrorKind,
        internal_message: impl Into<String>,
        source: sqlx::Error,
    ) -> Self {
        let (sqlstate, constraint) = source
            .as_database_error()
            .map(|database_error| {
                (
                    database_error.code().map(|code| code.into_owned()),
                    database_error.constraint().map(ToOwned::to_owned),
                )
            })
            .unwrap_or((None, None));

        Self {
            classification: QueryErrorClassification::Fixed(kind),
            sqlstate,
            constraint,
            message: internal_message.into(),
            source: Some(Arc::new(source)),
        }
    }

    #[must_use]
    pub fn from_sqlx_with_constraint_classifier<F>(
        error: sqlx::Error,
        context: Option<&str>,
        classify_constraint: F,
    ) -> Self
    where
        F: Fn(&str) -> Option<FrameworkConstraintSpec>,
    {
        let (sqlstate, constraint, spec, raw_message) = if let Some(db) = error.as_database_error()
        {
            let sqlstate = db.code().map(|code| code.into_owned());
            let constraint = db.constraint().map(ToOwned::to_owned);
            let spec = classify_query_error_with_constraint_classifier(
                &db.kind(),
                sqlstate.as_deref(),
                constraint.as_deref(),
                classify_constraint,
            );
            (sqlstate, constraint, spec, db.message().to_owned())
        } else {
            (
                None,
                None,
                QueryErrorSpec::internal().into(),
                error.to_string(),
            )
        };

        let message = match context {
            Some(ctx) => format!("{ctx}: {raw_message}"),
            None => raw_message,
        };

        let classification = match QueryErrorKind::from_sqlstate(sqlstate.as_deref()) {
            Some(kind) => QueryErrorClassification::Fixed(kind),
            None => QueryErrorClassification::Classified(QueryErrorSpec {
                category: spec.category(),
                code: spec.code(),
                client_message: spec.client_message(),
            }),
        };

        Self {
            classification,
            sqlstate,
            constraint,
            message,
            source: Some(Arc::new(error)),
        }
    }

    pub(crate) fn from_sqlx(error: sqlx::Error, context: Option<&str>) -> Self {
        Self::from_sqlx_with_constraint_classifier(error, context, |_| None)
    }

    #[must_use]
    pub const fn category(&self) -> QueryErrorCategory {
        self.classification.spec().category
    }

    /// Returns a stable semantic kind when this error participates in
    /// cross-crate runtime policy.
    #[must_use]
    pub const fn kind(&self) -> Option<QueryErrorKind> {
        self.classification.kind()
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.classification.spec().code
    }

    #[must_use]
    pub const fn client_message(&self) -> &'static str {
        self.classification.spec().client_message
    }

    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }

    #[must_use]
    pub fn constraint(&self) -> Option<&str> {
        self.constraint.as_deref()
    }

    #[must_use]
    pub(crate) fn sanitized_diagnostics(&self) -> SanitizedQueryErrorDiagnostics<'_> {
        SanitizedQueryErrorDiagnostics {
            code: self.code(),
            sqlstate: self.sqlstate(),
            constraint: self.constraint(),
        }
    }

    #[must_use]
    pub fn internal_message(&self) -> &str {
        &self.message
    }

    /// Returns the underlying SQLx error for trusted diagnostics.
    ///
    /// Public [`Display`](fmt::Display) and [`Debug`](fmt::Debug) output for
    /// [`QueryError`] is sanitized, but the returned source may contain raw
    /// database details. Do not log or expose it on untrusted boundaries without
    /// redaction.
    #[must_use]
    pub fn source_arc(&self) -> Option<Arc<sqlx::Error>> {
        self.source.clone()
    }

    #[must_use]
    pub fn reclassified_with_constraint_classifier<F>(mut self, classify_constraint: F) -> Self
    where
        F: Fn(&str) -> Option<FrameworkConstraintSpec>,
    {
        let Some(spec) = self.constraint.as_deref().and_then(classify_constraint) else {
            return self;
        };

        self.classification = QueryErrorClassification::Classified(QueryErrorSpec {
            category: spec.category(),
            code: spec.code(),
            client_message: spec.client_message(),
        });
        self
    }
}

impl fmt::Debug for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryError")
            .field("category", &self.category())
            .field("kind", &self.kind())
            .field("code", &self.code())
            .field("client_message", &self.client_message())
            .field("sqlstate", &self.sqlstate)
            .field("constraint", &self.constraint)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.client_message())
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[derive(Debug, Clone, Copy)]
struct QueryErrorSpec {
    category: QueryErrorCategory,
    code: &'static str,
    client_message: &'static str,
}

impl QueryErrorSpec {
    const fn conflict(code: &'static str, client_message: &'static str) -> Self {
        Self {
            category: QueryErrorCategory::Conflict,
            code,
            client_message,
        }
    }

    const fn validation(code: &'static str, client_message: &'static str) -> Self {
        Self {
            category: QueryErrorCategory::Validation,
            code,
            client_message,
        }
    }

    const fn forbidden(code: &'static str, client_message: &'static str) -> Self {
        Self {
            category: QueryErrorCategory::Forbidden,
            code,
            client_message,
        }
    }

    const fn internal() -> Self {
        Self {
            category: QueryErrorCategory::Internal,
            code: "db.query_failed",
            client_message: "Database operation failed.",
        }
    }
}

impl From<QueryErrorSpec> for FrameworkConstraintSpec {
    fn from(spec: QueryErrorSpec) -> Self {
        Self::new(spec.category, spec.code, spec.client_message)
    }
}

#[must_use]
pub fn classify_query_error(
    kind: &ErrorKind,
    sqlstate: Option<&str>,
    constraint: Option<&str>,
) -> FrameworkConstraintSpec {
    classify_query_error_with_constraint_classifier(kind, sqlstate, constraint, |_| None)
}

#[must_use]
pub fn classify_query_error_with_constraint_classifier<F>(
    kind: &ErrorKind,
    sqlstate: Option<&str>,
    constraint: Option<&str>,
    classify_constraint: F,
) -> FrameworkConstraintSpec
where
    F: Fn(&str) -> Option<FrameworkConstraintSpec>,
{
    if let Some(spec) = constraint.and_then(classify_constraint) {
        return spec;
    }

    classify_database_error(kind, sqlstate, constraint).into()
}

fn classify_database_error(
    kind: &ErrorKind,
    sqlstate: Option<&str>,
    constraint: Option<&str>,
) -> QueryErrorSpec {
    if let Some(spec) = constraint.and_then(classify_constraint) {
        return spec;
    }

    match (kind, sqlstate) {
        (ErrorKind::UniqueViolation, _) | (_, Some("23505")) => {
            QueryErrorSpec::conflict("db.unique_violation", "Resource already exists.")
        }
        (ErrorKind::ForeignKeyViolation, _) | (_, Some("23503")) => QueryErrorSpec::validation(
            "db.related_resource_missing",
            "Related resource does not exist.",
        ),
        (_, Some("23001")) => QueryErrorSpec::validation(
            "db.related_resource_still_referenced",
            "Related resource is still referenced and cannot be deleted.",
        ),
        (ErrorKind::CheckViolation, _) | (_, Some("23514")) => QueryErrorSpec::validation(
            "db.business_rule_violation",
            "Request violates a business rule.",
        ),
        (ErrorKind::NotNullViolation, _) | (_, Some("23502")) => {
            QueryErrorSpec::validation("db.required_field_missing", "Required data is missing.")
        }
        (_, Some("42501")) => {
            QueryErrorSpec::forbidden("db.permission_denied", "Operation is not allowed.")
        }
        _ => QueryErrorSpec::internal(),
    }
}

fn classify_constraint(constraint: &str) -> Option<QueryErrorSpec> {
    classify::classify_constraint(constraint)
}

#[must_use]
pub fn classify_framework_constraint(constraint: &str) -> Option<FrameworkConstraintSpec> {
    classify_constraint(constraint).map(FrameworkConstraintSpec::from)
}

#[must_use]
pub fn has_framework_constraint_classifier(constraint: &str) -> bool {
    classify_framework_constraint(constraint).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_job_idempotency_constraint() {
        let spec = classify_database_error(
            &ErrorKind::UniqueViolation,
            Some("23505"),
            Some("uq_job_queue_type_idempotency_org"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Conflict);
        assert_eq!(spec.code, "job.already_enqueued");
    }

    #[test]
    fn classifies_global_job_idempotency_constraint() {
        let spec = classify_database_error(
            &ErrorKind::UniqueViolation,
            Some("23505"),
            Some("uq_job_queue_type_idempotency_global"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Conflict);
        assert_eq!(spec.code, "job.already_enqueued");
    }

    #[test]
    fn classifies_workflow_idempotency_constraint() {
        let spec = classify_database_error(
            &ErrorKind::UniqueViolation,
            Some("23505"),
            Some("uq_workflow_runs_type_idempotency_org"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Conflict);
        assert_eq!(spec.code, "workflow.already_enqueued");
    }

    #[test]
    fn classifies_global_workflow_idempotency_constraint() {
        let spec = classify_database_error(
            &ErrorKind::UniqueViolation,
            Some("23505"),
            Some("uq_workflow_runs_type_idempotency_global"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Conflict);
        assert_eq!(spec.code, "workflow.already_enqueued");
    }

    #[test]
    fn classifies_job_definition_fk_constraint() {
        let spec = classify_database_error(
            &ErrorKind::ForeignKeyViolation,
            Some("23503"),
            Some("fk_job_queue_job_type"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "job.definition_not_found");
    }

    #[test]
    fn classifies_job_runtime_config_definition_fk_constraint() {
        let spec = classify_database_error(
            &ErrorKind::ForeignKeyViolation,
            Some("23503"),
            Some("fk_job_runtime_configs_job_type"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "job.definition_not_found");
    }

    #[test]
    fn classifies_job_organization_fk_constraint() {
        let spec = classify_database_error(
            &ErrorKind::ForeignKeyViolation,
            Some("23503"),
            Some("fk_job_queue_organization"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "job.organization_not_found");
    }

    #[test]
    fn classifies_workflow_linkage_symmetry_constraint() {
        let spec = classify_database_error(
            &ErrorKind::CheckViolation,
            Some("23514"),
            Some("os_workflow_job_linkage_symmetry"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "workflow.linkage_symmetry_violation");
    }

    #[test]
    fn classifies_workflow_linkage_symmetry_trigger_table_constraint() {
        let spec = classify_database_error(
            &ErrorKind::CheckViolation,
            Some("23514"),
            Some("os_workflow_job_linkage_symmetry_trigger_table"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "workflow.linkage_symmetry_trigger_table_invalid");
    }

    #[test]
    fn classifies_workflow_linkage_cutover_audit_constraints() {
        for constraint in [
            "os_workflow_job_linkage_expand_audit",
            "os_workflow_job_linkage_expand_rollback_audit",
            "os_workflow_job_linkage_contract_audit",
        ] {
            let spec = classify_database_error(
                &ErrorKind::CheckViolation,
                Some("23514"),
                Some(constraint),
            );
            assert_eq!(spec.code, "workflow.linkage_cutover_audit_failed");
        }
    }

    #[test]
    fn classifies_workflow_linkage_compatibility_trigger_table_constraint() {
        let spec = classify_database_error(
            &ErrorKind::CheckViolation,
            Some("23514"),
            Some("os_workflow_job_linkage_compatibility_trigger_table"),
        );
        assert_eq!(
            spec.code,
            "workflow.linkage_compatibility_trigger_table_invalid"
        );
    }

    #[test]
    fn classifies_external_gate_downgrade_blocked_constraint() {
        let spec = classify_database_error(
            &ErrorKind::CheckViolation,
            Some("23514"),
            Some("os_workflow_external_gate_downgrade_waiting_runs_exist"),
        );
        assert_eq!(spec.category, QueryErrorCategory::Validation);
        assert_eq!(spec.code, "workflow.external_gate_downgrade_blocked");
    }

    #[test]
    fn custom_constraint_classifier_takes_precedence() {
        let spec = classify_query_error_with_constraint_classifier(
            &ErrorKind::UniqueViolation,
            Some("23505"),
            Some("os_custom_override"),
            |constraint| {
                (constraint == "os_custom_override").then_some(FrameworkConstraintSpec::new(
                    QueryErrorCategory::Forbidden,
                    "custom.override",
                    "Custom override wins.",
                ))
            },
        );
        assert_eq!(spec.category(), QueryErrorCategory::Forbidden);
        assert_eq!(spec.code(), "custom.override");
        assert_eq!(spec.client_message(), "Custom override wins.");
    }

    #[test]
    fn query_error_debug_omits_internal_message() {
        let error = QueryError::from_classified(
            QueryErrorCategory::Conflict,
            "job.idempotency_conflict",
            "Job enqueue retry conflicts with the existing idempotency key.",
            "internal context includes secret-idempotency-key",
        );

        let debug = format!("{error:?}");
        assert_eq!(
            debug,
            "QueryError { category: Conflict, kind: None, code: \"job.idempotency_conflict\", client_message: \"Job enqueue retry conflicts with the existing idempotency key.\", sqlstate: None, constraint: None, has_source: false }"
        );
        assert!(!debug.contains("secret-idempotency-key"));

        let display = error.to_string();
        assert_eq!(
            display,
            "Job enqueue retry conflicts with the existing idempotency key."
        );
        assert!(!display.contains("secret-idempotency-key"));
    }

    #[test]
    fn query_error_from_sqlx_uses_sanitized_display_and_debug() {
        let error = QueryError::from_sqlx(
            sqlx::Error::Protocol("internal secret-idempotency-key detail".into()),
            Some("sensitive context"),
        );

        let display = error.to_string();
        assert_eq!(display, "Database operation failed.");
        assert!(!display.contains("secret-idempotency-key"));

        let debug = format!("{error:?}");
        assert!(debug.contains("db.query_failed"));
        assert!(!debug.contains("secret-idempotency-key"));
        assert!(error.internal_message().contains("secret-idempotency-key"));
        assert!(std::error::Error::source(&error).is_some());
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn sanitized_diagnostics_omit_internal_message_and_source() {
        let error = QueryError::from_sqlx(
            sqlx::Error::Protocol("database detail includes secret-idempotency-key".into()),
            Some("sensitive context includes secret-idempotency-key"),
        );

        let diagnostics = error.sanitized_diagnostics();

        assert_eq!(diagnostics.code(), "db.query_failed");
        assert_eq!(diagnostics.sqlstate(), None);
        assert_eq!(diagnostics.constraint(), None);
        let debug = format!("{diagnostics:?}");
        assert!(debug.contains("db.query_failed"));
        assert!(!debug.contains("secret-idempotency-key"));
    }

    #[test]
    fn typed_query_error_from_classified_sqlx_preserves_source_without_leaking_display() {
        let error = QueryError::from_sqlx_with_kind(
            QueryErrorKind::WorkflowReleaseConflict,
            "internal context includes secret-lock-key",
            sqlx::Error::Protocol("database detail includes secret-lock-key".into()),
        );

        assert_eq!(error.category(), QueryErrorCategory::Conflict);
        assert_eq!(error.kind(), Some(QueryErrorKind::WorkflowReleaseConflict));
        assert_eq!(error.code(), "workflow.release_conflict");
        assert_eq!(
            error.client_message(),
            "Workflow step release conflicted with another workflow mutation."
        );
        assert!(error.internal_message().contains("secret-lock-key"));
        assert!(error.source_arc().is_some());
        assert!(std::error::Error::source(&error).is_some());

        let display = error.to_string();
        assert_eq!(
            display,
            "Workflow step release conflicted with another workflow mutation."
        );
        assert!(!display.contains("secret-lock-key"));

        let debug = format!("{error:?}");
        assert_eq!(
            debug,
            "QueryError { category: Conflict, kind: Some(WorkflowReleaseConflict), code: \"workflow.release_conflict\", client_message: \"Workflow step release conflicted with another workflow mutation.\", sqlstate: None, constraint: None, has_source: true }"
        );
        assert!(!debug.contains("secret-lock-key"));
    }

    #[test]
    fn fixed_query_error_kind_metadata_is_exhaustive_and_stable() {
        let cases = [
            (
                QueryErrorKind::JobLeaseOwnerMismatch,
                QueryErrorCategory::Forbidden,
                "job.lease_owner_mismatch",
                "Job lease is not currently held by this worker.",
            ),
            (
                QueryErrorKind::JobInvalidCompletionProgress,
                QueryErrorCategory::Validation,
                "job.invalid_completion_progress",
                "Job completion progress is invalid.",
            ),
            (
                QueryErrorKind::JobInvalidContinuationDelay,
                QueryErrorCategory::Validation,
                "job.invalid_continuation_delay",
                "Job continuation delay is too large.",
            ),
            (
                QueryErrorKind::JobInvalidRetryTiming,
                QueryErrorCategory::Validation,
                "job.invalid_retry_timing",
                "Job retry timing is invalid.",
            ),
            (
                QueryErrorKind::JobUnstartedClaimReleaseNotApplicable,
                QueryErrorCategory::Validation,
                "job.unstarted_claim_release_not_applicable",
                "Job claim cannot be released as unstarted.",
            ),
            (
                QueryErrorKind::JobWorkflowHandlerContinuationNotEnabled,
                QueryErrorCategory::Validation,
                "job.workflow_handler_continuation_not_enabled",
                "Workflow step handler continuation is not enabled.",
            ),
            (
                QueryErrorKind::JobWorkflowRequeueNotSupported,
                QueryErrorCategory::Validation,
                "job.workflow_requeue_not_supported",
                "Workflow-managed jobs cannot be requeued directly.",
            ),
            (
                QueryErrorKind::PostgresLockNotAvailable,
                QueryErrorCategory::Internal,
                "db.query_failed",
                "Database operation failed.",
            ),
            (
                QueryErrorKind::WorkflowReleaseConflict,
                QueryErrorCategory::Conflict,
                "workflow.release_conflict",
                "Workflow step release conflicted with another workflow mutation.",
            ),
        ];

        for (kind, expected_category, expected_code, expected_client_message) in cases {
            let error = QueryError::from_kind(kind, "internal detail");

            assert_eq!(error.kind(), Some(kind));
            assert_eq!(error.category(), expected_category);
            assert_eq!(error.code(), expected_code);
            assert_eq!(error.client_message(), expected_client_message);
        }
    }

    #[test]
    fn maps_only_postgres_lock_not_available_to_a_runtime_policy_kind() {
        assert_eq!(
            QueryErrorKind::from_sqlstate(Some("55P03")),
            Some(QueryErrorKind::PostgresLockNotAvailable)
        );
        assert_eq!(QueryErrorKind::from_sqlstate(Some("57014")), None);
        assert_eq!(QueryErrorKind::from_sqlstate(None), None);
    }

    #[test]
    fn classifies_permission_denied() {
        let spec = classify_database_error(&ErrorKind::Other, Some("42501"), None);
        assert_eq!(spec.category, QueryErrorCategory::Forbidden);
        assert_eq!(spec.code, "db.permission_denied");
    }

    #[test]
    fn falls_back_to_internal_for_unmapped_errors() {
        let spec = classify_database_error(&ErrorKind::Other, Some("99999"), Some("not_mapped"));
        assert_eq!(spec.category, QueryErrorCategory::Internal);
        assert_eq!(spec.code, "db.query_failed");
    }
}
