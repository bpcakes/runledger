use crate::{Error, QueryError, QueryErrorCategory};

pub(super) fn invalid_job_state_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_state_transition",
        "Job cannot transition from its current state.",
        "job state transition precondition failed",
    ))
}

pub(super) fn job_not_found_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.not_found",
        "Job was not found.",
        "job lookup failed",
    ))
}

pub(super) fn lease_owner_mismatch_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Forbidden,
        "job.lease_owner_mismatch",
        "Job lease is owned by another worker.",
        "job lease owner mismatch",
    ))
}

pub(super) fn unstarted_claim_release_not_applicable_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.unstarted_claim_release_not_applicable",
        "Job claim cannot be released as unstarted.",
        "job claim is not eligible for unstarted release",
    ))
}

pub(super) fn runtime_config_not_found_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.runtime_config_not_found",
        "Job runtime configuration was not found.",
        format!("required runtime config missing for job type: {job_type}"),
    ))
}

pub(super) fn workflow_requeue_not_supported_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.workflow_requeue_not_supported",
        "Workflow-managed jobs cannot be requeued directly.",
        "workflow managed job requeue is not supported",
    ))
}
