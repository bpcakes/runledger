use crate::{Error, QueryError, QueryErrorCategory, Result};

use super::types::JOB_LIST_PAGE_LIMIT_MAX;

fn invalid_pagination_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_pagination",
        "Pagination limit and offset are invalid.",
        detail,
    ))
}

pub(super) fn validate_page_limit(limit: i64) -> Result<()> {
    if limit <= 0 {
        return Err(invalid_pagination_error(format!(
            "limit must be greater than zero, got {limit}"
        )));
    }

    if limit > JOB_LIST_PAGE_LIMIT_MAX {
        return Err(invalid_pagination_error(format!(
            "limit must be less than or equal to {JOB_LIST_PAGE_LIMIT_MAX}, got {limit}"
        )));
    }

    Ok(())
}

pub(super) fn validate_pagination(limit: i64, offset: i64) -> Result<()> {
    validate_page_limit(limit)?;

    if offset < 0 {
        return Err(invalid_pagination_error(format!(
            "offset must be greater than or equal to zero, got {offset}"
        )));
    }

    Ok(())
}

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
        "Job lease is not currently held by this worker.",
        "job lease owner mismatch, missing lease, or expired lease",
    ))
}

pub(super) fn validate_positive_lease_duration(lease_duration_seconds: i32) -> Result<()> {
    if lease_duration_seconds > 0 {
        return Ok(());
    }

    Err(Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_lease_duration",
        "Job lease duration must be positive.",
        format!("lease_duration_seconds must be greater than zero, got {lease_duration_seconds}"),
    )))
}

fn invalid_retry_delay_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_retry_delay",
        "Job retry delay must be positive.",
        detail,
    ))
}

pub(super) fn validate_positive_retry_delay(retry_delay_ms: i32) -> Result<()> {
    if retry_delay_ms > 0 {
        return Ok(());
    }

    Err(invalid_retry_delay_error(format!(
        "retry_delay_ms must be greater than zero, got {retry_delay_ms}"
    )))
}

pub(super) fn require_positive_retry_delay(retry_delay_ms: Option<i32>) -> Result<i32> {
    let Some(retry_delay_ms) = retry_delay_ms else {
        return Err(invalid_retry_delay_error(
            "retry_delay_ms is required and must be greater than zero".to_string(),
        ));
    };

    validate_positive_retry_delay(retry_delay_ms)?;
    Ok(retry_delay_ms)
}

fn invalid_completion_progress_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.invalid_completion_progress",
        "Job completion progress is invalid.",
        detail,
    ))
}

pub(super) fn validate_completion_progress(
    progress_done: Option<i64>,
    progress_total: Option<i64>,
) -> Result<()> {
    if let Some(progress_done) = progress_done
        && progress_done < 0
    {
        return Err(invalid_completion_progress_error(format!(
            "progress_done must be greater than or equal to zero, got {progress_done}"
        )));
    }

    if let Some(progress_total) = progress_total
        && progress_total < 0
    {
        return Err(invalid_completion_progress_error(format!(
            "progress_total must be greater than or equal to zero, got {progress_total}"
        )));
    }

    if let (Some(progress_done), Some(progress_total)) = (progress_done, progress_total)
        && progress_done > progress_total
    {
        return Err(invalid_completion_progress_error(format!(
            "progress_done must not exceed progress_total, got progress_done={progress_done}, progress_total={progress_total}"
        )));
    }

    Ok(())
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
