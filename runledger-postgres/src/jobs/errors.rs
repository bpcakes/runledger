use crate::{Error, QueryError, QueryErrorCategory, QueryErrorKind, Result};

use super::types::JOB_LIST_PAGE_LIMIT_MAX;

pub(super) const JOB_REPLAY_REQUEST_KEY_MAX_BYTES: usize = 512;

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

pub(super) fn ensure_rejection_rollback_succeeded(
    rollback_result: std::result::Result<(), sqlx::Error>,
) -> Result<()> {
    rollback_result.map_err(|error| Error::ConnectionError(error.to_string()))
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
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobLeaseOwnerMismatch,
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

pub(super) fn invalid_retry_timing_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobInvalidRetryTiming,
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

fn invalid_completion_progress_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobInvalidCompletionProgress,
        detail,
    ))
}

pub(super) fn validate_completion_progress(
    progress_done: Option<i64>,
    progress_total: Option<i64>,
) -> Result<()> {
    runledger_core::jobs::validate_job_progress(progress_done, progress_total).map_err(|error| {
        use runledger_core::jobs::JobProgressValidationError;
        let detail = match error {
            JobProgressValidationError::NegativeDone { actual } => {
                format!("progress_done must be greater than or equal to zero, got {actual}")
            }
            JobProgressValidationError::NegativeTotal { actual } => {
                format!("progress_total must be greater than or equal to zero, got {actual}")
            }
            _ => error.to_string(),
        };
        invalid_completion_progress_error(detail)
    })
}

pub(super) fn invalid_continuation_delay_error(detail: String) -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobInvalidContinuationDelay,
        detail,
    ))
}

pub(super) fn unstarted_claim_release_not_applicable_error() -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobUnstartedClaimReleaseNotApplicable,
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
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobWorkflowRequeueNotSupported,
        "workflow managed job requeue is not supported",
    ))
}

pub(super) fn workflow_handler_continuation_not_enabled_error() -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::JobWorkflowHandlerContinuationNotEnabled,
        "workflow-managed job continuation requires allow_handler_continuation",
    ))
}

pub(super) fn validate_job_replay_request(replay_request_key: &str, reason: &str) -> Result<()> {
    if replay_request_key.trim().is_empty() {
        return Err(Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job.replay_request_key_blank",
            "Job replay request key must not be blank.",
            "job replay request key was blank",
        )));
    }

    if replay_request_key.len() > JOB_REPLAY_REQUEST_KEY_MAX_BYTES {
        return Err(Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job.replay_request_key_too_long",
            "Job replay request key is too long.",
            format!(
                "job replay request key must be at most {JOB_REPLAY_REQUEST_KEY_MAX_BYTES} bytes, got {}",
                replay_request_key.len()
            ),
        )));
    }

    if reason.trim().is_empty() {
        return Err(Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job.replay_reason_blank",
            "Job replay reason must not be blank.",
            "job replay reason was blank",
        )));
    }

    Ok(())
}

pub(super) fn job_replay_idempotency_conflict_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "job.replay_idempotency_conflict",
        "Job replay request conflicts with the existing replay key.",
        "job replay request reused its key with different request metadata",
    ))
}

pub(super) fn job_replay_missing_existing_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.replay_idempotency_conflict_missing_existing",
        "Job replay retry could not be resolved.",
        "job replay lineage referenced an existing replay job that could not be loaded",
    ))
}

#[cfg(test)]
mod tests {
    use super::ensure_rejection_rollback_succeeded;
    use crate::Error;

    #[test]
    fn rejection_allows_domain_error_after_successful_rollback() {
        let result = ensure_rejection_rollback_succeeded(Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn rejection_returns_connection_error_when_rollback_fails() {
        let result = ensure_rejection_rollback_succeeded(Err(sqlx::Error::PoolTimedOut));
        match result {
            Err(Error::ConnectionError(message)) => assert!(!message.is_empty()),
            other => panic!("expected connection error, got {other:?}"),
        }
    }
}
