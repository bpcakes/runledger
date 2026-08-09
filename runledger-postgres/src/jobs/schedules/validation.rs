use cron::Schedule;
use std::str::FromStr;

use crate::{Error, QueryError, QueryErrorCategory, Result};

use super::super::types::{JOB_SCHEDULE_MAX_JITTER_SECONDS, JobScheduleUpsert};

pub(super) fn validate_job_schedule_upsert(payload: &JobScheduleUpsert<'_>) -> Result<()> {
    validate_job_schedule_name(payload.name)?;

    if payload.cron_expr.trim().is_empty() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must be non-empty.",
            "job schedule cron expression is blank",
        ));
    }

    if payload.cron_expr != payload.cron_expr.trim() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must not have surrounding whitespace.",
            "job schedule cron expression has surrounding whitespace",
        ));
    }

    if Schedule::from_str(payload.cron_expr).is_err() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must be valid.",
            "job schedule cron expression is invalid",
        ));
    }

    if payload.max_jitter_seconds < 0 {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_jitter",
            "Job schedule jitter must be non-negative.",
            "job schedule max_jitter_seconds is negative",
        ));
    }

    if payload.max_jitter_seconds > JOB_SCHEDULE_MAX_JITTER_SECONDS {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_jitter",
            "Job schedule jitter must not exceed 86400 seconds (24h).",
            "job schedule max_jitter_seconds exceeds 86400 seconds",
        ));
    }

    Ok(())
}

pub(super) fn validate_job_schedule_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_name",
            "Job schedule name must be non-empty.",
            "job schedule name is blank",
        ));
    }

    if name != name.trim() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_name",
            "Job schedule name must not have surrounding whitespace.",
            "job schedule name has surrounding whitespace",
        ));
    }

    Ok(())
}

fn job_schedule_validation_error(
    code: &'static str,
    client_message: &'static str,
    internal_message: impl Into<String>,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        internal_message,
    ))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use runledger_core::jobs::JobType;
    use serde_json::json;

    use super::{JOB_SCHEDULE_MAX_JITTER_SECONDS, JobScheduleUpsert, validate_job_schedule_upsert};
    use crate::{Error, QueryErrorCategory};

    fn valid_schedule<'a>(payload_template: &'a serde_json::Value) -> JobScheduleUpsert<'a> {
        JobScheduleUpsert {
            name: "daily-refresh",
            job_type: JobType::new("jobs.refresh"),
            organization_id: None,
            payload_template,
            cron_expr: "0 0 0 * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        }
    }

    fn assert_validation_code(payload: JobScheduleUpsert<'_>, expected_code: &str) {
        let error = validate_job_schedule_upsert(&payload)
            .expect_err("invalid schedule payload should fail validation");

        match error {
            Error::QueryError(query_error) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Validation);
                assert_eq!(query_error.code(), expected_code);
            }
            other => panic!("expected query validation error, got {other:?}"),
        }
    }

    #[test]
    fn validates_schedule_upsert_payload() {
        let payload_template = json!({});
        validate_job_schedule_upsert(&valid_schedule(&payload_template))
            .expect("valid schedule payload should pass validation");

        let mut blank_name = valid_schedule(&payload_template);
        blank_name.name = " ";
        assert_validation_code(blank_name, "job_schedule.invalid_name");

        let mut padded_name = valid_schedule(&payload_template);
        padded_name.name = " daily-refresh ";
        assert_validation_code(padded_name, "job_schedule.invalid_name");

        let mut blank_cron = valid_schedule(&payload_template);
        blank_cron.cron_expr = " ";
        assert_validation_code(blank_cron, "job_schedule.invalid_cron");

        let mut padded_cron = valid_schedule(&payload_template);
        padded_cron.cron_expr = " 0 0 0 * * * ";
        assert_validation_code(padded_cron, "job_schedule.invalid_cron");

        let mut invalid_cron = valid_schedule(&payload_template);
        invalid_cron.cron_expr = "not a cron expression";
        assert_validation_code(invalid_cron, "job_schedule.invalid_cron");

        let mut negative_jitter = valid_schedule(&payload_template);
        negative_jitter.max_jitter_seconds = -1;
        assert_validation_code(negative_jitter, "job_schedule.invalid_jitter");

        let mut excessive_jitter = valid_schedule(&payload_template);
        excessive_jitter.max_jitter_seconds = JOB_SCHEDULE_MAX_JITTER_SECONDS + 1;
        assert_validation_code(excessive_jitter, "job_schedule.invalid_jitter");
    }
}
