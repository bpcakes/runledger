use std::borrow::Cow;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::JobFailureKind;

/// What Runledger should do after a handler successfully finishes its bounded
/// unit of work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobCompletionDisposition {
    /// Finish the logical job successfully.
    Succeed,
    /// Close the current attempt successfully and schedule the same logical
    /// job for another run.
    ContinueAfter(Duration),
}

impl Default for JobCompletionDisposition {
    fn default() -> Self {
        Self::Succeed
    }
}

/// A handler's in-process completion result.
///
/// Its serde representation supports same-version use and reading older stored
/// values with newer Runledger versions. It is not a rolling-upgrade wire
/// protocol: an older consumer cannot safely interpret dispositions introduced
/// by a newer Runledger version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobCompletion {
    #[serde(default)]
    disposition: JobCompletionDisposition,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<serde_json::Value>,
    output: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SerializedJobCompletion {
    #[serde(default)]
    disposition: JobCompletionDisposition,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    checkpoint: Option<serde_json::Value>,
    output: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for JobCompletion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedJobCompletion::deserialize(deserializer)?;
        if matches!(
            serialized.disposition,
            JobCompletionDisposition::ContinueAfter(_)
        ) && serialized.output.is_some()
        {
            return Err(serde::de::Error::custom(
                "continuation completion cannot carry final output",
            ));
        }

        Ok(Self {
            disposition: serialized.disposition,
            progress_done: serialized.progress_done,
            progress_total: serialized.progress_total,
            checkpoint: serialized.checkpoint,
            output: serialized.output,
        })
    }
}

impl JobCompletion {
    #[must_use]
    pub fn success() -> Self {
        Self {
            disposition: JobCompletionDisposition::Succeed,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            output: None,
        }
    }

    /// Successfully finishes this bounded slice of a direct job and makes the
    /// same logical job immediately eligible for another run.
    #[must_use]
    pub fn continue_now() -> Self {
        Self::continue_after(Duration::ZERO)
    }

    /// Successfully finishes this bounded slice of a direct job and schedules
    /// the same logical job for another run after `delay`.
    ///
    /// Continuations cannot carry final output; that state is not constructible
    /// through this API and is rejected during deserialization.
    #[must_use]
    pub fn continue_after(delay: Duration) -> Self {
        Self {
            disposition: JobCompletionDisposition::ContinueAfter(delay),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            output: None,
        }
    }

    /// Sets the final JSON output for this job.
    ///
    /// Workflow result steps persist this value on the job, step, and workflow
    /// run rows, so keep it to compact metadata and store large artifacts
    /// externally.
    #[must_use]
    pub fn with_output(output: serde_json::Value) -> Self {
        Self {
            output: Some(output),
            ..Self::success()
        }
    }

    /// Returns the terminal-success or continuation disposition.
    #[must_use]
    pub const fn disposition(&self) -> JobCompletionDisposition {
        self.disposition
    }

    /// Returns final output for terminal success, if provided.
    #[must_use]
    pub const fn output(&self) -> Option<&serde_json::Value> {
        self.output.as_ref()
    }

    #[must_use]
    pub fn progress(mut self, progress_done: i64, progress_total: i64) -> Self {
        self.progress_done = Some(progress_done);
        self.progress_total = Some(progress_total);
        self
    }

    #[must_use]
    pub fn checkpoint(mut self, checkpoint: serde_json::Value) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::{JobCompletion, JobCompletionDisposition, JobContext, JobFailure, JobRetryTiming};

    #[test]
    fn continuation_constructors_preserve_progress_and_checkpoint_builders() {
        let immediate = JobCompletion::continue_now()
            .progress(4, 10)
            .checkpoint(json!({"cursor": 4}));
        assert_eq!(
            immediate.disposition(),
            JobCompletionDisposition::ContinueAfter(Duration::ZERO)
        );
        assert_eq!(immediate.progress_done, Some(4));
        assert_eq!(immediate.progress_total, Some(10));
        assert_eq!(immediate.checkpoint, Some(json!({"cursor": 4})));
        assert_eq!(immediate.output(), None);

        let delayed = JobCompletion::continue_after(Duration::from_secs(30));
        assert_eq!(
            delayed.disposition(),
            JobCompletionDisposition::ContinueAfter(Duration::from_secs(30))
        );
    }

    #[test]
    fn terminal_success_uses_succeed_disposition() {
        assert_eq!(
            JobCompletion::success().disposition(),
            JobCompletionDisposition::Succeed
        );
        assert_eq!(
            JobCompletion::with_output(json!({"result": "ok"})).disposition(),
            JobCompletionDisposition::Succeed
        );
    }

    #[test]
    fn legacy_serialized_completion_defaults_to_succeed() {
        let completion: JobCompletion = serde_json::from_value(json!({
            "progress_done": 4,
            "progress_total": 10,
            "checkpoint": {"cursor": 4},
            "output": null
        }))
        .expect("pre-0.6 completion should remain deserializable");

        assert_eq!(completion.disposition(), JobCompletionDisposition::Succeed);
        assert_eq!(completion.progress_done, Some(4));
        assert_eq!(completion.progress_total, Some(10));
        assert_eq!(completion.checkpoint, Some(json!({"cursor": 4})));
        assert_eq!(completion.output(), None);
    }

    #[test]
    fn serialized_continuation_with_output_is_rejected() {
        let error = serde_json::from_value::<JobCompletion>(json!({
            "disposition": {"continue_after": {"secs": 0, "nanos": 0}},
            "progress_done": null,
            "progress_total": null,
            "checkpoint": null,
            "output": {"must_not_be_dropped": true}
        }))
        .expect_err("continuation output must be rejected at the type boundary");

        assert!(
            error
                .to_string()
                .contains("continuation completion cannot carry final output")
        );
    }

    #[test]
    fn legacy_serialized_job_context_defaults_to_no_checkpoint() {
        let context: JobContext = serde_json::from_value(json!({
            "job_id": "00000000-0000-0000-0000-000000000001",
            "run_number": 1,
            "attempt": 1,
            "organization_id": null,
            "worker_id": "legacy-worker"
        }))
        .expect("pre-checkpoint job context should remain deserializable");

        assert_eq!(context.checkpoint, None);
    }

    #[test]
    fn retry_timing_builders_use_the_last_selection() {
        let retry_at = Utc
            .with_ymd_and_hms(2026, 7, 28, 12, 30, 0)
            .single()
            .expect("valid reset timestamp");
        let absolute = JobFailure::retryable("provider.rate_limited", "retry at reset")
            .retry_not_before_delay(Duration::from_secs(30))
            .retry_not_before(retry_at);
        assert_eq!(absolute.retry_timing(), Some(JobRetryTiming::At(retry_at)));

        let relative = JobFailure::retryable("provider.rate_limited", "retry later")
            .retry_not_before(retry_at)
            .retry_not_before_delay(Duration::from_secs(45));
        assert_eq!(
            relative.retry_timing(),
            Some(JobRetryTiming::After(Duration::from_secs(45)))
        );
    }

    #[test]
    fn ordinary_failure_serialization_keeps_the_legacy_shape() {
        let serialized = serde_json::to_value(JobFailure::retryable(
            "job.test.retryable",
            "retryable failure",
        ))
        .expect("serialize retryable failure");

        assert_eq!(
            serialized,
            json!({
                "kind": "RETRYABLE",
                "code": "job.test.retryable",
                "message": "retryable failure"
            })
        );
    }

    #[test]
    fn relative_retry_timing_serializes_exactly() {
        let serialized = serde_json::to_value(
            JobFailure::retryable("provider.rate_limited", "retry later")
                .retry_not_before_delay(Duration::from_millis(250)),
        )
        .expect("serialize timed retryable failure");

        assert_eq!(
            serialized,
            json!({
                "kind": "RETRYABLE",
                "code": "provider.rate_limited",
                "message": "retry later",
                "retry_timing": {
                    "after": {
                        "secs": 0,
                        "nanos": 250_000_000
                    }
                }
            })
        );
    }

    #[test]
    fn absolute_retry_timing_serializes_exactly() {
        let retry_at = Utc
            .with_ymd_and_hms(2026, 7, 28, 12, 30, 0)
            .single()
            .expect("valid reset timestamp");
        let serialized = serde_json::to_value(
            JobFailure::retryable("provider.rate_limited", "retry at reset")
                .retry_not_before(retry_at),
        )
        .expect("serialize absolute retry timing");

        assert_eq!(
            serialized,
            json!({
                "kind": "RETRYABLE",
                "code": "provider.rate_limited",
                "message": "retry at reset",
                "retry_timing": {
                    "at": "2026-07-28T12:30:00Z"
                }
            })
        );
    }
}

/// A handler-selected lower bound for another attempt after a retryable failure.
///
/// This timing is consulted only when the failed attempt remains retryable.
/// Terminal failures and failures that exhaust `max_attempts` are dead-lettered
/// without validating or applying the requested timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JobRetryTiming {
    /// Do not schedule another attempt before this duration has elapsed from
    /// the persistence database's completion clock.
    After(Duration),
    /// Do not schedule another attempt before this absolute UTC provider reset
    /// time.
    At(DateTime<Utc>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobFailure {
    pub kind: JobFailureKind,
    pub code: &'static str,
    pub message: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_timing: Option<JobRetryTiming>,
}

impl JobFailure {
    #[must_use]
    pub fn new(
        kind: JobFailureKind,
        code: &'static str,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            retry_timing: None,
        }
    }

    #[must_use]
    pub fn retryable(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Retryable, code, message)
    }

    #[must_use]
    pub fn terminal(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Terminal, code, message)
    }

    #[must_use]
    pub fn timeout(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Timeout, code, message)
    }

    #[must_use]
    pub fn lease_expired(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::LeaseExpired, code, message)
    }

    #[must_use]
    pub fn panicked(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Panicked, code, message)
    }

    /// Deprecated name for [`Self::retry_not_before_delay`].
    ///
    /// This method does not override policy backoff: the duration is a lower
    /// bound, despite the older `retry_after` name.
    #[deprecated(
        since = "0.8.0",
        note = "use retry_not_before_delay; relative retry timing is a lower bound, not an exact override"
    )]
    #[must_use]
    pub fn retry_after(mut self, delay: Duration) -> Self {
        self.retry_timing = Some(JobRetryTiming::After(delay));
        self
    }

    /// Requests that another attempt not run before `delay` has elapsed when
    /// this failure remains retryable.
    ///
    /// The failed attempt still consumes the job's attempt budget. A positive
    /// sub-millisecond delay is rounded up to one millisecond by persistence.
    /// Zero supplies no additional lower bound. A winning delay that cannot be
    /// represented is converted into a terminal `job.invalid_retry_timing`
    /// failure by the runtime. The effective schedule is the later of ordinary
    /// policy backoff and this lower bound.
    #[must_use]
    pub fn retry_not_before_delay(mut self, delay: Duration) -> Self {
        self.retry_timing = Some(JobRetryTiming::After(delay));
        self
    }

    /// Deprecated name for [`Self::retry_not_before`].
    ///
    /// This method does not override policy backoff: the timestamp is a lower
    /// bound, despite the older `retry_at` name.
    #[deprecated(
        since = "0.8.0",
        note = "use retry_not_before; absolute retry timing is a lower bound, not an exact override"
    )]
    #[must_use]
    pub fn retry_at(mut self, retry_at: DateTime<Utc>) -> Self {
        self.retry_timing = Some(JobRetryTiming::At(retry_at));
        self
    }

    /// Requests that another attempt not run before `retry_not_before`.
    ///
    /// The failed attempt still consumes the job's attempt budget. The
    /// timestamp is a lower bound; the effective schedule is the later of
    /// ordinary policy backoff and this hint.
    #[must_use]
    pub fn retry_not_before(mut self, retry_not_before: DateTime<Utc>) -> Self {
        self.retry_timing = Some(JobRetryTiming::At(retry_not_before));
        self
    }

    /// Returns the handler-selected retry lower bound, if one was supplied.
    #[must_use]
    pub const fn retry_timing(&self) -> Option<JobRetryTiming> {
        self.retry_timing
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobDeadLetterReason {
    FailureKindNonRetryable,
    AttemptsExhausted,
    LeaseExpired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobDeadLetterInfo {
    pub failure: JobFailure,
    pub reason: JobDeadLetterReason,
    pub max_attempts: Option<i32>,
}

impl JobDeadLetterInfo {
    #[must_use]
    pub fn new(
        failure: JobFailure,
        reason: JobDeadLetterReason,
        max_attempts: Option<i32>,
    ) -> Self {
        Self {
            failure,
            reason,
            max_attempts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobContext {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub organization_id: Option<Uuid>,
    pub worker_id: String,
    /// Durable resume checkpoint captured for this run. Handler execution
    /// receives the checkpoint stored before execution; dead-letter hooks
    /// receive the latest checkpoint committed before terminal persistence.
    #[serde(default)]
    pub checkpoint: Option<serde_json::Value>,
}
