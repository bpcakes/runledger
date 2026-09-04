use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;

const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_CLAIM_BATCH_SIZE: i64 = 16;
const DEFAULT_INTENT_PROMOTER_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_INTENT_PROMOTER_BATCH_SIZE: i64 = 16;
const DEFAULT_LEASE_TTL_SECONDS: i32 = 60;
const DEFAULT_MAX_GLOBAL_CONCURRENCY: usize = 32;
const DEFAULT_REAPER_INTERVAL_SECONDS: u64 = 15;
const DEFAULT_SCHEDULE_POLL_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_REAPER_RETRY_DELAY_MS: i32 = 30_000;

/// Maximum batch size accepted by runtime worker, intent promoter, scheduler,
/// and reaper loops.
pub const JOBS_CLAIM_BATCH_SIZE_MAX: i64 = 1_000;

#[derive(Debug, Clone)]
pub struct JobsConfig {
    pub worker_id: String,
    pub poll_interval: Duration,
    pub claim_batch_size: i64,
    pub lease_ttl_seconds: i32,
    pub max_global_concurrency: usize,
    pub reaper_interval: Duration,
    pub schedule_poll_interval: Duration,
    pub reaper_retry_delay_ms: i32,
}

/// Polling and batch controls for durable enqueue-intent promotion.
///
/// [`crate::Supervisor`] derives these values from [`JobsConfig`] by default so
/// existing deployments keep their current behavior. Use this type when intent
/// traffic needs a cadence independent from ordinary queue claiming.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IntentPromoterConfig {
    poll_interval: Duration,
    batch_size: i64,
}

impl IntentPromoterConfig {
    #[must_use]
    pub const fn new(poll_interval: Duration, batch_size: i64) -> Self {
        Self {
            poll_interval,
            batch_size,
        }
    }

    /// Reads intent-specific settings from the environment.
    ///
    /// `JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS` defaults to 500 milliseconds and
    /// `JOBS_INTENT_PROMOTER_BATCH_SIZE` defaults to 16.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            poll_interval: Duration::from_millis(
                parse_env(
                    "JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS",
                    DEFAULT_INTENT_PROMOTER_POLL_INTERVAL_MS,
                )
                .max(1),
            ),
            batch_size: parse_env(
                "JOBS_INTENT_PROMOTER_BATCH_SIZE",
                DEFAULT_INTENT_PROMOTER_BATCH_SIZE,
            )
            .clamp(1, JOBS_CLAIM_BATCH_SIZE_MAX),
        }
    }

    /// Reads intent-specific settings from the environment, inheriting worker
    /// settings for variables that are absent or invalid.
    ///
    /// This is the environment-composition policy used by
    /// [`crate::Supervisor::builder_from_env`]. It preserves the default
    /// coupling between worker claiming and intent promotion while allowing
    /// either promoter control to be overridden independently.
    #[must_use]
    pub fn from_env_with_jobs_config_defaults(config: &JobsConfig) -> Self {
        let poll_interval = parse_env_value::<u64>("JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS")
            .map(|milliseconds| Duration::from_millis(milliseconds.max(1)))
            .unwrap_or(config.poll_interval);
        let batch_size = parse_env_value::<i64>("JOBS_INTENT_PROMOTER_BATCH_SIZE")
            .map(|batch_size| batch_size.clamp(1, JOBS_CLAIM_BATCH_SIZE_MAX))
            .unwrap_or(config.claim_batch_size);

        Self::new(poll_interval, batch_size)
    }

    #[must_use]
    pub const fn from_jobs_config(config: &JobsConfig) -> Self {
        Self::new(config.poll_interval, config.claim_batch_size)
    }

    pub fn validate(&self) -> Result<(), JobsConfigValidationError> {
        if self.poll_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroPollInterval);
        }
        validate_claim_batch_size(self.batch_size)
    }

    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    #[must_use]
    pub const fn batch_size(&self) -> i64 {
        self.batch_size
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Error, Eq, PartialEq)]
pub enum JobsConfigValidationError {
    #[error("jobs config worker_id must not be empty")]
    EmptyWorkerId,
    #[error("jobs config poll_interval must be greater than zero")]
    ZeroPollInterval,
    #[error("jobs config claim_batch_size must be between 1 and 1000, got {actual}")]
    InvalidClaimBatchSize { actual: i64 },
    #[error("jobs config lease_ttl_seconds must be at least 1, got {actual}")]
    InvalidLeaseTtlSeconds { actual: i32 },
    #[error("jobs config max_global_concurrency must be at least 1")]
    InvalidMaxGlobalConcurrency,
    #[error("jobs config reaper_interval must be greater than zero")]
    ZeroReaperInterval,
    #[error("jobs config schedule_poll_interval must be greater than zero")]
    ZeroSchedulePollInterval,
    #[error("jobs config reaper_retry_delay_ms must be at least 1, got {actual}")]
    InvalidReaperRetryDelayMs { actual: i32 },
}

impl JobsConfig {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            worker_id: std::env::var("JOBS_WORKER_ID")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("worker-{}", uuid::Uuid::now_v7())),
            poll_interval: Duration::from_millis(
                parse_env("JOBS_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS).max(1),
            ),
            claim_batch_size: parse_env("JOBS_CLAIM_BATCH_SIZE", DEFAULT_CLAIM_BATCH_SIZE)
                .clamp(1, JOBS_CLAIM_BATCH_SIZE_MAX),
            lease_ttl_seconds: parse_env("JOBS_LEASE_TTL_SECONDS", DEFAULT_LEASE_TTL_SECONDS)
                .max(10),
            max_global_concurrency: parse_env(
                "JOBS_MAX_GLOBAL_CONCURRENCY",
                DEFAULT_MAX_GLOBAL_CONCURRENCY,
            )
            .max(1),
            reaper_interval: Duration::from_secs(
                parse_env(
                    "JOBS_REAPER_INTERVAL_SECONDS",
                    DEFAULT_REAPER_INTERVAL_SECONDS,
                )
                .max(1),
            ),
            schedule_poll_interval: Duration::from_secs(
                parse_env(
                    "JOBS_SCHEDULE_POLL_INTERVAL_SECONDS",
                    DEFAULT_SCHEDULE_POLL_INTERVAL_SECONDS,
                )
                .max(1),
            ),
            reaper_retry_delay_ms: parse_env(
                "JOBS_REAPER_RETRY_DELAY_MS",
                DEFAULT_REAPER_RETRY_DELAY_MS,
            )
            .max(1_000),
        }
    }

    pub fn validate(&self) -> Result<(), JobsConfigValidationError> {
        if self.worker_id.trim().is_empty() {
            return Err(JobsConfigValidationError::EmptyWorkerId);
        }
        if self.poll_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroPollInterval);
        }
        validate_claim_batch_size(self.claim_batch_size)?;
        if self.lease_ttl_seconds < 1 {
            return Err(JobsConfigValidationError::InvalidLeaseTtlSeconds {
                actual: self.lease_ttl_seconds,
            });
        }
        if self.max_global_concurrency < 1 {
            return Err(JobsConfigValidationError::InvalidMaxGlobalConcurrency);
        }
        if self.reaper_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroReaperInterval);
        }
        if self.schedule_poll_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroSchedulePollInterval);
        }
        if self.reaper_retry_delay_ms < 1 {
            return Err(JobsConfigValidationError::InvalidReaperRetryDelayMs {
                actual: self.reaper_retry_delay_ms,
            });
        }

        Ok(())
    }

    pub(crate) fn validate_worker_loop(&self) -> Result<(), JobsConfigValidationError> {
        if self.worker_id.trim().is_empty() {
            return Err(JobsConfigValidationError::EmptyWorkerId);
        }
        if self.poll_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroPollInterval);
        }
        validate_claim_batch_size(self.claim_batch_size)?;
        if self.lease_ttl_seconds < 1 {
            return Err(JobsConfigValidationError::InvalidLeaseTtlSeconds {
                actual: self.lease_ttl_seconds,
            });
        }
        if self.max_global_concurrency < 1 {
            return Err(JobsConfigValidationError::InvalidMaxGlobalConcurrency);
        }

        Ok(())
    }

    pub(crate) fn validate_scheduler_loop(&self) -> Result<(), JobsConfigValidationError> {
        validate_claim_batch_size(self.claim_batch_size)?;
        if self.schedule_poll_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroSchedulePollInterval);
        }

        Ok(())
    }

    pub(crate) fn validate_reaper_loop(&self) -> Result<(), JobsConfigValidationError> {
        validate_claim_batch_size(self.claim_batch_size)?;
        if self.reaper_interval.is_zero() {
            return Err(JobsConfigValidationError::ZeroReaperInterval);
        }
        if self.reaper_retry_delay_ms < 1 {
            return Err(JobsConfigValidationError::InvalidReaperRetryDelayMs {
                actual: self.reaper_retry_delay_ms,
            });
        }

        Ok(())
    }
}

fn validate_claim_batch_size(claim_batch_size: i64) -> Result<(), JobsConfigValidationError> {
    if (1..=JOBS_CLAIM_BATCH_SIZE_MAX).contains(&claim_batch_size) {
        return Ok(());
    }

    Err(JobsConfigValidationError::InvalidClaimBatchSize {
        actual: claim_batch_size,
    })
}

fn parse_env<T>(name: &str, default: T) -> T
where
    T: FromStr,
{
    parse_env_value(name).unwrap_or(default)
}

fn parse_env_value<T>(name: &str) -> Option<T>
where
    T: FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
}

#[cfg(test)]
mod tests {
    use runledger_test_support::ScopedEnv;

    use super::*;

    fn test_config() -> JobsConfig {
        JobsConfig {
            worker_id: "config-test-worker".to_string(),
            poll_interval: Duration::from_millis(1),
            claim_batch_size: 1,
            lease_ttl_seconds: 1,
            max_global_concurrency: 1,
            reaper_interval: Duration::from_millis(1),
            schedule_poll_interval: Duration::from_millis(1),
            reaper_retry_delay_ms: 1,
        }
    }

    #[test]
    fn validate_accepts_minimum_direct_config_values() {
        test_config()
            .validate()
            .expect("minimum direct config should be valid");
    }

    #[test]
    fn intent_promoter_config_validates_independently() {
        let jobs_config = test_config();
        assert_eq!(
            IntentPromoterConfig::from_jobs_config(&jobs_config),
            IntentPromoterConfig::new(jobs_config.poll_interval, jobs_config.claim_batch_size)
        );
        assert_eq!(
            IntentPromoterConfig::new(Duration::ZERO, 1).validate(),
            Err(JobsConfigValidationError::ZeroPollInterval)
        );
        assert_eq!(
            IntentPromoterConfig::new(Duration::from_millis(1), 0).validate(),
            Err(JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 })
        );
    }

    #[test]
    fn validate_rejects_invalid_direct_config_values() {
        let cases = [
            {
                let mut config = test_config();
                config.worker_id = "  ".to_string();
                (config, JobsConfigValidationError::EmptyWorkerId)
            },
            {
                let mut config = test_config();
                config.poll_interval = Duration::ZERO;
                (config, JobsConfigValidationError::ZeroPollInterval)
            },
            {
                let mut config = test_config();
                config.claim_batch_size = 0;
                (
                    config,
                    JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 },
                )
            },
            {
                let mut config = test_config();
                config.claim_batch_size = JOBS_CLAIM_BATCH_SIZE_MAX + 1;
                (
                    config,
                    JobsConfigValidationError::InvalidClaimBatchSize {
                        actual: JOBS_CLAIM_BATCH_SIZE_MAX + 1,
                    },
                )
            },
            {
                let mut config = test_config();
                config.lease_ttl_seconds = 0;
                (
                    config,
                    JobsConfigValidationError::InvalidLeaseTtlSeconds { actual: 0 },
                )
            },
            {
                let mut config = test_config();
                config.max_global_concurrency = 0;
                (
                    config,
                    JobsConfigValidationError::InvalidMaxGlobalConcurrency,
                )
            },
            {
                let mut config = test_config();
                config.reaper_interval = Duration::ZERO;
                (config, JobsConfigValidationError::ZeroReaperInterval)
            },
            {
                let mut config = test_config();
                config.schedule_poll_interval = Duration::ZERO;
                (config, JobsConfigValidationError::ZeroSchedulePollInterval)
            },
            {
                let mut config = test_config();
                config.reaper_retry_delay_ms = 0;
                (
                    config,
                    JobsConfigValidationError::InvalidReaperRetryDelayMs { actual: 0 },
                )
            },
        ];

        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
        }
    }

    #[test]
    fn from_env_clamps_zero_intervals_to_non_zero_minimum() {
        let _env = ScopedEnv::set(&[
            ("JOBS_POLL_INTERVAL_MS", Some("0")),
            ("JOBS_REAPER_INTERVAL_SECONDS", Some("0")),
            ("JOBS_SCHEDULE_POLL_INTERVAL_SECONDS", Some("0")),
        ]);

        let config = JobsConfig::from_env();
        assert_eq!(config.poll_interval, Duration::from_millis(1));
        assert_eq!(config.reaper_interval, Duration::from_secs(1));
        assert_eq!(config.schedule_poll_interval, Duration::from_secs(1));
    }

    #[test]
    fn intent_promoter_from_env_uses_independent_controls() {
        let _env = ScopedEnv::set(&[
            ("JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS", Some("37")),
            ("JOBS_INTENT_PROMOTER_BATCH_SIZE", Some("9")),
        ]);

        let config = IntentPromoterConfig::from_env();
        assert_eq!(config.poll_interval(), Duration::from_millis(37));
        assert_eq!(config.batch_size(), 9);
    }

    #[test]
    fn intent_promoter_env_overrides_fall_back_to_jobs_config_independently() {
        let _env = ScopedEnv::set(&[
            ("JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS", Some("37")),
            ("JOBS_INTENT_PROMOTER_BATCH_SIZE", None),
        ]);
        let mut jobs_config = test_config();
        jobs_config.poll_interval = Duration::from_millis(83);
        jobs_config.claim_batch_size = 7;

        let config = IntentPromoterConfig::from_env_with_jobs_config_defaults(&jobs_config);
        assert_eq!(config.poll_interval(), Duration::from_millis(37));
        assert_eq!(config.batch_size(), 7);
    }

    #[test]
    fn from_env_clamps_non_interval_limits_and_falls_back_worker_id() {
        let _env = ScopedEnv::set(&[
            ("JOBS_CLAIM_BATCH_SIZE", Some("1001")),
            ("JOBS_LEASE_TTL_SECONDS", Some("1")),
            ("JOBS_MAX_GLOBAL_CONCURRENCY", Some("0")),
            ("JOBS_REAPER_RETRY_DELAY_MS", Some("1")),
            ("JOBS_WORKER_ID", Some("   ")),
        ]);

        let config = JobsConfig::from_env();
        assert_eq!(config.claim_batch_size, JOBS_CLAIM_BATCH_SIZE_MAX);
        assert_eq!(config.lease_ttl_seconds, 10);
        assert_eq!(config.max_global_concurrency, 1);
        assert_eq!(config.reaper_retry_delay_ms, 1_000);
        assert!(config.worker_id.starts_with("worker-"));
    }
}
