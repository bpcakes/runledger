use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;

const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_CLAIM_BATCH_SIZE: i64 = 16;
const DEFAULT_LEASE_TTL_SECONDS: i32 = 60;
const DEFAULT_MAX_GLOBAL_CONCURRENCY: usize = 32;
const DEFAULT_REAPER_INTERVAL_SECONDS: u64 = 15;
const DEFAULT_SCHEDULE_POLL_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_REAPER_RETRY_DELAY_MS: i32 = 30_000;

/// Maximum batch size accepted by runtime worker, scheduler, and reaper loops.
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
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[derive(Debug)]
    struct ScopedEnv {
        _guard: std::sync::MutexGuard<'static, ()>,
        prior: Vec<(String, Option<String>)>,
    }

    impl ScopedEnv {
        #[allow(
            unsafe_code,
            reason = "Rust 2024 requires unsafe environment mutation, serialized here by ENV_LOCK"
        )]
        fn set(overrides: &[(&str, Option<&str>)]) -> Self {
            let guard = ENV_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            let prior = overrides
                .iter()
                .map(|(key, _)| (key.to_string(), std::env::var(key).ok()))
                .collect();

            // SAFETY: env mutation is serialized through ENV_LOCK.
            unsafe {
                for (key, value) in overrides {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }

            Self {
                _guard: guard,
                prior,
            }
        }
    }

    impl Drop for ScopedEnv {
        #[allow(
            unsafe_code,
            reason = "Rust 2024 requires unsafe environment mutation, serialized here by the held ENV_LOCK guard"
        )]
        fn drop(&mut self) {
            // SAFETY: env mutation is serialized through ENV_LOCK.
            unsafe {
                for (key, value) in self.prior.drain(..) {
                    match value {
                        Some(value) => std::env::set_var(&key, value),
                        None => std::env::remove_var(&key),
                    }
                }
            }
        }
    }

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
