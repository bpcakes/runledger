use std::str::FromStr;
use std::time::Duration;

const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_CLAIM_BATCH_SIZE: i64 = 16;
const DEFAULT_LEASE_TTL_SECONDS: i32 = 60;
const DEFAULT_MAX_GLOBAL_CONCURRENCY: usize = 32;
const DEFAULT_REAPER_INTERVAL_SECONDS: u64 = 15;
const DEFAULT_SCHEDULE_POLL_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_REAPER_RETRY_DELAY_MS: i32 = 30_000;

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
            claim_batch_size: parse_env("JOBS_CLAIM_BATCH_SIZE", DEFAULT_CLAIM_BATCH_SIZE).max(1),
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
    use super::*;
    use runledger_test_support::ScopedEnv;

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
            ("JOBS_CLAIM_BATCH_SIZE", Some("0")),
            ("JOBS_LEASE_TTL_SECONDS", Some("1")),
            ("JOBS_MAX_GLOBAL_CONCURRENCY", Some("0")),
            ("JOBS_REAPER_RETRY_DELAY_MS", Some("1")),
            ("JOBS_WORKER_ID", Some("   ")),
        ]);

        let config = JobsConfig::from_env();
        assert_eq!(config.claim_batch_size, 1);
        assert_eq!(config.lease_ttl_seconds, 10);
        assert_eq!(config.max_global_concurrency, 1);
        assert_eq!(config.reaper_retry_delay_ms, 1_000);
        assert!(config.worker_id.starts_with("worker-"));
    }
}
