use std::sync::{Mutex, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug)]
pub struct ScopedEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
    prior: Vec<(String, Option<String>)>,
}

impl ScopedEnv {
    #[allow(
        unsafe_code,
        reason = "Rust 2024 requires unsafe environment mutation, serialized here by ENV_LOCK"
    )]
    pub fn set(overrides: &[(&str, Option<&str>)]) -> Self {
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

#[cfg(test)]
mod tests {
    use super::ScopedEnv;

    #[test]
    fn applies_and_restores_environment_overrides() {
        const SET_KEY: &str = "RUNLEDGER_SCOPED_ENV_SET_TEST";
        const REMOVE_KEY: &str = "RUNLEDGER_SCOPED_ENV_REMOVE_TEST";

        let prior_set_value = std::env::var_os(SET_KEY);
        let prior_remove_value = std::env::var_os(REMOVE_KEY);

        {
            let _env = ScopedEnv::set(&[(SET_KEY, Some("temporary")), (REMOVE_KEY, None)]);

            assert_eq!(std::env::var(SET_KEY).as_deref(), Ok("temporary"));
            assert_eq!(std::env::var_os(REMOVE_KEY), None);
        }

        assert_eq!(std::env::var_os(SET_KEY), prior_set_value);
        assert_eq!(std::env::var_os(REMOVE_KEY), prior_remove_value);
    }
}
