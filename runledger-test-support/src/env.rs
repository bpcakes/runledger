use std::sync::{Mutex, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug)]
pub struct ScopedEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
    prior: Vec<(String, Option<String>)>,
}

impl ScopedEnv {
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
