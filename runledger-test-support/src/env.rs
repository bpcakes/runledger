use std::sync::{Mutex, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug)]
pub struct ScopedEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
    prior: Vec<(String, Option<String>)>,
}

impl ScopedEnv {
    /// Temporarily overrides process environment variables and restores them
    /// when the returned guard is dropped.
    ///
    /// Prefer injecting configuration values directly or setting variables on
    /// a child process. The process environment is global shared state, and
    /// this helper's mutex can coordinate only callers of this helper.
    ///
    /// # Safety
    ///
    /// No other thread may read or modify the process environment from before
    /// this call begins until after the returned guard is dropped. Every caller
    /// participating in the test process must uphold that invariant; this
    /// helper cannot enforce it.
    #[allow(
        unsafe_code,
        reason = "caller upholds the process-wide environment access invariant"
    )]
    pub unsafe fn set(overrides: &[(&str, Option<&str>)]) -> Self {
        let guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let prior = overrides
            .iter()
            .map(|(key, _)| (key.to_string(), std::env::var(key).ok()))
            .collect();

        // SAFETY: the caller promises exclusive process-environment access for
        // the guard's full lifetime; ENV_LOCK additionally serializes callers
        // of this helper.
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
        reason = "the constructor's caller upholds the invariant until this guard is dropped"
    )]
    fn drop(&mut self) {
        // SAFETY: ScopedEnv::set requires the caller to exclude all other
        // process-environment access until this restoration completes.
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
