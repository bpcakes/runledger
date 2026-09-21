use crate::{
    Error,
    jobs::{JobEnqueueIntentDisposition, JobEnqueueIntentOutcome, JobEnqueueIntentOutcomeState},
};
use sqlx::types::Uuid;
use std::fmt;

/// Only accepted point-in-time handoff observations. This is not a guarantee
/// that asynchronous promotion cannot subsequently conflict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptedIntentState {
    Pending,
    Promoted { job_id: Uuid },
}

/// A known conflicted observation cannot inhabit canonical success.
/// Fields and construction are private; inspect only after runner completion
/// when an acknowledged durable result is needed.
///
/// ```compile_fail,E0599
/// let invalid = runledger_postgres::AcceptedIntentState::Conflicted;
/// ```
/// ```compile_fail,E0451
/// use runledger_postgres::{AcceptedIntentOutcome, AcceptedIntentState};
/// use runledger_postgres::jobs::JobEnqueueIntentDisposition;
/// let forged = AcceptedIntentOutcome {
///     intent_id: sqlx::types::Uuid::nil(), state: AcceptedIntentState::Pending,
///     disposition: JobEnqueueIntentDisposition::Inserted,
/// };
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedIntentOutcome {
    intent_id: Uuid,
    state: AcceptedIntentState,
    disposition: JobEnqueueIntentDisposition,
}

impl AcceptedIntentOutcome {
    pub fn intent_id(&self) -> Uuid {
        self.intent_id
    }
    pub fn state(&self) -> AcceptedIntentState {
        self.state
    }
    pub fn disposition(&self) -> JobEnqueueIntentDisposition {
        self.disposition
    }
    pub(crate) fn require(outcome: JobEnqueueIntentOutcome) -> Result<Self, RequiredIntentError> {
        let state = match outcome.state {
            JobEnqueueIntentOutcomeState::Pending => AcceptedIntentState::Pending,
            JobEnqueueIntentOutcomeState::Promoted { job_id } => {
                AcceptedIntentState::Promoted { job_id }
            }
            JobEnqueueIntentOutcomeState::Conflicted => {
                return Err(RequiredIntentError::Conflict(IntentConflict {
                    intent_id: outcome.intent_id,
                }));
            }
        };
        Ok(Self {
            intent_id: outcome.intent_id,
            state,
            disposition: outcome.disposition,
        })
    }
}

/// Identity of the retained handoff that was observed conflicted.
#[derive(Debug)]
pub struct IntentConflict {
    intent_id: Uuid,
}
impl IntentConflict {
    pub fn intent_id(&self) -> Uuid {
        self.intent_id
    }
}
impl fmt::Display for IntentConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("required enqueue intent is conflicted")
    }
}
impl std::error::Error for IntentConflict {}

/// Canonical handoff rejection. Storage and known conflict stay distinct.
pub enum RequiredIntentError {
    Conflict(IntentConflict),
    Storage(Error),
}
impl fmt::Debug for RequiredIntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for RequiredIntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Conflict(_) => "required enqueue intent is conflicted",
            Self::Storage(_) => "required enqueue intent storage failed",
        })
    }
}
impl std::error::Error for RequiredIntentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Conflict(error) => error,
            Self::Storage(error) => error,
        })
    }
}
