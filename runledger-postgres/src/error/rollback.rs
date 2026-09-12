use crate::Error;
use std::fmt;

/// A failed operation followed by a failed explicit rollback. Neither cause is
/// discarded; formatting omits both because native diagnostics may contain secrets.
///
/// ```
/// use runledger_postgres::Error;
/// fn inspect(error: &Error) -> Option<(&Error, &sqlx::Error)> {
///     match error {
///         Error::RollbackFailure(pair) => Some((&pair.operation, &pair.rollback)),
///         _ => None,
///     }
/// }
/// ```
pub struct RollbackFailure {
    /// Original operation or scope-classification failure.
    pub operation: Error,
    /// Original SQLx error returned while rolling back that operation.
    pub rollback: sqlx::Error,
}

impl fmt::Debug for RollbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for RollbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation failed and rollback failed; both causes retained")
    }
}

impl std::error::Error for RollbackFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn rollback_failure_retains_both_sources_without_formatting_them() {
        let failure = Error::RollbackFailure(Box::new(RollbackFailure {
            operation: Error::from_query_sqlx(sqlx::Error::Protocol("private operation".into())),
            rollback: sqlx::Error::Protocol("private rollback".into()),
        }));
        assert!(!format!("{failure:?} {failure}").contains("private"));
        let Error::RollbackFailure(pair) = &failure else {
            unreachable!()
        };
        assert!(
            matches!(&pair.rollback, sqlx::Error::Protocol(value) if value == "private rollback")
        );
        let native = pair.operation.source().expect("native operation source");
        let original = native
            .source()
            .expect("retained SQLx source")
            .downcast_ref::<sqlx::Error>()
            .expect("original concrete SQLx error");
        assert!(matches!(original, sqlx::Error::Protocol(value) if value == "private operation"));
    }
}
