use std::fmt;

/// An owned transaction whose `COMMIT` did not confirm success.
///
/// This is an unknown outcome, not a failed one: the server may have committed
/// before the acknowledgement was lost, or a deferred constraint may have rolled
/// the transaction back. It is a top-level [`crate::Error`] variant, never a
/// [`crate::QueryError`] classification, so exhaustive matches must decide what
/// to do with it and SQLSTATE-based business/validation handling cannot absorb
/// it. Nothing here authorizes replay. Establish the durable outcome with an
/// independent read, or repeat only operations that are idempotent by key.
///
/// Formatting omits the SQLx source because native diagnostics may contain
/// secrets; [`Self::operation`] is fixed library text.
///
/// ```
/// use runledger_postgres::Error;
/// fn outcome_is_unknown(error: &Error) -> bool {
///     match error {
///         Error::CommitUnconfirmed(_) => true,
///         Error::RollbackFailure(_)
///         | Error::QueryError(_)
///         | Error::ConfigError(_)
///         | Error::ConnectionError(_)
///         | Error::MigrationError(_) => false,
///     }
/// }
/// ```
pub struct CommitUnconfirmed {
    operation: &'static str,
    source: sqlx::Error,
}

impl CommitUnconfirmed {
    /// Stable machine-readable code for adapters that report non-query errors.
    pub const CODE: &'static str = "db.transaction_commit_unconfirmed";
    /// Client-safe message; it does not claim that the operation failed.
    pub const CLIENT_MESSAGE: &'static str = "Database transaction commit was not confirmed.";

    pub(crate) fn new(operation: &'static str, source: sqlx::Error) -> Self {
        Self { operation, source }
    }

    /// The library-authored description of the commit that was not confirmed.
    #[must_use]
    pub fn operation(&self) -> &str {
        self.operation
    }

    /// The original SQLx error returned by `COMMIT`, for outage diagnostics.
    #[must_use]
    pub fn sqlx_error(&self) -> &sqlx::Error {
        &self.source
    }

    /// SQLSTATE reported with the commit error, when the server answered at all.
    /// A deferred-constraint SQLSTATE here is diagnostic evidence only.
    #[must_use]
    pub fn sqlstate(&self) -> Option<String> {
        self.source
            .as_database_error()
            .and_then(|error| error.code().map(|code| code.into_owned()))
    }
}

impl fmt::Debug for CommitUnconfirmed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitUnconfirmed")
            .field("operation", &self.operation)
            .field("sqlstate", &self.sqlstate())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CommitUnconfirmed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "transaction commit was not confirmed: {}",
            self.operation
        )
    }
}

impl std::error::Error for CommitUnconfirmed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
mod tests {
    use crate::Error;
    use std::error::Error as _;

    #[test]
    fn commit_unconfirmed_retains_source_without_formatting_it() {
        let error = Error::commit_unconfirmed(
            "commit test operation",
            sqlx::Error::Protocol("private commit".into()),
        );
        let formatted = format!("{error:?} {error}");
        assert!(!formatted.contains("private"));
        assert!(formatted.contains("commit test operation"));
        let Error::CommitUnconfirmed(unconfirmed) = &error else {
            panic!("commit failures are a top-level outcome");
        };
        assert_eq!(unconfirmed.operation(), "commit test operation");
        assert_eq!(unconfirmed.sqlstate(), None);
        let original = error
            .source()
            .expect("unconfirmed commit")
            .source()
            .expect("retained SQLx source")
            .downcast_ref::<sqlx::Error>()
            .expect("original concrete SQLx error");
        assert!(matches!(original, sqlx::Error::Protocol(value) if value == "private commit"));
    }
}
