//! PostgreSQL persistence layer for Runledger durable execution.
//!
//! This crate owns the SQLx-backed storage and query helpers used by the
//! runtime and application crates. The main entrypoint is the [`jobs`] module,
//! which exposes APIs for:
//! - queueing, claiming, heartbeating, and completing jobs
//! - listing admin/job log data and runtime configuration
//! - enqueueing and querying workflow runs and steps
//!
//! Typical consumers share a [`DbPool`] with `runledger-runtime`, then call the
//! exported [`jobs`] functions from application setup, admin APIs, or tests.
//! This crate assumes the matching Runledger schema is already present in the
//! target database.

use std::fmt;

mod error;
pub mod jobs;

pub use error::{
    FrameworkConstraintSpec, QueryError, QueryErrorCategory, classify_framework_constraint,
    classify_query_error, classify_query_error_with_constraint_classifier,
    has_framework_constraint_classifier,
};

pub type DbPool = sqlx::PgPool;
pub type DbTx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ConfigError(String),
    ConnectionError(String),
    MigrationError(String),
    QueryError(QueryError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(message) => write!(f, "{message}"),
            Self::ConnectionError(message) => write!(f, "{message}"),
            Self::MigrationError(message) => write!(f, "{message}"),
            Self::QueryError(query_error) => write!(f, "{query_error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueryError(query_error) => Some(query_error),
            Self::ConfigError(_) | Self::ConnectionError(_) | Self::MigrationError(_) => None,
        }
    }
}

impl Error {
    #[must_use]
    pub fn from_query_sqlx(error: sqlx::Error) -> Self {
        Self::QueryError(QueryError::from_sqlx(error, None))
    }

    #[must_use]
    pub fn from_query_sqlx_with_context(context: &str, error: sqlx::Error) -> Self {
        Self::QueryError(QueryError::from_sqlx(error, Some(context)))
    }
}
