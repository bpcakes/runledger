//! PostgreSQL persistence layer for Runledger durable execution.
//!
//! This crate owns the SQLx-backed storage and query helpers used by the
//! runtime and application crates. The main entrypoint is the [`jobs`] module,
//! which exposes APIs for:
//! - queueing, claiming, heartbeating, and completing jobs
//! - listing admin/job log data and runtime configuration
//! - enqueueing and querying workflow runs and steps
//! - applying or validating the bundled Runledger schema migrations
//!
//! Typical consumers share a [`DbPool`] with `runledger-runtime`, then call the
//! exported [`jobs`] functions from application setup, admin APIs, or tests.
//!
//! # Security Boundary
//!
//! This crate is a persistence layer, not an authentication or authorization
//! layer. Public job and workflow APIs that accept `organization_id`,
//! idempotency keys, workflow IDs, job IDs, or metadata expect those values to
//! come from a trusted service boundary. HTTP or RPC handlers should derive
//! organization scope from authenticated claims or server-side policy, not from
//! untrusted request parameters alone.
//!
//! [`QueryError::client_message`] and [`QueryError::code`] are the stable values
//! intended for public error responses. Detailed internal context remains
//! available through [`QueryError::internal_message`] for server-side diagnostics.
//! Raw SQLx errors are kept out of the standard error source chain; use
//! [`QueryError::source_arc`] only in trusted server-side diagnostics.
//! Runtime lifecycle, workflow mutation, and idempotent enqueue APIs are designed
//! for PostgreSQL's default `READ COMMITTED` transaction isolation so they can
//! observe rows committed after lock waits or uniqueness conflicts.
//! Release-sensitive, workflow-append, and keyed-enqueue paths validate this
//! before running because their correctness depends on second reads after waits
//! or conflicts.
//!
//! For simple embedding, call [`migrate_after_idempotency_cutover`] during
//! startup:
//!
//! ```rust,no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = sqlx::PgPool::connect("postgres://localhost/runledger").await?;
//! runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;
//! # Ok(())
//! # }
//! ```
//!
//! For deployments that manage DDL elsewhere, call
//! [`ensure_schema_compatible_after_idempotency_cutover`] instead to fail fast
//! if the schema is missing, drifted, or still has keyed legacy rows without
//! idempotency request snapshots. That check is read-only, but it expects the
//! database to retain SQLx migration history in `_sqlx_migrations` and, when
//! available, Runledger-owned migration state in `runledger_migration_history`.

use std::fmt;

mod error;
pub mod jobs;
mod migrations;

pub use error::{
    FrameworkConstraintSpec, QueryError, QueryErrorCategory, classify_framework_constraint,
    classify_query_error, classify_query_error_with_constraint_classifier,
    has_framework_constraint_classifier,
};
pub use migrations::{
    MIGRATOR, SchemaCompatibilityError, ensure_schema_compatible_after_idempotency_cutover,
    migrate_after_idempotency_cutover,
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
