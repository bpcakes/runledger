//! Declared Runledger database authority shared by ordinary and atomic access.
use crate::DbPool;
use batter_sqlx::PgProfiledPool;
pub use batter_sqlx::{PgProfileError, PgSessionProfile};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// A pool whose acquisitions establish the declared database profile. There is
/// deliberately no constructor from an arbitrary pool or arbitrary hook. Existing
/// SET ROLE/search_path/tenant hooks must become explicit profile declarations.
///
/// `pool()` serves ordinary SQLx/native Runledger APIs with exactly the same
/// profile used by `run_atomic`, migrations and schema verification. Connections
/// are normalized before entering the idle queue, including for native fast
/// acquisition paths; callers cannot replace the pool hooks.
///
/// ```compile_fail,E0308
/// async fn unprofiled(pool: &sqlx::PgPool) {
///     runledger_postgres::run_atomic(pool, async |_| Ok::<_, ()>(())).await;
/// }
/// ```
#[derive(Clone)]
pub struct RunledgerDatabase {
    database: PgProfiledPool,
}

impl std::fmt::Debug for RunledgerDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunledgerDatabase")
    }
}

impl RunledgerDatabase {
    /// Connect under explicit policy. Schemas/roles must already exist; this does
    /// not provision them. A direct probe preserves configuration errors rather
    /// than reducing them to a pool retry timeout. Zero capacity is rejected.
    /// Runledger admits one ordinary schema, without fallback schemas. Its path
    /// uses the implicit catalog first and pg_temp last. Qualify application
    /// objects located elsewhere. Use a separate declared migration role when
    /// serving credentials intentionally lack DDL privileges.
    /// Profile reset/setup failures have redacted default Debug/Display, with
    /// the original error retained inside a `batter_sqlx::SqlxFailure` payload
    /// of `sqlx::Error::Configuration`. Connection-establishment errors remain
    /// native SQLx errors; this is not a blanket log-redaction guarantee.
    pub async fn connect(
        options: PgConnectOptions,
        profile: PgSessionProfile,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
        Self::validate(&profile, max_connections)?;
        Ok(Self {
            database: PgProfiledPool::connect(options, profile, max_connections).await?,
        })
    }

    /// Construct a lazy, profiled pool for an application-owned startup and
    /// cleanup boundary. Every acquisition establishes and verifies policy before
    /// yielding a connection. Construction does not await connectivity; SQLx
    /// may start minimum-connection maintenance in the background. Reserve pool
    /// cleanup before construction and register close before yielding control.
    /// Native capacity/lifetime settings are preserved, but all three session
    /// hooks are replaced: authority must be declared in the profile, not hooks.
    /// Returned sessions are reset and verified before becoming idle. Failed
    /// restoration discards the connection; `try_*` calls can return `None`
    /// while asynchronous release cleanup is still running.
    /// Setup errors reach SQLx's hook logging with redacted default formatting.
    /// Deliberate source inspection, independent query/notice logging and server
    /// logs remain application/operator responsibilities.
    pub fn connect_lazy(
        options: PgConnectOptions,
        profile: PgSessionProfile,
        pool_options: PgPoolOptions,
    ) -> Result<Self, sqlx::Error> {
        Self::validate(&profile, pool_options.get_max_connections())?;
        Ok(Self {
            database: PgProfiledPool::connect_lazy(options, profile, pool_options)?,
        })
    }

    fn validate(profile: &PgSessionProfile, max_connections: u32) -> Result<(), sqlx::Error> {
        // Runledger's static queries use unqualified names. A single ordinary
        // schema prevents a missing object silently resolving in a fallback
        // schema. Application objects elsewhere must be explicitly qualified.
        if profile.trusted_schemas().len() != 1 {
            return Err(sqlx::Error::Protocol(
                "Runledger requires one authoritative schema, without fallback schemas".into(),
            ));
        }
        if max_connections == 0 {
            return Err(sqlx::Error::Protocol(
                "Runledger pool capacity must be positive".into(),
            ));
        }
        Ok(())
    }

    /// Native access for ordinary APIs. Acquisitions enforce this database's
    /// profile. Raw SQL remains an escape hatch, not an atomic result guarantee.
    /// Even native `try_acquire`/`try_begin`/`try_begin_with` receive sessions
    /// restored before idle admission, without running acquisition hooks.
    pub fn pool(&self) -> &DbPool {
        self.database.pool()
    }

    /// Declared policy, immutable for the lifetime of this pool.
    pub fn profile(&self) -> &PgSessionProfile {
        self.database.profile()
    }

    /// Authoritative schema; all migration/verification relation names use it.
    pub fn schema(&self) -> &str {
        self.profile().schema()
    }

    pub(crate) fn relation(&self, name: &str) -> String {
        format!(
            "\"{}\".\"{}\"",
            self.schema().replace('"', "\"\""),
            name.replace('"', "\"\"")
        )
    }

    pub(crate) fn schema_identifier(&self) -> String {
        format!("\"{}\"", self.schema().replace('"', "\"\""))
    }
}
