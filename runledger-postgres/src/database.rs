//! Declared Runledger database authority shared by ordinary and atomic access.
use crate::DbPool;
pub use batter_sqlx::{PgProfileError, PgSessionProfile};
use sqlx::{
    ConnectOptions,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::sync::Arc;

/// A pool whose acquisitions establish the declared database profile. There is
/// deliberately no constructor from an arbitrary pool or arbitrary hook. Existing
/// SET ROLE/search_path/tenant hooks must become explicit profile declarations.
///
/// `pool()` serves ordinary SQLx/native Runledger APIs with exactly the same
/// profile used by `run_atomic`, migrations and schema verification. Connections
/// are normalized before reuse; callers cannot replace the pool hooks.
///
/// ```compile_fail,E0308
/// async fn unprofiled(pool: &sqlx::PgPool) {
///     runledger_postgres::run_atomic(pool, async |_| Ok::<_, ()>(())).await;
/// }
/// ```
#[derive(Clone)]
pub struct RunledgerDatabase {
    pool: DbPool,
    profile: Arc<PgSessionProfile>,
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
    pub async fn connect(
        options: PgConnectOptions,
        profile: PgSessionProfile,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
        Self::validate(&profile, max_connections)?;
        // The probe is never returned to a pool, even on cancellation.
        let mut probe = options.connect().await?;
        profile.reset_and_apply(&mut probe).await?;
        drop(probe);
        let database = Self::connect_lazy(
            options,
            profile,
            PgPoolOptions::new().max_connections(max_connections),
        )?;
        drop(database.pool.acquire().await?);
        Ok(database)
    }

    /// Construct a lazy, profiled pool for an application-owned startup and
    /// cleanup boundary. Every acquisition establishes and verifies policy before
    /// yielding a connection. Construction does not await connectivity; SQLx
    /// may start minimum-connection maintenance in the background. Reserve pool
    /// cleanup before construction and register close before yielding control.
    /// Native capacity/lifetime settings are preserved, but all three session
    /// hooks are replaced: authority must be declared in the profile, not hooks.
    pub fn connect_lazy(
        options: PgConnectOptions,
        profile: PgSessionProfile,
        pool_options: PgPoolOptions,
    ) -> Result<Self, sqlx::Error> {
        Self::validate(&profile, pool_options.get_max_connections())?;
        let profile = Arc::new(profile);
        let on_connect = Arc::clone(&profile);
        let on_acquire = Arc::clone(&profile);
        let pool = pool_options
            .after_connect(move |connection, _| {
                let profile = Arc::clone(&on_connect);
                Box::pin(async move { profile.reset_and_apply(connection).await })
            })
            .before_acquire(move |connection, _| {
                let profile = Arc::clone(&on_acquire);
                Box::pin(async move { profile.reset_and_apply(connection).await.map(|()| true) })
            })
            .after_release(|_, _| Box::pin(async { Ok(true) }))
            .connect_lazy_with(options);
        Ok(Self { pool, profile })
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
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Declared policy, immutable for the lifetime of this pool.
    pub fn profile(&self) -> &PgSessionProfile {
        &self.profile
    }

    /// Authoritative schema; all migration/verification relation names use it.
    pub fn schema(&self) -> &str {
        self.profile.schema()
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
