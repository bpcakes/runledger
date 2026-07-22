use std::collections::HashMap;
use std::fmt;

use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migrator};

use crate::DbPool;

/// Raw SQLx migrator for the migrations bundled with this crate version.
///
/// SQLx's [`Migrator::run`] rejects applied versions absent from this exact
/// bundle. During an additive compatibility window, an older binary should use
/// Runledger's filtered [`migrate_after_idempotency_cutover`] or
/// [`ensure_schema_compatible_after_idempotency_cutover`] startup API instead
/// of invoking its raw `MIGRATOR`: the filtered APIs also enforce Runledger's
/// compatibility-fence history. If an exact old binary unavoidably invokes its
/// raw migrator, use a compatible patched startup path or explicitly accept the
/// data-loss boundary of reverting newer migrations before starting it. For
/// the post-v0.6 successful-replay migration, that includes relational replay
/// lineage and replay-request idempotency state.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

type PgPoolConnection = sqlx::pool::PoolConnection<sqlx::Postgres>;
type RunledgerMigrationMap = HashMap<i64, &'static sqlx::migrate::Migration>;

#[derive(Debug)]
#[non_exhaustive]
pub enum SchemaCompatibilityError {
    Query(sqlx::Error),
    MissingMigrationHistory {
        required_first_migration_version: i64,
    },
    LegacyIdempotencySnapshotsMissing {
        job_count: i64,
        workflow_count: i64,
    },
    Incompatible(MigrateError),
    MigrationUnlock(MigrateError),
}

impl fmt::Display for SchemaCompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => write!(
                f,
                "Runledger schema compatibility check could not query PostgreSQL state: {error}"
            ),
            Self::MissingMigrationHistory {
                required_first_migration_version,
            } => write!(
                f,
                "Runledger schema compatibility check requires the _sqlx_migrations table; apply or record Runledger migrations first (expected migration history starting at version {required_first_migration_version})"
            ),
            Self::LegacyIdempotencySnapshotsMissing {
                job_count,
                workflow_count,
            } => write!(
                f,
                "Runledger idempotency cutover requires enqueue_request snapshots for all keyed rows; found {job_count} legacy job rows and {workflow_count} legacy workflow rows"
            ),
            Self::Incompatible(error) => write!(f, "{error}"),
            Self::MigrationUnlock(error) => {
                write!(
                    f,
                    "Runledger schema migration lock could not be released: {error}"
                )
            }
        }
    }
}

impl std::error::Error for SchemaCompatibilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(error) => Some(error),
            Self::MissingMigrationHistory { .. } => None,
            Self::LegacyIdempotencySnapshotsMissing { .. } => None,
            Self::Incompatible(error) | Self::MigrationUnlock(error) => Some(error),
        }
    }
}

impl From<MigrateError> for SchemaCompatibilityError {
    fn from(error: MigrateError) -> Self {
        Self::Incompatible(error)
    }
}

impl From<sqlx::Error> for SchemaCompatibilityError {
    fn from(error: sqlx::Error) -> Self {
        Self::Query(error)
    }
}

/// Apply the bundled Runledger schema migrations to a PostgreSQL pool, then
/// enforce the idempotency snapshot cutover.
///
/// This is intentionally named as a hard-cutover API. Downstream applications
/// upgrading from older Runledger versions must update their startup code and
/// verify no keyed legacy rows remain without `enqueue_request` snapshots.
/// Unlike raw [`MIGRATOR`] execution, this filters shared SQLx history through
/// Runledger's migration compatibility fence so declared additive migrations
/// can coexist with older compatible startup code.
pub async fn migrate_after_idempotency_cutover(
    pool: &DbPool,
) -> Result<(), SchemaCompatibilityError> {
    let mut conn = pool.acquire().await?;

    if MIGRATOR.locking {
        // PostgreSQL advisory migration locks are session-scoped; never return
        // a possibly locked session to the pool if this future is cancelled.
        conn.close_on_drop();
        (*conn)
            .lock()
            .await
            .map_err(SchemaCompatibilityError::Incompatible)?;
    }

    let result = run_migrations_with_filtered_history(&mut conn).await;
    let unlock_result = if MIGRATOR.locking {
        (*conn).unlock().await
    } else {
        Ok(())
    };

    match (result, unlock_result) {
        (Err(migration_error), Err(unlock_error)) => {
            tracing::error!(
                error = %unlock_error,
                "failed to unlock migration lock after migration failure"
            );
            Err(SchemaCompatibilityError::Incompatible(migration_error))
        }
        (Err(error), Ok(())) => Err(SchemaCompatibilityError::Incompatible(error)),
        (Ok(()), Err(error)) => Err(SchemaCompatibilityError::MigrationUnlock(error)),
        (Ok(()), Ok(())) => {
            // The DDL migration lock is no longer needed here: the NOT VALID
            // cutover constraints already block new violating rows, and
            // validation is idempotent if another startup validates first.
            reject_legacy_idempotency_rows(&mut conn).await?;
            validate_idempotency_cutover_constraints(&mut conn).await
        }
    }
}

/// Apply the bundled Runledger schema migrations to a PostgreSQL pool.
///
/// Deprecated compatibility alias for [`migrate_after_idempotency_cutover`].
/// The current migration set enforces the enqueue request snapshot cutover, so
/// this function has the same strict behavior as the new explicit API.
#[deprecated(
    since = "0.1.2",
    note = "use migrate_after_idempotency_cutover to make the enqueue request snapshot cutover explicit"
)]
pub async fn migrate(pool: &DbPool) -> Result<(), SchemaCompatibilityError> {
    migrate_after_idempotency_cutover(pool).await
}

/// Validate that the target database's SQLx migration history matches the
/// bundled Runledger migrations.
///
/// Unlike [`migrate_after_idempotency_cutover`], this does not apply pending
/// migrations. It is intended
/// for deployments that manage DDL outside the application process but still
/// want a startup guardrail. This check is read-only, but it relies on the
/// `_sqlx_migrations` history table being present and up to date. When present,
/// it also uses Runledger's own `runledger_migration_history` compatibility
/// fence to detect newer releases whose schema is not declared backward
/// compatible. Additive migrations may deliberately rely only on SQLx history
/// so older guards can coexist during expand-first rollout.
/// This differs from invoking raw [`MIGRATOR`] execution, which rejects any
/// applied migration version absent from that exact binary's bundle.
///
/// This read-only path does not validate `NOT VALID` cutover constraints after
/// legacy rows are remediated. Deployments that apply DDL externally can run
/// PostgreSQL `VALIDATE CONSTRAINT` for the idempotency cutover constraints
/// after this check passes, or use [`migrate_after_idempotency_cutover`] to let
/// Runledger do that promotion.
pub async fn ensure_schema_compatible_after_idempotency_cutover(
    pool: &DbPool,
) -> Result<(), SchemaCompatibilityError> {
    let mut conn = pool.acquire().await?;

    if !has_migrations_table(&mut conn).await? {
        return Err(SchemaCompatibilityError::MissingMigrationHistory {
            required_first_migration_version: first_up_migration_version(),
        });
    }

    let expected_migrations = expected_runledger_migrations();
    let history = list_migration_history(&mut conn).await?;

    if let Some(version) = first_conflicting_runledger_version(&history, &expected_migrations) {
        return Err(SchemaCompatibilityError::Incompatible(
            MigrateError::VersionMismatch(version),
        ));
    }

    if let Some(version) = first_dirty_runledger_version(&history, &expected_migrations) {
        return Err(SchemaCompatibilityError::Incompatible(MigrateError::Dirty(
            version,
        )));
    }

    if has_runledger_migration_history_table(&mut conn).await? {
        let recorded_versions = list_recorded_runledger_migrations(&mut conn).await?;
        if let Some(version) =
            first_missing_runledger_version(&recorded_versions, &expected_migrations)
        {
            return Err(SchemaCompatibilityError::Incompatible(
                MigrateError::VersionMissing(version),
            ));
        }
    }

    let applied = applied_runledger_migrations(&history, &expected_migrations);
    let applied_by_version: HashMap<_, _> = applied
        .iter()
        .map(|applied_migration| (applied_migration.version, applied_migration))
        .collect();
    let latest_applied_version = applied.iter().map(|migration| migration.version).max();

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        match applied_by_version.get(&migration.version) {
            Some(applied_migration) => {
                validate_checksum(migration.version, applied_migration, migration)
                    .map_err(SchemaCompatibilityError::from)?
            }
            None => {
                return Err(SchemaCompatibilityError::Incompatible(
                    MigrateError::VersionTooNew(
                        migration.version,
                        latest_applied_version.unwrap_or_default(),
                    ),
                ));
            }
        }
    }

    reject_legacy_idempotency_rows(&mut conn).await
}

/// Validate that the target database's SQLx migration history matches the
/// bundled Runledger migrations.
///
/// Deprecated compatibility alias for
/// [`ensure_schema_compatible_after_idempotency_cutover`]. The current schema
/// compatibility check rejects keyed legacy rows without enqueue request
/// snapshots, matching the stricter cutover API.
#[deprecated(
    since = "0.1.2",
    note = "use ensure_schema_compatible_after_idempotency_cutover to make the enqueue request snapshot cutover explicit"
)]
pub async fn ensure_schema_compatible(pool: &DbPool) -> Result<(), SchemaCompatibilityError> {
    ensure_schema_compatible_after_idempotency_cutover(pool).await
}

async fn has_migrations_table(conn: &mut PgPoolConnection) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut **conn)
        .await
}

async fn has_runledger_migration_history_table(
    conn: &mut PgPoolConnection,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass('runledger_migration_history') IS NOT NULL")
        .fetch_one(&mut **conn)
        .await
}

async fn list_migration_history(
    conn: &mut PgPoolConnection,
) -> Result<Vec<MigrationHistoryRow>, sqlx::Error> {
    sqlx::query_as::<_, MigrationHistoryRow>(
        "SELECT version, checksum, success
         FROM _sqlx_migrations
         ORDER BY version",
    )
    .fetch_all(&mut **conn)
    .await
}

async fn list_recorded_runledger_migrations(
    conn: &mut PgPoolConnection,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT version
         FROM runledger_migration_history
         ORDER BY version",
    )
    .fetch_all(&mut **conn)
    .await
}

async fn reject_legacy_idempotency_rows(
    conn: &mut PgPoolConnection,
) -> Result<(), SchemaCompatibilityError> {
    if idempotency_cutover_constraints_valid(conn).await? {
        return Ok(());
    }

    let row = sqlx::query!(
        r#"SELECT
            (
                SELECT COUNT(*)::bigint
                FROM job_queue
                WHERE idempotency_key IS NOT NULL
                  AND enqueue_request IS NULL
            ) AS "job_count!",
            (
                SELECT COUNT(*)::bigint
                FROM workflow_runs
                WHERE idempotency_key IS NOT NULL
                  AND enqueue_request IS NULL
            ) AS "workflow_count!""#,
    )
    .fetch_one(&mut **conn)
    .await?;

    if row.job_count == 0 && row.workflow_count == 0 {
        return Ok(());
    }

    Err(
        SchemaCompatibilityError::LegacyIdempotencySnapshotsMissing {
            job_count: row.job_count,
            workflow_count: row.workflow_count,
        },
    )
}

async fn validate_idempotency_cutover_constraints(
    conn: &mut PgPoolConnection,
) -> Result<(), SchemaCompatibilityError> {
    if idempotency_cutover_constraints_valid(conn).await? {
        return Ok(());
    }

    // PostgreSQL validates each table constraint independently. If one
    // validation succeeds and the other fails, the next startup skips the valid
    // constraint and retries the remaining one.
    sqlx::query(
        "ALTER TABLE job_queue
         VALIDATE CONSTRAINT ck_job_queue_idempotency_enqueue_request",
    )
    .execute(&mut **conn)
    .await
    .map_err(|error| {
        tracing::warn!(
            error = %error,
            "failed to validate job_queue idempotency cutover constraint"
        );
        SchemaCompatibilityError::Query(error)
    })?;

    sqlx::query(
        "ALTER TABLE workflow_runs
         VALIDATE CONSTRAINT ck_workflow_runs_idempotency_enqueue_request",
    )
    .execute(&mut **conn)
    .await
    .map_err(|error| {
        tracing::warn!(
            error = %error,
            "failed to validate workflow_runs idempotency cutover constraint"
        );
        SchemaCompatibilityError::Query(error)
    })?;

    Ok(())
}

async fn idempotency_cutover_constraints_valid(
    conn: &mut PgPoolConnection,
) -> Result<bool, sqlx::Error> {
    // A validated cutover constraint is the durable proof that legacy keyed rows
    // without enqueue_request snapshots cannot exist for that table. If future
    // migrations replace these constraints, they must preserve that invariant
    // before this short-circuit remains valid.
    sqlx::query_scalar::<_, bool>(
        "SELECT COUNT(*) FILTER (WHERE c.convalidated) = 2
         FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         WHERE (t.relname, c.conname) IN (
             ('job_queue', 'ck_job_queue_idempotency_enqueue_request'),
             ('workflow_runs', 'ck_workflow_runs_idempotency_enqueue_request')
         )",
    )
    .fetch_one(&mut **conn)
    .await
}

fn first_up_migration_version() -> i64 {
    MIGRATOR
        .iter()
        .find(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .unwrap_or_default()
}

fn expected_runledger_migrations() -> RunledgerMigrationMap {
    MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| (migration.version, migration))
        .collect()
}

fn first_conflicting_runledger_version(
    history: &[MigrationHistoryRow],
    expected_migrations: &RunledgerMigrationMap,
) -> Option<i64> {
    history.iter().find_map(|row| {
        expected_migrations
            .get(&row.version)
            .filter(|migration| row.checksum.as_slice() != migration.checksum.as_ref())
            .map(|_| row.version)
    })
}

fn first_dirty_runledger_version(
    history: &[MigrationHistoryRow],
    expected_migrations: &RunledgerMigrationMap,
) -> Option<i64> {
    history.iter().filter(|row| !row.success).find_map(|row| {
        expected_migrations
            .get(&row.version)
            .filter(|migration| row.checksum.as_slice() == migration.checksum.as_ref())
            .map(|_| row.version)
    })
}

fn first_missing_runledger_version(
    recorded_versions: &[i64],
    expected_migrations: &RunledgerMigrationMap,
) -> Option<i64> {
    recorded_versions
        .iter()
        .copied()
        .find(|version| !expected_migrations.contains_key(version))
}

fn applied_runledger_migrations(
    history: &[MigrationHistoryRow],
    expected_migrations: &RunledgerMigrationMap,
) -> Vec<AppliedMigration> {
    history
        .iter()
        .filter(|row| row.success)
        .filter(|row| {
            expected_migrations
                .get(&row.version)
                .is_some_and(|migration| row.checksum.as_slice() == migration.checksum.as_ref())
        })
        .map(|row| AppliedMigration {
            version: row.version,
            checksum: row.checksum.clone().into(),
        })
        .collect()
}

async fn run_migrations_with_filtered_history(
    conn: &mut PgPoolConnection,
) -> Result<(), MigrateError> {
    (**conn).ensure_migrations_table().await?;

    let expected_migrations = expected_runledger_migrations();
    let history = list_migration_history(conn).await?;

    if let Some(version) = first_conflicting_runledger_version(&history, &expected_migrations) {
        return Err(MigrateError::VersionMismatch(version));
    }

    if let Some(version) = first_dirty_runledger_version(&history, &expected_migrations) {
        return Err(MigrateError::Dirty(version));
    }

    if has_runledger_migration_history_table(conn).await? {
        let recorded_versions = list_recorded_runledger_migrations(conn).await?;
        if let Some(version) =
            first_missing_runledger_version(&recorded_versions, &expected_migrations)
        {
            return Err(MigrateError::VersionMissing(version));
        }
    }

    let applied = applied_runledger_migrations(&history, &expected_migrations);
    let applied_by_version: HashMap<_, _> = applied
        .into_iter()
        .map(|migration| (migration.version, migration))
        .collect();

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        match applied_by_version.get(&migration.version) {
            Some(applied_migration) => {
                validate_checksum(migration.version, applied_migration, migration)?
            }
            None => {
                (**conn).apply(migration).await?;
            }
        }
    }

    Ok(())
}

#[derive(sqlx::FromRow)]
struct MigrationHistoryRow {
    version: i64,
    checksum: Vec<u8>,
    success: bool,
}

fn validate_checksum(
    version: i64,
    applied_migration: &AppliedMigration,
    expected_migration: &sqlx::migrate::Migration,
) -> Result<(), MigrateError> {
    if applied_migration.checksum != expected_migration.checksum {
        return Err(MigrateError::VersionMismatch(version));
    }

    Ok(())
}
