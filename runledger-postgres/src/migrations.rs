use std::collections::HashMap;
use std::fmt;

use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migrator};

use crate::DbPool;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug)]
pub enum SchemaCompatibilityError {
    Query(sqlx::Error),
    MissingMigrationHistory {
        required_first_migration_version: i64,
    },
    Incompatible(MigrateError),
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
            Self::Incompatible(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SchemaCompatibilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(error) => Some(error),
            Self::MissingMigrationHistory { .. } => None,
            Self::Incompatible(error) => Some(error),
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

/// Apply the bundled Runledger schema migrations to a PostgreSQL pool.
pub async fn migrate(pool: &DbPool) -> Result<(), MigrateError> {
    let mut conn = pool.acquire().await?;

    if MIGRATOR.locking {
        (*conn).lock().await?;
    }

    let result = run_migrations_with_filtered_history(&mut conn).await;
    let unlock_result = if MIGRATOR.locking {
        (*conn).unlock().await
    } else {
        Ok(())
    };

    match (result, unlock_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Validate that the target database's SQLx migration history matches the
/// bundled Runledger migrations.
///
/// Unlike [`migrate`], this does not apply pending migrations. It is intended
/// for deployments that manage DDL outside the application process but still
/// want a startup guardrail. This check is read-only, but it relies on the
/// `_sqlx_migrations` history table being present and up to date. When present,
/// it also uses Runledger's own `runledger_migration_history` table to detect
/// migrations applied by newer Runledger releases.
pub async fn ensure_schema_compatible(pool: &DbPool) -> Result<(), SchemaCompatibilityError> {
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

    Ok(())
}

async fn has_migrations_table(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut **conn)
        .await
}

async fn has_runledger_migration_history_table(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass('runledger_migration_history') IS NOT NULL")
        .fetch_one(&mut **conn)
        .await
}

async fn list_migration_history(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
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
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT version
         FROM runledger_migration_history
         ORDER BY version",
    )
    .fetch_all(&mut **conn)
    .await
}

fn first_up_migration_version() -> i64 {
    MIGRATOR
        .iter()
        .find(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .unwrap_or_default()
}

fn expected_runledger_migrations() -> HashMap<i64, &'static sqlx::migrate::Migration> {
    MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| (migration.version, migration))
        .collect()
}

fn first_conflicting_runledger_version(
    history: &[MigrationHistoryRow],
    expected_migrations: &HashMap<i64, &'static sqlx::migrate::Migration>,
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
    expected_migrations: &HashMap<i64, &'static sqlx::migrate::Migration>,
) -> Option<i64> {
    history.iter().find_map(|row| {
        (!row.success)
            .then(|| {
                expected_migrations
                    .get(&row.version)
                    .filter(|migration| row.checksum.as_slice() == migration.checksum.as_ref())
                    .map(|_| row.version)
            })
            .flatten()
    })
}

fn first_missing_runledger_version(
    recorded_versions: &[i64],
    expected_migrations: &HashMap<i64, &'static sqlx::migrate::Migration>,
) -> Option<i64> {
    recorded_versions
        .iter()
        .copied()
        .find(|version| !expected_migrations.contains_key(version))
}

fn applied_runledger_migrations(
    history: &[MigrationHistoryRow],
    expected_migrations: &HashMap<i64, &'static sqlx::migrate::Migration>,
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
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
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
