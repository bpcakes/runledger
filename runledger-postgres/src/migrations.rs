use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migration, Migrator};

use crate::DbPool;

/// Raw SQLx migrator for inspecting the migrations bundled with this crate
/// version.
///
/// Iterating this value to inspect bundled versions and checksums is supported.
/// Calling [`Migrator::run`] or [`Migrator::undo`] on it with a shared
/// application pool is not. SQLx rejects applied versions absent from the exact
/// bundle, and PostgreSQL migration locks are session-scoped; SQLx can return
/// from a validation error before unlocking and put the still-locked session
/// back into the pool.
///
/// Use [`migrate_after_idempotency_cutover`] to apply Runledger migrations, or
/// [`ensure_schema_compatible_after_idempotency_cutover`] when DDL is managed
/// externally. If a compatibility diagnostic intentionally executes a raw
/// migrator that may mismatch history, give it a disposable connection or
/// single-use pool and close that connection or pool on every error path.
///
/// During an additive compatibility window, an exact older binary that cannot
/// use the filtered API must be patched before startup or explicitly accept the
/// data-loss boundary of reverting newer migrations. Reverting the 0.8
/// migrations erases workflow-recovery lineage/idempotency, active claims,
/// execution-resource keys and claims, retry audit fields, and workflow-step
/// continuation opt-ins. Reverting the post-v0.6 successful-replay migration
/// also erases relational replay lineage and replay-request idempotency state
/// while retaining the underlying replay-created queue rows.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

type PgPoolConnection = sqlx::pool::PoolConnection<sqlx::Postgres>;
type RunledgerMigrationMap = HashMap<i64, &'static sqlx::migrate::Migration>;

const MIGRATION_LOCK_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(100);
const MIGRATION_LOCK_RETRY_NOMINAL_MAX_INTERVAL: Duration = Duration::from_millis(1_600);
const MIGRATION_LOCK_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(2);
const MIGRATION_LOCK_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(30);

struct MigrationLockBackoff {
    attempt: u32,
    jitter_state: u64,
}

impl MigrationLockBackoff {
    fn new(seed: u64) -> Self {
        Self {
            attempt: 0,
            // Xorshift has an absorbing all-zero state.
            jitter_state: seed.max(1),
        }
    }

    fn next_delay(&mut self) -> Duration {
        let multiplier = 1_u32 << self.attempt.min(4);
        let nominal = MIGRATION_LOCK_RETRY_INITIAL_INTERVAL
            .saturating_mul(multiplier)
            .min(MIGRATION_LOCK_RETRY_NOMINAL_MAX_INTERVAL);
        self.attempt = self.attempt.saturating_add(1);

        // Add up to 25% jitter. This keeps retries bounded while preventing a
        // set of replicas that started together from polling in lockstep.
        let jitter_window = nominal / 4;
        let jitter_bound_nanos = u64::try_from(jitter_window.as_nanos()).unwrap_or(u64::MAX - 1);
        let jitter =
            Duration::from_nanos(self.next_random() % jitter_bound_nanos.saturating_add(1));

        nominal
            .saturating_add(jitter)
            .min(MIGRATION_LOCK_RETRY_MAX_INTERVAL)
    }

    fn next_random(&mut self) -> u64 {
        let mut value = self.jitter_state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.jitter_state = value;
        value
    }
}

struct RecoverableConcurrentIndex {
    migration_version: i64,
    index_name: &'static str,
    table_name: &'static str,
    key_columns: &'static [&'static str],
    key_options: &'static [i16],
    drop_sql: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConcurrentIndexRecovery {
    ApplyMigration,
    RecordExistingIndex,
}

#[derive(Debug, sqlx::FromRow)]
struct ConcurrentIndexState {
    relation_exists: bool,
    is_valid: Option<bool>,
    is_unique: Option<bool>,
    has_only_key_columns: Option<bool>,
    has_no_predicate: Option<bool>,
    has_only_default_operator_classes: Option<bool>,
    has_only_column_default_collations: Option<bool>,
    access_method: Option<String>,
    key_options: Option<Vec<i16>>,
    key_columns: Option<Vec<String>>,
}

const RECOVERABLE_CONCURRENT_INDEXES: &[RecoverableConcurrentIndex] = &[
    RecoverableConcurrentIndex {
        migration_version: 202608210001,
        index_name: "idx_job_events_job_id_newest",
        table_name: "job_events",
        key_columns: &["job_id", "id"],
        key_options: &[0, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_job_events_job_id_newest",
    },
    RecoverableConcurrentIndex {
        migration_version: 202608210002,
        index_name: "idx_job_logs_job_id_newest",
        table_name: "job_logs",
        key_columns: &["job_id", "id"],
        key_options: &[0, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_job_logs_job_id_newest",
    },
    RecoverableConcurrentIndex {
        migration_version: 202608210003,
        index_name: "idx_job_queue_admin_created",
        table_name: "job_queue",
        key_columns: &["created_at", "id"],
        key_options: &[3, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_job_queue_admin_created",
    },
    RecoverableConcurrentIndex {
        migration_version: 202608210004,
        index_name: "idx_job_queue_admin_org_created",
        table_name: "job_queue",
        key_columns: &["organization_id", "created_at", "id"],
        key_options: &[0, 3, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_job_queue_admin_org_created",
    },
    RecoverableConcurrentIndex {
        migration_version: 202608210005,
        index_name: "idx_workflow_runs_admin_created",
        table_name: "workflow_runs",
        key_columns: &["created_at", "id"],
        key_options: &[3, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_workflow_runs_admin_created",
    },
    RecoverableConcurrentIndex {
        migration_version: 202608210006,
        index_name: "idx_workflow_runs_admin_org_created",
        table_name: "workflow_runs",
        key_columns: &["organization_id", "created_at", "id"],
        key_options: &[0, 3, 3],
        drop_sql: "DROP INDEX CONCURRENTLY idx_workflow_runs_admin_org_created",
    },
];

const WORKFLOW_STEP_JOB_LINK_CONTRACT_MIGRATION_VERSION: i64 = 202608240002;
const AFTER_ROW_INSERT_OR_UPDATE_TRIGGER_TYPE: i16 = 21;

// Trigger names remain exact because the contract migration drops these named
// objects before removing job_queue.workflow_step_id. The validator below
// otherwise checks the safety properties needed during the mixed-version
// window, not byte-for-byte DDL identity: firing on additional UPDATE columns
// is safe, while changing events, timing, functions, or deferral is not.
// Migration checksums establish the function source applied initially. This
// read-only guard validates live catalog wiring and reciprocal data; it does
// not try to detect privileged post-migration function-body replacement by
// embedding a second, brittle copy of each PL/pgSQL body.
#[derive(Clone, Copy)]
struct WorkflowJobLinkTriggerSpec {
    table_name: &'static str,
    trigger_name: &'static str,
    function_name: &'static str,
    update_column_name: &'static str,
    constraint_mode: WorkflowJobLinkTriggerConstraintMode,
}

#[derive(Clone, Copy)]
enum WorkflowJobLinkTriggerConstraintMode {
    Deferred,
    Ordinary,
}

const WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS: [WorkflowJobLinkTriggerSpec; 3] = [
    WorkflowJobLinkTriggerSpec {
        table_name: "job_queue",
        trigger_name: "trg_job_queue_workflow_step_linkage_symmetry",
        function_name: "enforce_workflow_job_linkage_symmetry",
        update_column_name: "workflow_step_id",
        constraint_mode: WorkflowJobLinkTriggerConstraintMode::Deferred,
    },
    WorkflowJobLinkTriggerSpec {
        table_name: "workflow_steps",
        trigger_name: "trg_workflow_steps_job_linkage_symmetry",
        function_name: "enforce_workflow_job_linkage_symmetry",
        update_column_name: "job_id",
        constraint_mode: WorkflowJobLinkTriggerConstraintMode::Deferred,
    },
    WorkflowJobLinkTriggerSpec {
        table_name: "workflow_steps",
        trigger_name: "trg_workflow_steps_job_linkage_compatibility",
        function_name: "project_workflow_step_job_linkage_compatibility",
        update_column_name: "job_id",
        constraint_mode: WorkflowJobLinkTriggerConstraintMode::Ordinary,
    },
];

/// One reason an expand-window workflow/job-link trigger is unsafe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowJobLinkTriggerProblem {
    Missing,
    WrongFunction,
    NotEnabledForOriginWrites,
    InternallyGenerated,
    WrongFiringEvents,
    UpdateColumnNotCovered,
    UnexpectedTriggerArguments,
    UnexpectedWhenCondition,
    WrongConstraintMode,
}

impl fmt::Display for WorkflowJobLinkTriggerProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Missing => "missing from the expected public table",
            Self::WrongFunction => {
                "does not call the expected public zero-argument trigger function"
            }
            Self::NotEnabledForOriginWrites => "does not fire for origin/local writes",
            Self::InternallyGenerated => "is internally generated instead of user-defined",
            Self::WrongFiringEvents => "is not an AFTER ROW INSERT OR UPDATE trigger",
            Self::UpdateColumnNotCovered => "does not fire when the relationship column is updated",
            Self::UnexpectedTriggerArguments => "passes unexpected trigger arguments",
            Self::UnexpectedWhenCondition => "has an unexpected WHEN condition",
            Self::WrongConstraintMode => "has the wrong constraint or deferral mode",
        })
    }
}

/// Validation details for one unsafe expand-window workflow/job-link trigger.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct WorkflowJobLinkTriggerDiagnostic {
    table_name: &'static str,
    trigger_name: &'static str,
    problems: Vec<WorkflowJobLinkTriggerProblem>,
}

impl WorkflowJobLinkTriggerDiagnostic {
    #[must_use]
    pub const fn table_name(&self) -> &str {
        self.table_name
    }

    #[must_use]
    pub const fn trigger_name(&self) -> &str {
        self.trigger_name
    }

    #[must_use]
    pub fn problems(&self) -> &[WorkflowJobLinkTriggerProblem] {
        &self.problems
    }
}

impl fmt::Display for WorkflowJobLinkTriggerDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "public.{}.{}: ", self.table_name, self.trigger_name)?;
        for (index, problem) in self.problems.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{problem}")?;
        }
        Ok(())
    }
}

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
    WorkflowJobLinkExpandInvalid {
        compatibility_trigger_count: i64,
        inconsistent_link_count: i64,
    },
    WorkflowJobLinkExpandTriggersInvalid {
        trigger_diagnostics: Vec<WorkflowJobLinkTriggerDiagnostic>,
        inconsistent_link_count: i64,
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
            Self::WorkflowJobLinkExpandInvalid {
                compatibility_trigger_count,
                inconsistent_link_count,
            } => write!(
                f,
                "Runledger workflow-step/job expand schema requires all three expand-window triggers and empty reciprocal anti-joins before the contract migration; found {compatibility_trigger_count} valid triggers and {inconsistent_link_count} inconsistent relationships"
            ),
            Self::WorkflowJobLinkExpandTriggersInvalid {
                trigger_diagnostics,
                inconsistent_link_count,
            } => {
                f.write_str(
                    "Runledger workflow-step/job expand schema has invalid expand-window triggers: ",
                )?;
                for (index, diagnostic) in trigger_diagnostics.iter().enumerate() {
                    if index != 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "{diagnostic}")?;
                }
                write!(
                    f,
                    "; found {inconsistent_link_count} inconsistent relationships"
                )
            }
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
            Self::WorkflowJobLinkExpandInvalid { .. }
            | Self::WorkflowJobLinkExpandTriggersInvalid { .. } => None,
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
/// This function applies every pending bundled migration immediately, including
/// the workflow-step/job-link contract migration that removes
/// `job_queue.workflow_step_id` and crosses the 0.10 rollback boundary. For a
/// mixed-version rollout, apply the expand migration externally, deploy and
/// drain all 0.10 writers and leases, then apply the contract migration; use
/// [`ensure_schema_compatible_after_idempotency_cutover`] for startup checks
/// during that staged rollout.
///
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
        acquire_migration_lock(&mut conn)
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

/// Acquire SQLx's PostgreSQL migration lock without leaving a statement-level
/// snapshot open while another migrator owns it.
///
/// `CREATE INDEX CONCURRENTLY` waits for snapshots that predate its validation
/// scan. A blocking `pg_advisory_lock` statement can therefore deadlock with
/// the lock holder when the holder is applying a concurrent-index migration.
/// Polling `pg_try_advisory_lock` gives each unsuccessful attempt a bounded
/// statement lifetime while preserving coordination with raw SQLx migrators.
async fn acquire_migration_lock(conn: &mut PgPoolConnection) -> Result<(), MigrateError> {
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&mut **conn)
        .await?;
    let lock_id = sqlx_migration_lock_id(&database_name);
    let jitter_seed = RandomState::new().hash_one((lock_id, std::process::id()));
    let mut retry_backoff = MigrationLockBackoff::new(jitter_seed);
    let wait_started = Instant::now();
    let mut next_wait_log = MIGRATION_LOCK_WAIT_LOG_INTERVAL;
    let mut waiting = false;

    loop {
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut **conn)
            .await?;
        if acquired {
            if waiting {
                tracing::info!(
                    database_name,
                    waited_seconds = wait_started.elapsed().as_secs(),
                    "acquired Runledger migration lock"
                );
            }
            return Ok(());
        }
        let retry_delay = retry_backoff.next_delay();
        if waiting {
            let elapsed = wait_started.elapsed();
            if elapsed >= next_wait_log {
                tracing::info!(
                    database_name,
                    waited_seconds = elapsed.as_secs(),
                    "still waiting for Runledger migration lock"
                );
                next_wait_log = next_wait_log.saturating_add(MIGRATION_LOCK_WAIT_LOG_INTERVAL);
            }
        } else {
            tracing::info!(
                database_name,
                retry_interval_ms = retry_delay.as_millis(),
                "waiting for Runledger migration lock"
            );
            waiting = true;
        }
        tokio::time::sleep(retry_delay).await;
    }
}

/// Match SQLx 0.8's private PostgreSQL migrator key so mixed-version rolling
/// deployments and callers of the raw SQLx migrator still serialize together.
/// The PostgreSQL-backed `safe_migrator_does_not_deadlock_concurrent_index_migrations_while_waiting`
/// regression test must remain enabled when updating SQLx because it verifies
/// this private upstream contract end to end.
fn sqlx_migration_lock_id(database_name: &str) -> i64 {
    const CRC_IEEE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    0x3d32ad9e * i64::from(CRC_IEEE.checksum(database_name.as_bytes()))
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
/// The workflow-step/job contract migration may also remain pending while its
/// expand migration's compatibility projection is present and consistent.
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
                if migration.version == WORKFLOW_STEP_JOB_LINK_CONTRACT_MIGRATION_VERSION {
                    continue;
                }
                return Err(SchemaCompatibilityError::Incompatible(
                    MigrateError::VersionTooNew(
                        migration.version,
                        latest_applied_version.unwrap_or_default(),
                    ),
                ));
            }
        }
    }

    validate_workflow_job_link_expand_schema(&mut conn).await?;
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

async fn validate_workflow_job_link_expand_schema(
    conn: &mut PgPoolConnection,
) -> Result<(), SchemaCompatibilityError> {
    let deprecated_column_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'job_queue'
              AND column_name = 'workflow_step_id'
         )",
    )
    .fetch_one(&mut **conn)
    .await?;
    if !deprecated_column_exists {
        return Ok(());
    }

    let trigger_catalog = workflow_job_link_trigger_catalog(conn).await?;
    let trigger_diagnostics = workflow_job_link_trigger_diagnostics(&trigger_catalog);
    let inconsistent_link_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM (
            SELECT jq.id
            FROM job_queue jq
            WHERE jq.workflow_step_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM workflow_steps ws
                  WHERE ws.id = jq.workflow_step_id
                    AND ws.job_id = jq.id
              )

            UNION ALL

            SELECT ws.id
            FROM workflow_steps ws
            WHERE ws.job_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM job_queue jq
                  WHERE jq.id = ws.job_id
                    AND jq.workflow_step_id = ws.id
              )
         ) inconsistencies",
    )
    .fetch_one(&mut **conn)
    .await?;

    if !trigger_diagnostics.is_empty() {
        return Err(
            SchemaCompatibilityError::WorkflowJobLinkExpandTriggersInvalid {
                trigger_diagnostics,
                inconsistent_link_count,
            },
        );
    }

    if inconsistent_link_count == 0 {
        return Ok(());
    }

    Err(SchemaCompatibilityError::WorkflowJobLinkExpandInvalid {
        compatibility_trigger_count: i64::try_from(WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS.len())
            .expect("workflow job-link trigger count fits i64"),
        inconsistent_link_count,
    })
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct WorkflowJobLinkTriggerCatalogRow {
    table_name: String,
    trigger_name: String,
    function_schema: String,
    function_name: String,
    function_argument_count: i16,
    returns_trigger: bool,
    enabled_mode: String,
    is_internal: bool,
    trigger_type: i16,
    update_column_names: Vec<String>,
    trigger_argument_count: i16,
    has_when_condition: bool,
    is_constraint: bool,
    is_deferrable: bool,
    is_initially_deferred: bool,
}

async fn workflow_job_link_trigger_catalog(
    conn: &mut PgPoolConnection,
) -> Result<Vec<WorkflowJobLinkTriggerCatalogRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT
            relation.relname::text AS table_name,
            trigger_row.tgname::text AS trigger_name,
            function_namespace.nspname::text AS function_schema,
            function_row.proname::text AS function_name,
            function_row.pronargs AS function_argument_count,
            function_row.prorettype = 'pg_catalog.trigger'::regtype AS returns_trigger,
            trigger_row.tgenabled::text AS enabled_mode,
            trigger_row.tgisinternal AS is_internal,
            trigger_row.tgtype AS trigger_type,
            ARRAY(
                SELECT attribute.attname::text
                FROM unnest(trigger_row.tgattr::smallint[]) WITH ORDINALITY
                    AS trigger_column(attnum, ordinal)
                JOIN pg_attribute AS attribute
                  ON attribute.attrelid = relation.oid
                 AND attribute.attnum = trigger_column.attnum
                 AND NOT attribute.attisdropped
                ORDER BY trigger_column.ordinal
            ) AS update_column_names,
            trigger_row.tgnargs AS trigger_argument_count,
            trigger_row.tgqual IS NOT NULL AS has_when_condition,
            trigger_row.tgconstraint <> 0 AS is_constraint,
            trigger_row.tgdeferrable AS is_deferrable,
            trigger_row.tginitdeferred AS is_initially_deferred
         FROM pg_namespace AS table_namespace
         JOIN pg_class AS relation
           ON relation.relnamespace = table_namespace.oid
          AND relation.relkind IN ('r', 'p')
         JOIN pg_trigger AS trigger_row
           ON trigger_row.tgrelid = relation.oid
         JOIN pg_proc AS function_row
           ON function_row.oid = trigger_row.tgfoid
         JOIN pg_namespace AS function_namespace
           ON function_namespace.oid = function_row.pronamespace
         WHERE table_namespace.nspname = 'public'
           AND (
                (relation.relname, trigger_row.tgname) = (
                    'job_queue',
                    'trg_job_queue_workflow_step_linkage_symmetry'
                )
                OR (relation.relname, trigger_row.tgname) = (
                    'workflow_steps',
                    'trg_workflow_steps_job_linkage_symmetry'
                )
                OR (relation.relname, trigger_row.tgname) = (
                    'workflow_steps',
                    'trg_workflow_steps_job_linkage_compatibility'
                )
           )",
    )
    .fetch_all(&mut **conn)
    .await
}

fn workflow_job_link_trigger_diagnostics(
    catalog: &[WorkflowJobLinkTriggerCatalogRow],
) -> Vec<WorkflowJobLinkTriggerDiagnostic> {
    WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS
        .iter()
        .filter_map(|spec| {
            let Some(trigger) = catalog.iter().find(|trigger| {
                trigger.table_name == spec.table_name && trigger.trigger_name == spec.trigger_name
            }) else {
                return Some(WorkflowJobLinkTriggerDiagnostic {
                    table_name: spec.table_name,
                    trigger_name: spec.trigger_name,
                    problems: vec![WorkflowJobLinkTriggerProblem::Missing],
                });
            };

            let problems = workflow_job_link_trigger_problems(spec, trigger);
            (!problems.is_empty()).then_some(WorkflowJobLinkTriggerDiagnostic {
                table_name: spec.table_name,
                trigger_name: spec.trigger_name,
                problems,
            })
        })
        .collect()
}

fn workflow_job_link_trigger_problems(
    spec: &WorkflowJobLinkTriggerSpec,
    trigger: &WorkflowJobLinkTriggerCatalogRow,
) -> Vec<WorkflowJobLinkTriggerProblem> {
    let mut problems = Vec::new();

    if trigger.function_schema != "public"
        || trigger.function_name != spec.function_name
        || trigger.function_argument_count != 0
        || !trigger.returns_trigger
    {
        problems.push(WorkflowJobLinkTriggerProblem::WrongFunction);
    }
    if !matches!(trigger.enabled_mode.as_str(), "O" | "A") {
        problems.push(WorkflowJobLinkTriggerProblem::NotEnabledForOriginWrites);
    }
    if trigger.is_internal {
        problems.push(WorkflowJobLinkTriggerProblem::InternallyGenerated);
    }
    if trigger.trigger_type != AFTER_ROW_INSERT_OR_UPDATE_TRIGGER_TYPE {
        problems.push(WorkflowJobLinkTriggerProblem::WrongFiringEvents);
    }
    if !trigger.update_column_names.is_empty()
        && !trigger
            .update_column_names
            .iter()
            .any(|column_name| column_name == spec.update_column_name)
    {
        problems.push(WorkflowJobLinkTriggerProblem::UpdateColumnNotCovered);
    }
    if trigger.trigger_argument_count != 0 {
        problems.push(WorkflowJobLinkTriggerProblem::UnexpectedTriggerArguments);
    }
    if trigger.has_when_condition {
        problems.push(WorkflowJobLinkTriggerProblem::UnexpectedWhenCondition);
    }

    let constraint_mode_is_valid = match spec.constraint_mode {
        WorkflowJobLinkTriggerConstraintMode::Deferred => {
            trigger.is_constraint && trigger.is_deferrable && trigger.is_initially_deferred
        }
        WorkflowJobLinkTriggerConstraintMode::Ordinary => {
            !trigger.is_constraint && !trigger.is_deferrable && !trigger.is_initially_deferred
        }
    };
    if !constraint_mode_is_valid {
        problems.push(WorkflowJobLinkTriggerProblem::WrongConstraintMode);
    }

    problems
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
            None => match recover_pending_concurrent_index(conn, migration.version).await? {
                ConcurrentIndexRecovery::ApplyMigration => {
                    (**conn).apply(migration).await?;
                }
                ConcurrentIndexRecovery::RecordExistingIndex => {
                    record_existing_migration(conn, migration).await?;
                }
            },
        }
    }

    Ok(())
}

async fn recover_pending_concurrent_index(
    conn: &mut PgPoolConnection,
    migration_version: i64,
) -> Result<ConcurrentIndexRecovery, MigrateError> {
    let Some(index) = RECOVERABLE_CONCURRENT_INDEXES
        .iter()
        .find(|index| index.migration_version == migration_version)
    else {
        return Ok(ConcurrentIndexRecovery::ApplyMigration);
    };

    // PostgreSQL deliberately leaves an invalid index relation behind when a
    // concurrent build fails. SQLx then records no-transaction migrations in a
    // separate statement, so a process can also disappear after PostgreSQL has
    // made the index valid but before `_sqlx_migrations` is updated. Model both
    // states explicitly: invalid expected-table indexes are discarded, while a
    // valid index is adopted only when its complete simple-B-tree shape matches
    // the pending migration. Same-named relations with any other shape remain a
    // hard error instead of being silently blessed by migration bookkeeping.
    let state = concurrent_index_state(conn, index.index_name, index.table_name).await?;

    if !state.relation_exists {
        return Ok(ConcurrentIndexRecovery::ApplyMigration);
    }

    let Some(is_valid) = state.is_valid else {
        return Err(concurrent_index_conflict(
            index,
            "the name belongs to another relation",
        ));
    };

    if is_valid {
        let expected_key_columns = index
            .key_columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>();
        let matches_expected_shape = !state.is_unique.unwrap_or(true)
            && state.has_only_key_columns.unwrap_or(false)
            && state.has_no_predicate.unwrap_or(false)
            && state.has_only_default_operator_classes.unwrap_or(false)
            && state.has_only_column_default_collations.unwrap_or(false)
            && state.access_method.as_deref() == Some("btree")
            && state.key_columns.as_deref() == Some(expected_key_columns.as_slice())
            && state.key_options.as_deref() == Some(index.key_options);
        if matches_expected_shape {
            tracing::warn!(
                migration_version,
                index_name = index.index_name,
                "recording valid index left by interrupted migration bookkeeping"
            );
            return Ok(ConcurrentIndexRecovery::RecordExistingIndex);
        }

        return Err(concurrent_index_conflict(
            index,
            &format!("the valid index does not match the pending migration ({state:?})"),
        ));
    }

    tracing::warn!(
        migration_version,
        index_name = index.index_name,
        "removing invalid index left by a failed concurrent migration"
    );
    sqlx::query(index.drop_sql).execute(&mut **conn).await?;

    Ok(ConcurrentIndexRecovery::ApplyMigration)
}

async fn concurrent_index_state(
    conn: &mut sqlx::PgConnection,
    index_name: &str,
    table_name: &str,
) -> Result<ConcurrentIndexState, sqlx::Error> {
    sqlx::query_as::<_, ConcurrentIndexState>(
        "WITH named_relation AS (
            SELECT to_regclass($1) AS relation_id
         )
         SELECT
            named_relation.relation_id IS NOT NULL AS relation_exists,
            index_state.indisvalid AS is_valid,
            index_state.indisunique AS is_unique,
            index_state.indnkeyatts = index_state.indnatts AS has_only_key_columns,
            index_state.indpred IS NULL AS has_no_predicate,
            NOT EXISTS (
                SELECT 1
                FROM unnest(index_state.indclass::oid[]) operator_class_id
                JOIN pg_opclass operator_class
                  ON operator_class.oid = operator_class_id
                WHERE NOT operator_class.opcdefault
            ) AS has_only_default_operator_classes,
            NOT EXISTS (
                SELECT 1
                FROM unnest(
                    index_state.indkey::smallint[],
                    index_state.indcollation::oid[]
                ) key_state(attribute_number, collation_id)
                LEFT JOIN pg_attribute attribute
                  ON attribute.attrelid = index_state.indrelid
                 AND attribute.attnum = key_state.attribute_number
                WHERE attribute.attnum IS NULL
                   OR key_state.collation_id IS DISTINCT FROM attribute.attcollation
            ) AS has_only_column_default_collations,
            access_method.amname AS access_method,
            CASE WHEN index_state.indexrelid IS NULL THEN NULL ELSE ARRAY(
                SELECT key_option
                FROM unnest(index_state.indoption::smallint[]) key_option
            ) END AS key_options,
            CASE WHEN index_state.indexrelid IS NULL THEN NULL ELSE ARRAY(
                SELECT pg_get_indexdef(
                    index_state.indexrelid,
                    key_position,
                    false
                )
                FROM generate_series(1, index_state.indnkeyatts::int) key_position
            ) END AS key_columns
         FROM named_relation
         LEFT JOIN pg_index index_state
           ON index_state.indexrelid = named_relation.relation_id
          AND index_state.indrelid = to_regclass($2)
         LEFT JOIN pg_class index_relation
           ON index_relation.oid = index_state.indexrelid
         LEFT JOIN pg_am access_method
           ON access_method.oid = index_relation.relam",
    )
    .bind(index_name)
    .bind(table_name)
    .fetch_one(conn)
    .await
}

fn concurrent_index_conflict(index: &RecoverableConcurrentIndex, detail: &str) -> MigrateError {
    MigrateError::ExecuteMigration(
        sqlx::Error::Protocol(format!(
            "cannot recover concurrent index {} on {}: {detail}",
            index.index_name, index.table_name
        )),
        index.migration_version,
    )
}

async fn record_existing_migration(
    conn: &mut PgPoolConnection,
    migration: &Migration,
) -> Result<(), MigrateError> {
    sqlx::query(
        "INSERT INTO _sqlx_migrations (
            version,
            description,
            success,
            checksum,
            execution_time
         )
         VALUES ($1, $2, TRUE, $3, 0)",
    )
    .bind(migration.version)
    .bind(migration.description.as_ref())
    .bind(migration.checksum.as_ref())
    .execute(&mut **conn)
    .await?;

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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    use super::*;

    #[test]
    fn migration_lock_backoff_grows_and_remains_bounded() {
        let mut backoff = MigrationLockBackoff::new(0x5eed);
        let expected_nominal_intervals = [100_u64, 200, 400, 800, 1_600, 1_600, 1_600];

        for expected_nominal_ms in expected_nominal_intervals {
            let delay = backoff.next_delay();
            let nominal = Duration::from_millis(expected_nominal_ms);
            assert!(delay >= nominal, "delay {delay:?} must include its base");
            assert!(
                delay <= nominal + nominal / 4,
                "delay {delay:?} must keep jitter within 25%"
            );
            assert!(delay <= MIGRATION_LOCK_RETRY_MAX_INTERVAL);
        }
    }

    #[tokio::test]
    async fn concurrent_index_state_compares_collation_with_the_indexed_column() {
        let (pool, database) = setup_ephemeral_pool("postgres_concurrent_index_collation", 1).await;
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version");
        eprintln!("concurrent index collation PostgreSQL server_version={server_version}");

        let mut conn = pool
            .acquire()
            .await
            .expect("acquire collation diagnostic connection");
        sqlx::query("CREATE TABLE collation_recovery_test (value text NOT NULL)")
            .execute(&mut *conn)
            .await
            .expect("create collation diagnostic table");
        sqlx::query(
            "CREATE INDEX collation_recovery_default_idx
             ON collation_recovery_test (value)",
        )
        .execute(&mut *conn)
        .await
        .expect("create default-collated index");
        sqlx::query(
            "CREATE INDEX collation_recovery_explicit_idx
             ON collation_recovery_test (value COLLATE \"C\")",
        )
        .execute(&mut *conn)
        .await
        .expect("create explicitly collated index");

        let default_state = concurrent_index_state(
            &mut conn,
            "collation_recovery_default_idx",
            "collation_recovery_test",
        )
        .await
        .expect("inspect default-collated index");
        assert_eq!(default_state.has_only_column_default_collations, Some(true));

        let explicit_state = concurrent_index_state(
            &mut conn,
            "collation_recovery_explicit_idx",
            "collation_recovery_test",
        )
        .await
        .expect("inspect explicitly collated index");
        assert_eq!(
            explicit_state.has_only_column_default_collations,
            Some(false)
        );

        drop(conn);
        teardown_ephemeral_pool(pool, database).await;
    }

    #[test]
    fn every_concurrent_index_migration_has_exact_recovery_metadata() {
        let concurrent_indexes = MIGRATOR
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
            .filter_map(|migration| {
                concurrent_index_target(migration.sql.as_ref())
                    .map(|(index_name, table_name)| (migration.version, (index_name, table_name)))
            })
            .collect::<BTreeMap<_, _>>();
        let recovery_versions = RECOVERABLE_CONCURRENT_INDEXES
            .iter()
            .map(|recovery| recovery.migration_version)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            recovery_versions.len(),
            RECOVERABLE_CONCURRENT_INDEXES.len(),
            "concurrent-index recovery entries must not duplicate migration versions"
        );
        assert_eq!(
            concurrent_indexes.keys().copied().collect::<BTreeSet<_>>(),
            recovery_versions,
            "every concurrent-index migration must have exactly one recovery entry"
        );
        for recovery in RECOVERABLE_CONCURRENT_INDEXES {
            let migration = MIGRATOR
                .iter()
                .find(|migration| {
                    migration.version == recovery.migration_version
                        && migration.migration_type.is_up_migration()
                })
                .unwrap_or_else(|| {
                    panic!(
                        "recovery metadata references missing concurrent-index migration {}",
                        recovery.migration_version
                    )
                });
            let statements = executable_sql_statements(migration.sql.as_ref());
            assert_eq!(
                statements.len(),
                1,
                "recoverable concurrent-index migration {} must contain exactly one executable \
                 statement because recovery records it without executing the migration SQL",
                recovery.migration_version
            );
            let target = concurrent_indexes
                .get(&recovery.migration_version)
                .unwrap_or_else(|| {
                    panic!(
                        "recovery metadata references missing concurrent-index migration {}",
                        recovery.migration_version
                    )
                });
            assert_eq!(target.0, recovery.index_name);
            assert_eq!(target.1, recovery.table_name);
            assert_eq!(
                recovery.drop_sql,
                format!("DROP INDEX CONCURRENTLY {}", recovery.index_name)
            );
        }
    }

    #[test]
    fn valid_catalog_rows_have_no_diagnostics() {
        let catalog = WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS
            .iter()
            .map(valid_catalog_row)
            .collect::<Vec<_>>();

        assert!(workflow_job_link_trigger_diagnostics(&catalog).is_empty());
    }

    #[test]
    fn missing_triggers_are_reported_individually() {
        let diagnostics = workflow_job_link_trigger_diagnostics(&[]);

        assert_eq!(
            diagnostics.len(),
            WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS.len()
        );
        for (diagnostic, spec) in diagnostics
            .iter()
            .zip(WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS)
        {
            assert_eq!(diagnostic.table_name(), spec.table_name);
            assert_eq!(diagnostic.trigger_name(), spec.trigger_name);
            assert_eq!(
                diagnostic.problems(),
                &[WorkflowJobLinkTriggerProblem::Missing]
            );
        }
    }

    #[test]
    fn executable_sql_statements_ignore_comments_and_empty_segments() {
        assert_eq!(
            executable_sql_statements(
                "-- no-transaction\n\nCREATE INDEX CONCURRENTLY idx_example ON example (id);\n-- end\n",
            ),
            ["CREATE INDEX CONCURRENTLY idx_example ON example (id)"]
        );
        assert_eq!(
            executable_sql_statements(
                "CREATE INDEX CONCURRENTLY idx_one ON example (id);\n\
                 CREATE INDEX CONCURRENTLY idx_two ON example (created_at);",
            ),
            [
                "CREATE INDEX CONCURRENTLY idx_one ON example (id)",
                "CREATE INDEX CONCURRENTLY idx_two ON example (created_at)",
            ]
        );
    }

    fn executable_sql_statements(sql: &str) -> Vec<String> {
        sql.lines()
            .map(|line| line.split_once("--").map_or(line, |(sql, _)| sql))
            .collect::<Vec<_>>()
            .join("\n")
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn concurrent_index_target(sql: &str) -> Option<(&str, &str)> {
        let tokens = sql.split_whitespace().collect::<Vec<_>>();
        let create_index = tokens
            .windows(3)
            .position(|tokens| tokens == ["CREATE", "INDEX", "CONCURRENTLY"])?;
        let index_name = tokens.get(create_index + 3)?;
        if tokens.get(create_index + 4) != Some(&"ON") {
            return None;
        }
        let table_name = tokens.get(create_index + 5)?;

        Some((
            index_name.trim_end_matches(';'),
            table_name.trim_end_matches(';'),
        ))
    }

    #[test]
    fn every_unsafe_catalog_property_has_a_typed_problem() {
        let spec = &WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS[0];
        let valid = valid_catalog_row(spec);

        let mut trigger = valid.clone();
        trigger.function_schema = "shadow".to_owned();
        assert_only_problem(spec, &trigger, WorkflowJobLinkTriggerProblem::WrongFunction);

        let mut trigger = valid.clone();
        trigger.enabled_mode = "R".to_owned();
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::NotEnabledForOriginWrites,
        );

        let mut trigger = valid.clone();
        trigger.is_internal = true;
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::InternallyGenerated,
        );

        let mut trigger = valid.clone();
        trigger.trigger_type = 20;
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::WrongFiringEvents,
        );

        let mut trigger = valid.clone();
        trigger.update_column_names = vec!["stage".to_owned()];
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::UpdateColumnNotCovered,
        );

        let mut trigger = valid.clone();
        trigger.trigger_argument_count = 1;
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::UnexpectedTriggerArguments,
        );

        let mut trigger = valid.clone();
        trigger.has_when_condition = true;
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::UnexpectedWhenCondition,
        );

        let mut trigger = valid;
        trigger.is_deferrable = false;
        trigger.is_initially_deferred = false;
        assert_only_problem(
            spec,
            &trigger,
            WorkflowJobLinkTriggerProblem::WrongConstraintMode,
        );
    }

    #[test]
    fn safe_update_column_supersets_and_always_enabled_mode_are_accepted() {
        let spec = &WORKFLOW_JOB_LINK_EXPAND_TRIGGER_SPECS[2];

        let mut all_updates = valid_catalog_row(spec);
        all_updates.update_column_names.clear();
        assert!(workflow_job_link_trigger_problems(spec, &all_updates).is_empty());

        let mut additional_columns = valid_catalog_row(spec);
        additional_columns
            .update_column_names
            .push("stage".to_owned());
        additional_columns.enabled_mode = "A".to_owned();
        assert!(workflow_job_link_trigger_problems(spec, &additional_columns).is_empty());
    }

    fn valid_catalog_row(spec: &WorkflowJobLinkTriggerSpec) -> WorkflowJobLinkTriggerCatalogRow {
        let (is_constraint, is_deferrable, is_initially_deferred) = match spec.constraint_mode {
            WorkflowJobLinkTriggerConstraintMode::Deferred => (true, true, true),
            WorkflowJobLinkTriggerConstraintMode::Ordinary => (false, false, false),
        };

        WorkflowJobLinkTriggerCatalogRow {
            table_name: spec.table_name.to_owned(),
            trigger_name: spec.trigger_name.to_owned(),
            function_schema: "public".to_owned(),
            function_name: spec.function_name.to_owned(),
            function_argument_count: 0,
            returns_trigger: true,
            enabled_mode: "O".to_owned(),
            is_internal: false,
            trigger_type: AFTER_ROW_INSERT_OR_UPDATE_TRIGGER_TYPE,
            update_column_names: vec![spec.update_column_name.to_owned()],
            trigger_argument_count: 0,
            has_when_condition: false,
            is_constraint,
            is_deferrable,
            is_initially_deferred,
        }
    }

    fn assert_only_problem(
        spec: &WorkflowJobLinkTriggerSpec,
        trigger: &WorkflowJobLinkTriggerCatalogRow,
        expected: WorkflowJobLinkTriggerProblem,
    ) {
        assert_eq!(
            workflow_job_link_trigger_problems(spec, trigger),
            vec![expected]
        );
    }
}
