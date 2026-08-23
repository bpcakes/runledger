use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use runledger_postgres::{
    MIGRATOR, SchemaCompatibilityError, ensure_schema_compatible_after_idempotency_cutover,
    migrate_after_idempotency_cutover,
};
use runledger_test_support::{
    EphemeralDatabase, acquire_test_db_connection_budget, setup_unmigrated_ephemeral_pool,
    teardown_ephemeral_pool,
};
use serde_json::{Value, json};
use sqlx::migrate::{Migrate, MigrateError, Migrator};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

const ENQUEUE_REQUEST_CUTOVER_VERSION: i64 = 202605220001;
const V0_6_LATEST_MIGRATION_VERSION: i64 = 202606030001;
const REPLAY_METRICS_MIGRATION_VERSION: i64 = 202607190001;
const CONTINUATION_METRICS_VALIDATION_MIGRATION_VERSION: i64 = 202607250001;
const WORKFLOW_CONTINUATION_MIGRATION_VERSION: i64 = 202607280001;
const WORKFLOW_ACTIVE_CLAIMS_MIGRATION_VERSION: i64 = 202607280002;
const HANDLER_RETRY_AUDIT_MIGRATION_VERSION: i64 = 202607280003;
const JOB_EXECUTION_RESOURCES_MIGRATION_VERSION: i64 = 202607280004;
const WORKFLOW_RECOVERIES_MIGRATION_VERSION: i64 = 202607280005;
const JOB_ENQUEUE_INTENTS_MIGRATION_VERSION: i64 = 202608180001;
const ADMIN_JOB_EVENTS_HISTORY_INDEX_MIGRATION_VERSION: i64 = 202608210001;
const ADMIN_JOB_LOGS_HISTORY_INDEX_MIGRATION_VERSION: i64 = 202608210002;
const ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION: i64 = 202608210003;
const ADMIN_JOBS_ORG_CREATED_INDEX_MIGRATION_VERSION: i64 = 202608210004;
const ADMIN_WORKFLOWS_CREATED_INDEX_MIGRATION_VERSION: i64 = 202608210005;
const ADMIN_WORKFLOWS_ORG_CREATED_INDEX_MIGRATION_VERSION: i64 = 202608210006;
const ADMIN_CONCURRENT_INDEXES: &[(i64, &str, &str, &str)] = &[
    (
        ADMIN_JOB_EVENTS_HISTORY_INDEX_MIGRATION_VERSION,
        "job_events",
        "idx_job_events_job_id_newest",
        "(job_id, id DESC)",
    ),
    (
        ADMIN_JOB_LOGS_HISTORY_INDEX_MIGRATION_VERSION,
        "job_logs",
        "idx_job_logs_job_id_newest",
        "(job_id, id DESC)",
    ),
    (
        ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION,
        "job_queue",
        "idx_job_queue_admin_created",
        "(created_at DESC, id DESC)",
    ),
    (
        ADMIN_JOBS_ORG_CREATED_INDEX_MIGRATION_VERSION,
        "job_queue",
        "idx_job_queue_admin_org_created",
        "(organization_id, created_at DESC, id DESC)",
    ),
    (
        ADMIN_WORKFLOWS_CREATED_INDEX_MIGRATION_VERSION,
        "workflow_runs",
        "idx_workflow_runs_admin_created",
        "(created_at DESC, id DESC)",
    ),
    (
        ADMIN_WORKFLOWS_ORG_CREATED_INDEX_MIGRATION_VERSION,
        "workflow_runs",
        "idx_workflow_runs_admin_org_created",
        "(organization_id, created_at DESC, id DESC)",
    ),
];
const ADMIN_JOBS_FOR_ORGANIZATION_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_job_summaries_for_organization.sql");
const ADMIN_JOBS_GLOBAL_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_job_summaries_global.sql");
const ADMIN_WORKFLOWS_FOR_ORGANIZATION_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_workflow_summaries_for_organization.sql");
const ADMIN_WORKFLOWS_GLOBAL_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_workflow_summaries_global.sql");
const WORKFLOW_STEPS_FOR_ORGANIZATION_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_workflow_steps_for_organization.sql");
const WORKFLOW_STEPS_GLOBAL_QUERY: &str =
    include_str!("../src/jobs/admin/queries/list_workflow_steps_global.sql");
const CONTINUATION_METRICS_CTE_MIGRATION_VERSION: i64 = 202608230001;
const COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS: &[i64] = &[
    // Adds replay lineage and a read-only metrics view without changing legacy writes.
    REPLAY_METRICS_MIGRATION_VERSION,
    // Replaces only the metrics view definition; queue storage is unchanged.
    CONTINUATION_METRICS_VALIDATION_MIGRATION_VERSION,
    // Adds opt-in columns and constraints whose defaults preserve legacy behavior.
    WORKFLOW_CONTINUATION_MIGRATION_VERSION,
    // Adds a claim table that is unused until the coordinated enqueue API is called.
    WORKFLOW_ACTIVE_CLAIMS_MIGRATION_VERSION,
    // Adds nullable retry-audit columns that legacy writers leave empty.
    HANDLER_RETRY_AUDIT_MIGRATION_VERSION,
    // Adds nullable resource columns; legacy leasing remains compatible until
    // an application opts a row into resource coordination.
    JOB_EXECUTION_RESOURCES_MIGRATION_VERSION,
    // Adds recovery lineage and snapshots that legacy writers never invoke.
    WORKFLOW_RECOVERIES_MIGRATION_VERSION,
    // Adds an opt-in intent table that older workers and non-retention writers
    // ignore. Promoted rows deliberately fence linked-job deletion, so rollout
    // must order application retention as documented by the public API.
    JOB_ENQUEUE_INTENTS_MIGRATION_VERSION,
    // Adds read-only admin pagination indexes without changing legacy writes.
    ADMIN_JOB_EVENTS_HISTORY_INDEX_MIGRATION_VERSION,
    ADMIN_JOB_LOGS_HISTORY_INDEX_MIGRATION_VERSION,
    ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION,
    ADMIN_JOBS_ORG_CREATED_INDEX_MIGRATION_VERSION,
    ADMIN_WORKFLOWS_CREATED_INDEX_MIGRATION_VERSION,
    ADMIN_WORKFLOWS_ORG_CREATED_INDEX_MIGRATION_VERSION,
    // Refactors only the continuation metrics view query; its columns and
    // result semantics are unchanged.
    CONTINUATION_METRICS_CTE_MIGRATION_VERSION,
];
const TEST_HARNESS_POOL_CONNECTIONS: u32 = 4;

struct TestHarness {
    pool: PgPool,
    database: EphemeralDatabase,
}

impl TestHarness {
    async fn fresh(prefix: &str) -> Self {
        let (pool, database) =
            setup_unmigrated_ephemeral_pool(prefix, TEST_HARNESS_POOL_CONNECTIONS).await;
        Self { pool, database }
    }

    async fn teardown(self) {
        teardown_ephemeral_pool(self.pool, self.database).await;
    }
}

#[tokio::test]
async fn migrate_applies_bundled_schema_to_fresh_database() {
    let harness = TestHarness::fresh("runledger_pg_migrate").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for bundled schema regression");
    eprintln!("bundled schema regression PostgreSQL server_version={server_version}");

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("repeat migrate after constraints are validated");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("schema should validate after migrate");
    assert!(
        idempotency_cutover_constraints_valid(&harness.pool).await,
        "migrate should validate idempotency cutover constraints after legacy check passes"
    );

    let migrations_row_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&harness.pool)
            .await
            .expect("count applied migrations");
    assert_eq!(
        migrations_row_count,
        runledger_migration_versions().len() as i64
    );

    let recorded_runledger_versions = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM runledger_migration_history ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("list recorded runledger migrations");
    assert_eq!(
        recorded_runledger_versions,
        expected_compatibility_fence_versions()
    );

    let metrics_view_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.views
             WHERE table_schema = 'public'
               AND table_name = 'job_metrics_rollup'
         )",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("query metrics view");
    assert!(metrics_view_exists);

    let continuation_metrics_view_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.views
             WHERE table_schema = 'public'
               AND table_name = 'job_continuation_metrics_rollup'
         )",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("query continuation metrics view");
    assert!(continuation_metrics_view_exists);
    for (index_name, expected_columns) in [
        ("idx_job_events_job_id_newest", "(job_id, id DESC)"),
        ("idx_job_logs_job_id_newest", "(job_id, id DESC)"),
    ] {
        let index_definition = sqlx::query_scalar::<_, String>(&format!(
            "SELECT pg_get_indexdef('{index_name}'::regclass)"
        ))
        .fetch_one(&harness.pool)
        .await
        .unwrap_or_else(|error| panic!("read {index_name}: {error}"));
        assert!(
            index_definition.contains(expected_columns),
            "{index_name} must support bounded newest-first history scans: {index_definition}"
        );
    }
    for (index_name, expected_columns) in [
        ("idx_job_queue_admin_created", "(created_at DESC, id DESC)"),
        (
            "idx_job_queue_admin_org_created",
            "(organization_id, created_at DESC, id DESC)",
        ),
        (
            "idx_workflow_runs_admin_created",
            "(created_at DESC, id DESC)",
        ),
        (
            "idx_workflow_runs_admin_org_created",
            "(organization_id, created_at DESC, id DESC)",
        ),
    ] {
        let index_definition = sqlx::query_scalar::<_, String>(&format!(
            "SELECT pg_get_indexdef('{index_name}'::regclass)"
        ))
        .fetch_one(&harness.pool)
        .await
        .unwrap_or_else(|error| panic!("read {index_name}: {error}"));
        assert!(
            index_definition.contains(expected_columns),
            "{index_name} must support bounded admin list scans: {index_definition}"
        );
    }
    let mut plan_tx = harness
        .pool
        .begin()
        .await
        .expect("begin history plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *plan_tx)
        .await
        .expect("prefer indexes for deterministic history plan check");
    for (table_name, index_name) in [
        ("job_events", "idx_job_events_job_id_newest"),
        ("job_logs", "idx_job_logs_job_id_newest"),
    ] {
        let plan = sqlx::query_scalar::<_, String>(&format!(
            "EXPLAIN (COSTS OFF) \
             SELECT history.id \
             FROM {table_name} history \
             JOIN job_queue job ON job.id = history.job_id \
             WHERE history.job_id = '00000000-0000-4000-8000-000000000001'::uuid \
               AND history.id < 9223372036854775807 \
             ORDER BY history.id DESC \
             LIMIT 51"
        ))
        .fetch_all(&mut *plan_tx)
        .await
        .unwrap_or_else(|error| panic!("explain {table_name} newest-first scan: {error}"))
        .join("\n");
        assert!(
            plan.contains(index_name),
            "PostgreSQL 18 must plan {table_name} newest-first pagination with {index_name}:\n{plan}"
        );
    }
    for (query_name, query, index_name) in [
        (
            "global job list",
            ADMIN_JOBS_GLOBAL_QUERY,
            "idx_job_queue_admin_created",
        ),
        (
            "organization job list",
            ADMIN_JOBS_FOR_ORGANIZATION_QUERY,
            "idx_job_queue_admin_org_created",
        ),
        (
            "global workflow list",
            ADMIN_WORKFLOWS_GLOBAL_QUERY,
            "idx_workflow_runs_admin_created",
        ),
        (
            "organization workflow list",
            ADMIN_WORKFLOWS_FOR_ORGANIZATION_QUERY,
            "idx_workflow_runs_admin_org_created",
        ),
    ] {
        let explain = format!("EXPLAIN (GENERIC_PLAN, COSTS OFF) {query}");
        let plan = sqlx::raw_sql(&explain)
            .fetch_all(&mut *plan_tx)
            .await
            .unwrap_or_else(|error| panic!("explain {query_name}: {error}"))
            .into_iter()
            .map(|row| {
                row.try_get::<String, _>(0)
                    .unwrap_or_else(|error| panic!("decode {query_name} plan row: {error}"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains(index_name),
            "PostgreSQL 18 must generically plan {query_name} with {index_name}:\n{plan}"
        );
    }
    for (query_name, query, expected_relation) in [
        (
            "global workflow step list",
            WORKFLOW_STEPS_GLOBAL_QUERY,
            None,
        ),
        (
            "organization workflow step list",
            WORKFLOW_STEPS_FOR_ORGANIZATION_QUERY,
            Some("workflow_step_dependencies"),
        ),
    ] {
        let explain = format!("EXPLAIN (GENERIC_PLAN, COSTS OFF) {query}");
        let plan = sqlx::raw_sql(&explain)
            .fetch_all(&mut *plan_tx)
            .await
            .unwrap_or_else(|error| panic!("explain {query_name}: {error}"))
            .into_iter()
            .map(|row| {
                row.try_get::<String, _>(0)
                    .unwrap_or_else(|error| panic!("decode {query_name} plan row: {error}"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        match expected_relation {
            Some(relation) => assert!(
                plan.contains(relation),
                "PostgreSQL 18 must retain scoped dependency aggregation in {query_name}:\n{plan}"
            ),
            None => assert!(
                !plan.contains("workflow_step_dependencies"),
                "service-wide workflow steps must not aggregate scoped dependencies:\n{plan}"
            ),
        }
    }
    plan_tx
        .rollback()
        .await
        .expect("rollback history plan check");
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('job_replays')::text")
            .fetch_one(&harness.pool)
            .await
            .expect("query job replay table"),
        Some("job_replays".to_owned())
    );

    let resource_claim_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_queue_execution_resource_claim_order'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read execution-resource claim-order index");
    assert!(
        resource_claim_index.contains(
            "(priority DESC, next_run_at, created_at, id) \
             INCLUDE (execution_resource_key, job_type)"
        ),
        "resource-head lookup must use a bounded queue-order index scan: {resource_claim_index}"
    );

    let active_cleanup_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_workflow_active_claims_release_pending'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read active-claim cleanup index");
    assert!(active_cleanup_index.contains("WHERE release_pending"));
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'trg_workflow_runs_mark_active_claim_release_pending'
                  AND NOT tgisinternal
             )",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read terminal active-claim trigger")
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'trg_job_queue_enforce_execution_resource_claim'
                  AND NOT tgisinternal
             )",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read execution-resource lease enforcement trigger")
    );

    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('job_enqueue_intents')::text")
            .fetch_one(&harness.pool)
            .await
            .expect("query job enqueue intents table"),
        Some("job_enqueue_intents".to_owned())
    );
    let pending_intent_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_pending'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read pending intent index");
    assert!(pending_intent_index.contains("(next_promotion_at, created_at, id)"));
    assert!(!pending_intent_index.contains("INCLUDE"));
    assert!(pending_intent_index.contains("WHERE (status = 'PENDING'::text)"));
    let pending_intent_type_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_pending_type'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read pending intent type index");
    assert!(pending_intent_type_index.contains("(job_type, next_promotion_at, created_at, id)"));
    assert!(!pending_intent_type_index.contains("INCLUDE"));
    assert!(pending_intent_type_index.contains("WHERE (status = 'PENDING'::text)"));
    let promoted_job_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_promoted_job'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read promoted intent job index");
    assert!(promoted_job_index.contains("(promoted_job_id)"));
    assert!(promoted_job_index.contains("WHERE (promoted_job_id IS NOT NULL)"));
    let pending_metrics_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_pending_metrics'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read pending intent metrics index");
    assert!(pending_metrics_index.contains("(job_type, created_at)"));
    assert!(pending_metrics_index.contains("INCLUDE (promotion_attempts)"));
    assert!(pending_metrics_index.contains("WHERE (status = 'PENDING'::text)"));
    let conflicted_metrics_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_conflicted_metrics'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read conflicted intent metrics index");
    assert!(conflicted_metrics_index.contains("(conflicted_at, job_type)"));
    assert!(conflicted_metrics_index.contains("WHERE (status = 'CONFLICTED'::text)"));
    let org_conflicted_metrics_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_org_conflicted_metrics'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read organization conflicted intent metrics index");
    assert!(org_conflicted_metrics_index.contains("(organization_id, conflicted_at, job_type)"));
    assert!(org_conflicted_metrics_index.contains("WHERE ((status = 'CONFLICTED'::text)"));
    let org_promoted_metrics_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_org_promoted_metrics'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read organization promoted intent metrics index");
    assert!(org_promoted_metrics_index.contains("(organization_id, promoted_at, job_type)"));
    assert!(org_promoted_metrics_index.contains("WHERE ((status = 'PROMOTED'::text)"));
    let promoted_cleanup_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_promoted_cleanup'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read promoted intent cleanup index");
    assert!(promoted_cleanup_index.contains("(promoted_at, id)"));
    assert!(promoted_cleanup_index.contains("INCLUDE (job_type, organization_id)"));
    assert!(promoted_cleanup_index.contains("WHERE (status = 'PROMOTED'::text)"));
    let global_created_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_created'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read global intent listing index");
    assert!(global_created_index.contains("(created_at DESC, id DESC)"));
    assert!(!global_created_index.contains("INCLUDE"));
    let organization_created_index = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_enqueue_intents_org_created'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read organization intent listing index");
    assert!(organization_created_index.contains("(organization_id, created_at DESC, id DESC)"));
    assert!(!organization_created_index.contains("INCLUDE"));
    for index_name in [
        "uq_job_enqueue_intents_type_idempotency_org",
        "uq_job_enqueue_intents_type_idempotency_global",
        "idx_job_enqueue_intents_org_pending_metrics",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(index_name)
                .fetch_one(&harness.pool)
                .await
                .expect("query enqueue intent index"),
            Some(index_name.to_owned())
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT data_type
             FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'job_enqueue_intents'
               AND column_name = 'enqueue_request_version'",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read enqueue request version type"),
        "smallint"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'job_enqueue_intents'
               AND (
                    (column_name = 'promotion_attempts' AND data_type = 'integer')
                 OR (column_name = 'next_promotion_at' AND data_type = 'timestamp with time zone')
                 OR (column_name = 'last_attempted_at' AND data_type = 'timestamp with time zone')
               )",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read enqueue intent retry metadata columns"),
        3
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'chk_job_enqueue_intents_promotion_attempts'
                  AND conrelid = 'job_enqueue_intents'::regclass
             )",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read enqueue intent promotion-attempt constraint")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT confdeltype::text
             FROM pg_constraint
             WHERE conname = 'fk_job_enqueue_intents_promoted_job'
               AND conrelid = 'job_enqueue_intents'::regclass",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read promoted-job foreign-key delete action"),
        "r"
    );
    let resource_constraint = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conname = 'chk_job_enqueue_intents_execution_resource_key'
           AND conrelid = 'job_enqueue_intents'::regclass",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read enqueue intent execution-resource constraint");
    assert!(resource_constraint.contains("octet_length(execution_resource_key) <= 512"));
    assert!(resource_constraint.contains("~"));
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'chk_job_enqueue_intents_state_fields'
                  AND conrelid = 'job_enqueue_intents'::regclass
             )",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("read enqueue intent state constraint")
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_recovers_invalid_indexes_left_by_failed_concurrent_builds() {
    for &(migration_version, table_name, index_name, expected_columns) in ADMIN_CONCURRENT_INDEXES {
        let harness = TestHarness::fresh("runledger_pg_concurrent_index_recovery").await;
        let server_version_num = sqlx::query_scalar::<_, String>("SHOW server_version_num")
            .fetch_one(&harness.pool)
            .await
            .expect("read PostgreSQL version for concurrent index recovery");
        eprintln!("concurrent index recovery PostgreSQL server_version_num={server_version_num}");

        apply_runledger_migrations_through(&harness.pool, migration_version - 1).await;
        let duplicate_column = match table_name {
            "job_events" | "job_logs" => {
                seed_legacy_job_definition(&harness.pool).await;
                let job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
                    "INSERT INTO job_queue (
                        job_type, payload, max_attempts, timeout_seconds
                     )
                     VALUES ('jobs.test.legacy_cutover', '{}'::jsonb, 3, 30)
                     RETURNING id",
                )
                .fetch_one(&harness.pool)
                .await
                .expect("insert job for concurrent index failure");
                let seed_history_sql = if table_name == "job_events" {
                    "INSERT INTO job_events (job_id, event_type)
                     VALUES ($1, 'ENQUEUED'), ($1, 'ENQUEUED')"
                } else {
                    "INSERT INTO job_logs (job_id, level, message)
                     VALUES ($1, 'INFO', 'first'), ($1, 'INFO', 'second')"
                };
                sqlx::query(seed_history_sql)
                    .bind(job_id)
                    .execute(&harness.pool)
                    .await
                    .unwrap_or_else(|error| panic!("seed duplicate {table_name} rows: {error}"));
                "job_id"
            }
            "job_queue" => {
                seed_legacy_job_definition(&harness.pool).await;
                sqlx::query(
                    "INSERT INTO job_queue (
                        job_type, payload, max_attempts, timeout_seconds
                     )
                     VALUES
                        ('jobs.test.legacy_cutover', '{}'::jsonb, 3, 30),
                        ('jobs.test.legacy_cutover', '{}'::jsonb, 3, 30)",
                )
                .execute(&harness.pool)
                .await
                .expect("seed duplicate job queue rows");
                "job_type"
            }
            "workflow_runs" => {
                sqlx::query(
                    "INSERT INTO workflow_runs (workflow_type, metadata)
                     VALUES
                        ('workflow.test.concurrent_index', '{}'::jsonb),
                        ('workflow.test.concurrent_index', '{}'::jsonb)",
                )
                .execute(&harness.pool)
                .await
                .expect("seed duplicate workflow run rows");
                "workflow_type"
            }
            other => panic!("unexpected concurrent index recovery table {other}"),
        };

        let failed_build = sqlx::query(&format!(
            "CREATE UNIQUE INDEX CONCURRENTLY {index_name} ON {table_name} ({duplicate_column})"
        ))
        .execute(&harness.pool)
        .await;
        assert!(
            failed_build.is_err(),
            "duplicate job ids must make the diagnostic unique index build fail"
        );
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT index_state.indisvalid
                 FROM pg_index index_state
                 WHERE index_state.indexrelid = to_regclass($1)",
            )
            .bind(index_name)
            .fetch_one(&harness.pool)
            .await
            .unwrap_or_else(|error| panic!("inspect failed index {index_name}: {error}")),
            "PostgreSQL 18 must retain the failed concurrent build as an invalid index"
        );

        migrate_after_idempotency_cutover(&harness.pool)
            .await
            .unwrap_or_else(|error| panic!("recover failed build for {index_name}: {error}"));

        let recovered_definition = sqlx::query_scalar::<_, String>(
            "SELECT pg_get_indexdef(index_state.indexrelid)
             FROM pg_index index_state
             WHERE index_state.indexrelid = to_regclass($1)
               AND index_state.indisvalid",
        )
        .bind(index_name)
        .fetch_one(&harness.pool)
        .await
        .unwrap_or_else(|error| panic!("read recovered index {index_name}: {error}"));
        assert!(
            recovered_definition.contains(expected_columns),
            "migration must replace the invalid relation with its canonical index: {recovered_definition}"
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM _sqlx_migrations
                    WHERE version = $1 AND success
                 )",
            )
            .bind(migration_version)
            .fetch_one(&harness.pool)
            .await
            .expect("read recovered migration history"),
            "recovered concurrent migration must be recorded as applied"
        );

        harness.teardown().await;
    }
}

#[tokio::test]
async fn migrate_records_valid_indexes_left_by_interrupted_bookkeeping() {
    for &(migration_version, _table_name, index_name, expected_columns) in ADMIN_CONCURRENT_INDEXES
    {
        let harness = TestHarness::fresh("runledger_pg_concurrent_index_adoption").await;
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&harness.pool)
            .await
            .expect("read PostgreSQL version for concurrent index adoption");
        eprintln!("concurrent index adoption PostgreSQL server_version={server_version}");

        apply_runledger_migrations_through(&harness.pool, migration_version - 1).await;
        execute_migration_sql_without_history(&harness.pool, migration_version).await;

        let index_oid_before = sqlx::query_scalar::<_, i32>(
            "SELECT index_state.indexrelid::int4
             FROM pg_index index_state
             WHERE index_state.indexrelid = to_regclass($1)
               AND index_state.indisvalid",
        )
        .bind(index_name)
        .fetch_one(&harness.pool)
        .await
        .unwrap_or_else(|error| panic!("read valid unrecorded index {index_name}: {error}"));
        assert!(
            !migration_history_contains(&harness.pool, migration_version).await,
            "diagnostic setup must leave {migration_version} unrecorded"
        );

        migrate_after_idempotency_cutover(&harness.pool)
            .await
            .unwrap_or_else(|error| panic!("adopt valid index {index_name}: {error}"));

        let index_oid_after = sqlx::query_scalar::<_, i32>(
            "SELECT index_state.indexrelid::int4
             FROM pg_index index_state
             WHERE index_state.indexrelid = to_regclass($1)
               AND index_state.indisvalid",
        )
        .bind(index_name)
        .fetch_one(&harness.pool)
        .await
        .unwrap_or_else(|error| panic!("read adopted index {index_name}: {error}"));
        assert_eq!(
            index_oid_after, index_oid_before,
            "recovery must record rather than rebuild the valid index"
        );
        let definition = sqlx::query_scalar::<_, String>("SELECT pg_get_indexdef($1::oid)")
            .bind(index_oid_after)
            .fetch_one(&harness.pool)
            .await
            .unwrap_or_else(|error| panic!("read adopted index definition {index_name}: {error}"));
        assert!(definition.contains(expected_columns));
        assert!(migration_history_contains(&harness.pool, migration_version).await);

        harness.teardown().await;
    }
}

#[tokio::test]
async fn migrate_rejects_a_valid_same_named_index_with_the_wrong_shape() {
    let harness = TestHarness::fresh("runledger_pg_concurrent_index_conflict").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read PostgreSQL version for concurrent index conflict");
    eprintln!("concurrent index conflict PostgreSQL server_version={server_version}");

    apply_runledger_migrations_through(
        &harness.pool,
        ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION - 1,
    )
    .await;
    sqlx::query(
        "CREATE INDEX CONCURRENTLY idx_job_queue_admin_created
         ON job_queue (job_type)",
    )
    .execute(&harness.pool)
    .await
    .expect("create conflicting valid index");

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("a mismatched valid index must remain a hard migration error");
    assert!(
        error
            .to_string()
            .contains("valid index does not match the pending migration"),
        "unexpected migration error: {error}"
    );
    assert!(
        !migration_history_contains(&harness.pool, ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION)
            .await
    );
    let definition = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_indexdef('idx_job_queue_admin_created'::regclass)",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("read preserved conflicting index");
    assert!(definition.contains("(job_type)"));

    harness.teardown().await;
}

#[tokio::test]
async fn replay_metrics_upgrade_preserves_data_and_exposes_raw_v0_6_rollback_boundary() {
    let harness = TestHarness::fresh("runledger_pg_replay_metrics_upgrade").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for migration compatibility regression");
    eprintln!("migration compatibility regression PostgreSQL server_version={server_version}");
    apply_runledger_migrations_through(&harness.pool, V0_6_LATEST_MIGRATION_VERSION).await;

    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ('jobs.test.preexisting_continuation', 1, 3, 30, 100, true)",
    )
    .execute(&harness.pool)
    .await
    .expect("insert preexisting continuation definition");
    let job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type,
            payload,
            status,
            run_number,
            max_attempts,
            timeout_seconds,
            next_run_at
         )
         VALUES (
            'jobs.test.preexisting_continuation',
            '{}'::jsonb,
            'PENDING',
            2,
            3,
            30,
            clock_timestamp() + interval '1 hour'
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert preexisting continued job");
    sqlx::query(
        "INSERT INTO job_events (
            job_id,
            run_number,
            event_type,
            payload
         )
         VALUES (
            $1,
            1,
            'REQUEUED',
            jsonb_build_object(
                'reason', 'HANDLER_CONTINUATION',
                'next_run_number', 2,
                'next_run_at', '2026-07-19T12:00:00Z',
                'delay_microseconds', 1000000
            )
         )",
    )
    .bind(job_id)
    .execute(&harness.pool)
    .await
    .expect("insert preexisting continuation event");

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("upgrade v0.6 schema to replay and metrics migration");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("latest schema guard accepts upgraded database");

    let metrics = sqlx::query_as::<_, (i64, i64, i32)>(
        "SELECT continued_24h, active_continued_count, max_active_run_number
         FROM job_continuation_metrics_rollup
         WHERE organization_id IS NULL
           AND job_type = 'jobs.test.preexisting_continuation'",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("load upgraded continuation metrics");
    assert_eq!(metrics, (1, 1, 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&harness.pool)
            .await
            .expect("count preserved preexisting job"),
        1
    );

    let recorded_runledger_versions = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM runledger_migration_history ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("list compatibility-fence migration history");
    assert_eq!(
        recorded_runledger_versions,
        expected_compatibility_fence_versions(),
        "additive migration must not make released v0.6.0 guards reject the schema"
    );

    let sqlx_history = sqlx::query_as::<_, (i64, Vec<u8>, bool)>(
        "SELECT version, checksum, success
         FROM _sqlx_migrations
         ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("load SQLx history after additive upgrade");
    assert!(
        sqlx_history
            .iter()
            .any(|(version, _, success)| *version == REPLAY_METRICS_MIGRATION_VERSION && *success)
    );
    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version <= V0_6_LATEST_MIGRATION_VERSION)
    {
        let (_, checksum, success) = sqlx_history
            .iter()
            .find(|(version, _, _)| *version == migration.version)
            .unwrap_or_else(|| panic!("missing v0.6 migration {}", migration.version));
        assert!(*success, "v0.6 migration {} is dirty", migration.version);
        assert_eq!(
            checksum.as_slice(),
            migration.checksum.as_ref(),
            "v0.6 migration {} checksum changed",
            migration.version
        );
    }

    let v0_6_migrator = raw_v0_6_migrator();
    let mut raw_connection = harness
        .pool
        .acquire()
        .await
        .expect("acquire connection for raw v0.6 migrator");
    let raw_error = v0_6_migrator
        .run(&mut *raw_connection)
        .await
        .expect_err("raw v0.6 SQLx migrator must reject newer applied history");
    assert!(
        matches!(
            &raw_error,
            MigrateError::VersionMissing(version)
                if *version == REPLAY_METRICS_MIGRATION_VERSION
        ),
        "unexpected raw v0.6 migration error: {raw_error}"
    );
    // SQLx's raw Migrator returns before unlocking on this validation error.
    // Discard the session so the advisory lock cannot wedge later startup.
    raw_connection
        .close()
        .await
        .expect("close raw v0.6 migration connection after VersionMissing");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("Runledger's filtered startup guard accepts the additive migration");

    MIGRATOR
        .undo(&harness.pool, V0_6_LATEST_MIGRATION_VERSION)
        .await
        .expect("current migrator can revert the post-v0.6 additive migration");
    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reverted object {object}: {error}")),
            None,
            "{object} must be absent after reverting to the raw v0.6 migration set"
        );
    }

    v0_6_migrator
        .run(&harness.pool)
        .await
        .expect("raw v0.6 migrator runs after newer history is reverted");
    let v0_6_versions = v0_6_migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&harness.pool)
            .await
            .expect("list raw v0.6 SQLx history"),
        v0_6_versions
    );

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("current migrator reapplies the additive migration after rollback");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("current guard accepts the reapplied schema");
    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reapplied object {object}: {error}")),
            Some(object.to_owned()),
            "{object} must be restored after reapplying current migrations"
        );
    }
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i32)>(
            "SELECT continued_24h, active_continued_count, max_active_run_number
             FROM job_continuation_metrics_rollup
             WHERE organization_id IS NULL
               AND job_type = 'jobs.test.preexisting_continuation'",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("load continuation metrics after down/up cycle"),
        (1, 1, 2)
    );

    harness.teardown().await;
}

#[tokio::test]
async fn raw_version_missing_strands_session_lock_until_pool_close_but_safe_path_unlocks() {
    let harness = TestHarness::fresh("runledger_pg_raw_migration_lock").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for migration lock regression");
    eprintln!("migration lock regression PostgreSQL server_version={server_version}");

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply current migrations before raw compatibility check");

    let _migration_connection_budget = acquire_test_db_connection_budget(2).await;
    let database_url = harness.database.url.clone();
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect disposable raw migration pool");
    let raw_backend_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&raw_pool)
        .await
        .expect("read raw migration backend pid");

    let raw_error = raw_v0_7_migrator()
        .run(&raw_pool)
        .await
        .expect_err("raw v0.7 migrator must reject newer applied SQLx history");
    assert!(
        matches!(
            &raw_error,
            MigrateError::VersionMissing(version)
                if *version == CONTINUATION_METRICS_VALIDATION_MIGRATION_VERSION
        ),
        "unexpected raw v0.7 migration error: {raw_error}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&raw_pool)
            .await
            .expect("reacquire raw migration session after VersionMissing"),
        raw_backend_pid,
        "single-connection raw pool must retain the session that failed validation"
    );
    assert_eq!(
        advisory_lock_count_for_backend(&harness.pool, raw_backend_pid).await,
        1,
        "raw SQLx VersionMissing must leave its session migration lock held"
    );

    let contender_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect disposable contender migration pool");
    let contender_result =
        tokio::time::timeout(Duration::from_millis(500), MIGRATOR.run(&contender_pool)).await;
    assert!(
        contender_result.is_err(),
        "a second migration session must block on the raw session's stranded advisory lock"
    );
    contender_pool.close().await;

    raw_pool.close().await;
    assert_eq!(
        advisory_lock_count_for_backend(&harness.pool, raw_backend_pid).await,
        0,
        "closing the disposable raw pool must release the stranded session lock"
    );

    let safe_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect fresh safe migration pool");
    migrate_after_idempotency_cutover(&safe_pool)
        .await
        .expect("safe Runledger migrator acquires and releases after raw pool closure");
    assert_eq!(
        advisory_lock_count_for_database(&harness.pool).await,
        0,
        "successful safe Runledger migration must release its session lock"
    );

    let newer_fence_version = CONTINUATION_METRICS_VALIDATION_MIGRATION_VERSION + 1;
    seed_runledger_migration_history(&harness.pool, newer_fence_version).await;
    let safe_error = migrate_after_idempotency_cutover(&safe_pool)
        .await
        .expect_err("safe Runledger migration path must reject newer compatibility history");
    assert!(
        matches!(
            &safe_error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if *version == newer_fence_version
        ),
        "unexpected safe migration error: {safe_error}"
    );
    assert_eq!(
        advisory_lock_count_for_database(&harness.pool).await,
        0,
        "safe Runledger migration errors must explicitly release the session lock"
    );
    safe_pool.close().await;

    harness.teardown().await;
}

#[tokio::test]
async fn safe_migrator_does_not_deadlock_concurrent_index_migrations_while_waiting() {
    let harness = TestHarness::fresh("runledger_pg_concurrent_migration_lock").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for concurrent migration lock regression");
    eprintln!("concurrent migration lock regression PostgreSQL server_version={server_version}");

    apply_runledger_migrations_through(
        &harness.pool,
        ADMIN_JOB_LOGS_HISTORY_INDEX_MIGRATION_VERSION,
    )
    .await;

    let _migration_connection_budget = acquire_test_db_connection_budget(2).await;
    let database_url = harness.database.url.clone();
    let holder_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect migration lock holder pool");
    let mut holder = holder_pool
        .acquire()
        .await
        .expect("acquire migration lock holder connection");
    (*holder)
        .lock()
        .await
        .expect("acquire raw SQLx migration lock");

    let safe_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect waiting safe migration pool");
    let waiting_safe_pool = safe_pool.clone();
    let safe_migration =
        tokio::spawn(async move { migrate_after_idempotency_cutover(&waiting_safe_pool).await });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !safe_migration.is_finished(),
        "safe migrator must coordinate with the raw SQLx migration lock"
    );

    let concurrent_index_migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version == ADMIN_JOBS_CREATED_INDEX_MIGRATION_VERSION
        })
        .expect("admin jobs created-at concurrent index migration exists");
    tokio::time::timeout(
        Duration::from_secs(5),
        (*holder).apply(concurrent_index_migration),
    )
    .await
    .expect("concurrent index migration must not deadlock with the waiting safe migrator")
    .expect("apply concurrent index migration while holding the migration lock");

    (*holder)
        .unlock()
        .await
        .expect("release raw SQLx migration lock");
    drop(holder);
    holder_pool.close().await;

    tokio::time::timeout(Duration::from_secs(10), safe_migration)
        .await
        .expect("safe migrator must acquire the released lock")
        .expect("safe migration task must not panic")
        .expect("safe migrator must apply remaining migrations");
    assert_eq!(
        advisory_lock_count_for_database(&harness.pool).await,
        0,
        "safe migration completion must release the migration lock"
    );
    safe_pool.close().await;

    harness.teardown().await;
}

#[tokio::test]
async fn continuation_metrics_require_well_typed_v0_6_or_v0_7_payloads() {
    const JOB_TYPE: &str = "jobs.test.continuation_payload_validation";

    let harness = TestHarness::fresh("runledger_pg_continuation_payloads").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for continuation metrics regression");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(&harness.pool)
            .await
            .expect("read PostgreSQL numeric version for continuation metrics regression");
    eprintln!(
        "continuation metrics regression PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
    );

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply continuation metrics validation migration");
    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, 1, 3, 30, 100, true)",
    )
    .bind(JOB_TYPE)
    .execute(&harness.pool)
    .await
    .expect("insert continuation payload validation job definition");

    sqlx::query(
        r#"
WITH canonical(event_payload) AS (
    SELECT jsonb_build_object(
        'reason', 'HANDLER_CONTINUATION',
        'requeue_kind', 'HANDLER_CONTINUATION',
        'next_run_number', 2,
        'next_run_at', clock_timestamp() + interval '1 hour',
        'delay_microseconds', 1000
    )
),
payload_cases(case_name, current_run_number, event_payload) AS (
    SELECT cases.*
    FROM canonical
    CROSS JOIN LATERAL (
        VALUES
            ('v0_6_kindless', 2, canonical.event_payload - 'requeue_kind'),
            ('v0_7_discriminated', 2, canonical.event_payload),
            ('missing_reason', 2, canonical.event_payload - 'reason'),
            (
                'null_reason',
                2,
                canonical.event_payload
                    || jsonb_build_object('reason', NULL::text)
            ),
            (
                'wrong_reason_type',
                2,
                canonical.event_payload || jsonb_build_object('reason', 7)
            ),
            (
                'null_next_run_at',
                2,
                canonical.event_payload
                    || jsonb_build_object('next_run_at', NULL::text)
            ),
            (
                'wrong_next_run_number_type',
                2,
                canonical.event_payload
                    || jsonb_build_object('next_run_number', '2'::text)
            ),
            (
                'null_next_run_number',
                2,
                canonical.event_payload
                    || jsonb_build_object('next_run_number', NULL::int4)
            ),
            (
                'wrong_next_run_at_type',
                2,
                canonical.event_payload
                    || jsonb_build_object('next_run_at', 12345)
            ),
            (
                'invalid_next_run_at',
                3,
                canonical.event_payload
                    || jsonb_build_object(
                        'next_run_number', 3,
                        'next_run_at', 'not-a-timestamp'
                    )
            ),
            (
                'non_rfc3339_next_run_at',
                2,
                canonical.event_payload
                    || jsonb_build_object('next_run_at', 'tomorrow')
            ),
            (
                'wrong_delay_type',
                2,
                canonical.event_payload
                    || jsonb_build_object('delay_microseconds', '0'::text)
            ),
            (
                'null_delay',
                2,
                canonical.event_payload
                    || jsonb_build_object('delay_microseconds', NULL::bigint)
            ),
            (
                'fractional_delay',
                2,
                canonical.event_payload
                    || jsonb_build_object('delay_microseconds', 0.5::numeric)
            ),
            (
                'negative_delay',
                2,
                canonical.event_payload
                    || jsonb_build_object('delay_microseconds', -1)
            ),
            (
                'overflow_delay',
                2,
                canonical.event_payload
                    || jsonb_build_object(
                        'delay_microseconds',
                        9223372036854775808::numeric
                    )
            )
    ) AS cases(case_name, current_run_number, event_payload)
),
inserted_jobs AS (
    INSERT INTO job_queue (
        job_type,
        payload,
        status,
        run_number,
        max_attempts,
        timeout_seconds,
        next_run_at
    )
    SELECT
        $1,
        jsonb_build_object('case_name', payload_cases.case_name),
        'PENDING',
        payload_cases.current_run_number,
        3,
        30,
        clock_timestamp() + interval '1 hour'
    FROM payload_cases
    RETURNING id, payload ->> 'case_name' AS case_name, run_number
)
INSERT INTO job_events (
    job_id,
    run_number,
    event_type,
    payload
)
SELECT
    inserted_jobs.id,
    inserted_jobs.run_number - 1,
    'REQUEUED',
    payload_cases.event_payload
FROM inserted_jobs
JOIN payload_cases USING (case_name)
        "#,
    )
    .bind(JOB_TYPE)
    .execute(&harness.pool)
    .await
    .expect("seed valid and malformed continuation event payloads");

    assert_eq!(
        load_continuation_metrics(&harness.pool, JOB_TYPE).await,
        (2, 2, 2),
        "only genuine kindless v0.6 and discriminated v0.7 continuation events count"
    );

    let factored_view_definition = continuation_metrics_view_definition(&harness.pool).await;
    assert!(
        factored_view_definition.contains("valid_continuation_events AS NOT MATERIALIZED"),
        "forward migration must keep the shared validity predicate in a local NOT MATERIALIZED CTE: {factored_view_definition}"
    );

    let custom_plan = explain_continuation_metrics_plan(&harness.pool, PlanCacheMode::Custom).await;
    let generic_plan =
        explain_continuation_metrics_plan(&harness.pool, PlanCacheMode::Generic).await;
    assert_eq!(
        continuation_plan_node_types(&custom_plan),
        continuation_plan_node_types(&generic_plan),
        "PostgreSQL 18 custom and generic plans must retain the same operator shape"
    );
    for (mode, plan) in [("custom", &custom_plan), ("generic", &generic_plan)] {
        eprintln!(
            "continuation metrics {mode} EXPLAIN ANALYZE node_types={:?}, planning_time_ms={}, execution_time_ms={}",
            continuation_plan_node_types(plan),
            plan[0]["Planning Time"],
            plan[0]["Execution Time"]
        );
        assert_eq!(
            plan[0]["Plan"]["Actual Rows"].as_f64(),
            Some(1.0),
            "{mode} EXPLAIN ANALYZE must return the seeded metrics row"
        );
        assert!(
            !plan.to_string().contains("CTE Scan"),
            "{mode} plan must inline the NOT MATERIALIZED CTE: {plan}"
        );
    }

    let cte_down_migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_down_migration()
                && migration.version == CONTINUATION_METRICS_CTE_MIGRATION_VERSION
        })
        .expect("continuation metrics CTE down migration exists");
    let mut conn = harness
        .pool
        .acquire()
        .await
        .expect("acquire continuation metrics CTE revert connection");
    (*conn)
        .revert(cte_down_migration)
        .await
        .expect("restore the duplicated strict continuation metrics predicate");
    drop(conn);

    assert_eq!(
        load_continuation_metrics(&harness.pool, JOB_TYPE).await,
        (2, 2, 2),
        "CTE down migration must preserve strict continuation metric results"
    );
    assert!(
        !continuation_metrics_view_definition(&harness.pool)
            .await
            .contains("valid_continuation_events"),
        "CTE down migration must restore the prior duplicated view definition"
    );

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("reapply continuation metrics CTE migration");
    assert_eq!(
        load_continuation_metrics(&harness.pool, JOB_TYPE).await,
        (2, 2, 2),
        "reapplying the CTE migration must preserve strict continuation metric results"
    );
    assert!(
        continuation_metrics_view_definition(&harness.pool)
            .await
            .contains("valid_continuation_events AS NOT MATERIALIZED"),
        "reapplying the CTE migration must restore the local NOT MATERIALIZED CTE"
    );

    let down_migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_down_migration()
                && migration.version == CONTINUATION_METRICS_VALIDATION_MIGRATION_VERSION
        })
        .expect("continuation metrics validation down migration exists");
    let mut conn = harness
        .pool
        .acquire()
        .await
        .expect("acquire continuation metrics revert connection");
    (*conn)
        .revert(down_migration)
        .await
        .expect("restore the prior continuation metrics view");
    drop(conn);

    assert_eq!(
        load_continuation_metrics(&harness.pool, JOB_TYPE).await,
        (16, 14, 3),
        "down migration must restore the published v0.7 payload-presence semantics"
    );

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("reapply continuation metrics validation migration");
    assert_eq!(
        load_continuation_metrics(&harness.pool, JOB_TYPE).await,
        (2, 2, 2),
        "reapplying the forward migration restores strict payload validation"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn replay_metrics_down_drops_lineage_objects_but_preserves_queue_rows() {
    let harness = TestHarness::fresh("runledger_pg_replay_metrics_down").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply all migrations before replay down test");
    seed_legacy_job_definition(&harness.pool).await;

    let source_job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type, payload, status, max_attempts, timeout_seconds, finished_at
         )
         VALUES (
            'jobs.test.legacy_cutover', '{}'::jsonb, 'SUCCEEDED', 3, 30, now()
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert replay source for down test");
    let replay_job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type, payload, status, max_attempts, timeout_seconds
         )
         VALUES (
            'jobs.test.legacy_cutover', '{}'::jsonb, 'PENDING', 3, 30
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert replay job for down test");
    sqlx::query(
        "INSERT INTO job_replays (
            source_job_id,
            source_run_number,
            replay_request_key,
            replay_job_id,
            reason
         )
         VALUES ($1, 1, 'down-test', $2, 'verify destructive down boundary')",
    )
    .bind(source_job_id)
    .bind(replay_job_id)
    .execute(&harness.pool)
    .await
    .expect("insert replay lineage for down test");

    let replay_delete_policies = sqlx::query_as::<_, (String, String)>(
        "SELECT conname, confdeltype::text
         FROM pg_constraint
         WHERE conname IN (
            'fk_job_replays_source_job',
            'fk_job_replays_replay_job'
         )
         ORDER BY conname",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("inspect replay lineage delete policy");
    assert_eq!(
        replay_delete_policies,
        vec![
            ("fk_job_replays_replay_job".to_owned(), "a".to_owned()),
            ("fk_job_replays_source_job".to_owned(), "c".to_owned()),
        ],
        "replay-only deletion must retain the idempotency guard while source deletion removes its lineage"
    );

    let down_migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_down_migration()
                && migration.version == REPLAY_METRICS_MIGRATION_VERSION
        })
        .expect("replay and metrics down migration exists");
    let mut conn = harness
        .pool
        .acquire()
        .await
        .expect("acquire revert connection");
    (*conn)
        .revert(down_migration)
        .await
        .expect("revert replay and metrics migration");
    drop(conn);

    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reverted object {object}: {error}")),
            None,
            "{object} must be removed by the down migration"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue WHERE id = ANY($1::uuid[])",)
            .bind(vec![source_job_id, replay_job_id])
            .fetch_one(&harness.pool)
            .await
            .expect("count queue rows after lineage down migration"),
        2,
        "down migration must not delete source or replay queue rows"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_ignores_unrelated_sqlx_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_shared").await;
    seed_unrelated_sqlx_migration(&harness.pool, 202401010001, false).await;

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply runledger migrations alongside app migrations");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("schema should validate when unrelated migrations are present");

    let migration_versions =
        sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&harness.pool)
            .await
            .expect("list applied migrations");
    assert!(migration_versions.contains(&202401010001));
    for version in runledger_migration_versions() {
        assert!(migration_versions.contains(&version));
    }

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_fresh_database_without_migrations() {
    let harness = TestHarness::fresh("runledger_pg_validate").await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should fail before migrations are applied");
    match &error {
        SchemaCompatibilityError::MissingMigrationHistory {
            required_first_migration_version,
        } => {
            assert_eq!(*required_first_migration_version, 202603280001);
        }
        other => panic!("unexpected migration validation error: {other}"),
    }
    assert!(
        error.to_string().contains("_sqlx_migrations"),
        "missing history error should explain the required table"
    );

    let migrations_table_exists =
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&harness.pool)
            .await
            .expect("check for migrations table");
    assert!(
        !migrations_table_exists,
        "schema compatibility validation must not create _sqlx_migrations"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_ignores_unrelated_sqlx_history() {
    let harness = TestHarness::fresh("runledger_pg_validate_shared").await;
    seed_unrelated_sqlx_migration(&harness.pool, 202401010001, false).await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("validation should ignore unrelated migration versions");

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_ignores_unrelated_sqlx_history_with_runledger_description() {
    let harness = TestHarness::fresh("runledger_pg_validate_shared_named").await;
    seed_sqlx_migration(
        &harness.pool,
        202401010001,
        "runledger host app schema",
        true,
        vec![1_u8, 2, 3, 4],
    )
    .await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("validation should ignore unrelated descriptions");

    harness.teardown().await;
}

#[test]
fn vendored_migration_copies_match_root_migrations() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("postgres crate should live under workspace root");
    let root_migrations = workspace_root.join("migrations");

    assert_migration_dir_matches(
        &root_migrations,
        &workspace_root.join("runledger-postgres/migrations"),
    );
    assert_migration_dir_matches(
        &root_migrations,
        &workspace_root.join("runledger-test-support/migrations"),
    );
}

#[test]
fn compatibility_fence_exemptions_are_explicit_bundled_migrations() {
    let bundled_versions = runledger_migration_versions()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let exempt_versions = COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        exempt_versions.len(),
        COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS.len(),
        "compatibility-fence exemptions must be unique"
    );
    assert!(
        exempt_versions.is_subset(&bundled_versions),
        "every compatibility-fence exemption must name a bundled up migration"
    );
    assert_eq!(
        expected_compatibility_fence_versions().len() + exempt_versions.len(),
        bundled_versions.len(),
        "every bundled up migration must be fenced unless explicitly exempted"
    );
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_validate_legacy").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    seed_legacy_idempotency_rows(&harness.pool).await;
    apply_enqueue_request_cutover_migration(&harness.pool).await;
    apply_runledger_migrations_after_cutover(&harness.pool).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject legacy keyed rows without snapshots");
    assert_legacy_idempotency_snapshot_error(&error, 1, 1);

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_after_idempotency_cutover_rejects_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_migrate_legacy").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    seed_legacy_idempotency_rows(&harness.pool).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject legacy keyed rows without snapshots");
    assert_legacy_idempotency_snapshot_error(&error, 1, 1);

    harness.teardown().await;
}

#[tokio::test]
async fn workflow_results_migration_preserves_existing_enqueue_request_snapshots() {
    let harness = TestHarness::fresh("runledger_pg_workflow_results_preserve").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    apply_enqueue_request_cutover_migration(&harness.pool).await;
    insert_workflow_row_with_pre_result_snapshot(&harness.pool).await;

    apply_runledger_migrations_after_cutover(&harness.pool).await;

    let snapshot = sqlx::query_scalar::<_, Value>(
        "SELECT enqueue_request
         FROM workflow_runs
         WHERE idempotency_key = 'legacy-result-snapshot'
         LIMIT 1",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("load preserved workflow snapshot");
    assert_eq!(
        snapshot,
        json!({
            "metadata": {},
            "steps": []
        })
    );
    assert!(snapshot.get("result_step_key").is_none());

    harness.teardown().await;
}

#[tokio::test]
async fn enqueue_request_cutover_constraints_reject_new_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_cutover_constraints").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    seed_legacy_job_definition(&harness.pool).await;

    let job_error = insert_legacy_job_row(&harness.pool, "legacy-job-after-cutover")
        .await
        .expect_err("job_queue constraint should reject keyed rows without enqueue_request");
    assert!(
        job_error
            .to_string()
            .contains("ck_job_queue_idempotency_enqueue_request"),
        "unexpected job constraint error: {job_error}"
    );

    let workflow_error = insert_legacy_workflow_row(&harness.pool, "legacy-workflow-after-cutover")
        .await
        .expect_err("workflow_runs constraint should reject keyed rows without enqueue_request");
    assert!(
        workflow_error
            .to_string()
            .contains("ck_workflow_runs_idempotency_enqueue_request"),
        "unexpected workflow constraint error: {workflow_error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_rejects_conflicting_sqlx_version_namespace() {
    let harness = TestHarness::fresh("runledger_pg_migrate_conflict").await;
    let conflicting_version = runledger_migration_versions()
        .into_iter()
        .next()
        .expect("runledger should include at least one up migration");
    seed_unrelated_sqlx_migration(&harness.pool, conflicting_version, true).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject conflicting version namespace");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMismatch(version))
                if *version == conflicting_version
        ),
        "unexpected migration error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_newer").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject newer runledger history");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if *version == newer_version
        ),
        "unexpected migration error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_conflicting_sqlx_version_namespace() {
    let harness = TestHarness::fresh("runledger_pg_validate_conflict").await;
    let conflicting_version = runledger_migration_versions()
        .into_iter()
        .next()
        .expect("runledger should include at least one up migration");
    seed_unrelated_sqlx_migration(&harness.pool, conflicting_version, true).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject conflicting version namespace");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMismatch(version))
                if *version == conflicting_version
        ),
        "unexpected schema compatibility error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_validate_newer").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject newer runledger history");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if *version == newer_version
        ),
        "unexpected schema compatibility error: {error}"
    );

    harness.teardown().await;
}

async fn load_continuation_metrics(pool: &PgPool, job_type: &str) -> (i64, i64, i32) {
    sqlx::query_as::<_, (i64, i64, i32)>(
        "SELECT continued_24h, active_continued_count, max_active_run_number
         FROM job_continuation_metrics_rollup
         WHERE organization_id IS NULL
           AND job_type = $1",
    )
    .bind(job_type)
    .fetch_one(pool)
    .await
    .expect("load continuation metrics")
}

async fn continuation_metrics_view_definition(pool: &PgPool) -> String {
    sqlx::query_scalar::<_, String>(
        "SELECT pg_get_viewdef('job_continuation_metrics_rollup'::regclass, true)",
    )
    .fetch_one(pool)
    .await
    .expect("load continuation metrics view definition")
}

#[derive(Clone, Copy)]
enum PlanCacheMode {
    Custom,
    Generic,
}

async fn explain_continuation_metrics_plan(pool: &PgPool, mode: PlanCacheMode) -> Value {
    let mut conn = pool
        .acquire()
        .await
        .expect("acquire continuation metrics plan connection");
    let set_plan_cache_mode = match mode {
        PlanCacheMode::Custom => "SET plan_cache_mode = force_custom_plan",
        PlanCacheMode::Generic => "SET plan_cache_mode = force_generic_plan",
    };
    sqlx::query(set_plan_cache_mode)
        .execute(&mut *conn)
        .await
        .expect("set continuation metrics plan cache mode");
    sqlx::query(
        "PREPARE runledger_continuation_metrics_plan(text) AS
         SELECT continued_24h, active_continued_count, max_active_run_number
         FROM job_continuation_metrics_rollup
         WHERE organization_id IS NULL
           AND job_type = $1",
    )
    .execute(&mut *conn)
    .await
    .expect("prepare continuation metrics plan probe");
    let plan = sqlx::query_scalar::<_, Value>(
        "EXPLAIN (ANALYZE, FORMAT JSON)
         EXECUTE runledger_continuation_metrics_plan(
             'jobs.test.continuation_payload_validation'
         )",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("explain continuation metrics prepared statement");
    sqlx::query("DEALLOCATE runledger_continuation_metrics_plan")
        .execute(&mut *conn)
        .await
        .expect("deallocate continuation metrics plan probe");
    sqlx::query("RESET plan_cache_mode")
        .execute(&mut *conn)
        .await
        .expect("reset continuation metrics plan cache mode");

    plan
}

fn continuation_plan_node_types(plan: &Value) -> Vec<String> {
    fn collect(node: &Value, node_types: &mut Vec<String>) {
        if let Some(node_type) = node["Node Type"].as_str() {
            node_types.push(node_type.to_owned());
        }
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                collect(child, node_types);
            }
        }
    }

    let mut node_types = Vec::new();
    collect(&plan[0]["Plan"], &mut node_types);
    node_types
}

async fn advisory_lock_count_for_backend(pool: &PgPool, backend_pid: i32) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM pg_locks
         WHERE locktype = 'advisory'
           AND mode = 'ExclusiveLock'
           AND granted
           AND pid = $1",
    )
    .bind(backend_pid)
    .fetch_one(pool)
    .await
    .expect("count backend advisory locks")
}

async fn advisory_lock_count_for_database(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM pg_locks
         WHERE locktype = 'advisory'
           AND mode = 'ExclusiveLock'
           AND granted
           AND database = (
               SELECT oid
               FROM pg_database
               WHERE datname = current_database()
           )",
    )
    .fetch_one(pool)
    .await
    .expect("count database advisory locks")
}

fn runledger_migration_versions() -> Vec<i64> {
    MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect()
}

fn raw_v0_6_migrator() -> Migrator {
    let migrations = MIGRATOR
        .iter()
        .filter(|migration| migration.version <= V0_6_LATEST_MIGRATION_VERSION)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        migrations.iter().any(|migration| {
            migration.version == V0_6_LATEST_MIGRATION_VERSION
                && migration.migration_type.is_up_migration()
        }),
        "raw v0.6 migration fixture must include its final up migration"
    );

    Migrator {
        migrations: Cow::Owned(migrations),
        ..Migrator::DEFAULT
    }
}

fn raw_v0_7_migrator() -> Migrator {
    let migrations = MIGRATOR
        .iter()
        .filter(|migration| migration.version <= REPLAY_METRICS_MIGRATION_VERSION)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        migrations.iter().any(|migration| {
            migration.version == REPLAY_METRICS_MIGRATION_VERSION
                && migration.migration_type.is_up_migration()
        }),
        "raw v0.7 migration fixture must include its final up migration"
    );

    Migrator {
        migrations: Cow::Owned(migrations),
        ..Migrator::DEFAULT
    }
}

fn expected_compatibility_fence_versions() -> Vec<i64> {
    runledger_migration_versions()
        .into_iter()
        .filter(|version| !COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS.contains(version))
        .collect()
}

fn assert_migration_dir_matches(expected_dir: &Path, actual_dir: &Path) {
    let expected_names = migration_file_names(expected_dir);
    let actual_names = migration_file_names(actual_dir);
    assert_eq!(
        actual_names,
        expected_names,
        "migration filenames in {} must match {}",
        actual_dir.display(),
        expected_dir.display()
    );

    for name in expected_names {
        let expected = fs::read(expected_dir.join(&name)).unwrap_or_else(|error| {
            panic!(
                "read expected migration {} from {}: {error}",
                name,
                expected_dir.display()
            )
        });
        let actual = fs::read(actual_dir.join(&name)).unwrap_or_else(|error| {
            panic!(
                "read actual migration {} from {}: {error}",
                name,
                actual_dir.display()
            )
        });
        assert_eq!(
            actual,
            expected,
            "migration {} in {} must match {}",
            name,
            actual_dir.display(),
            expected_dir.display()
        );
    }
}

fn migration_file_names(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read migration directory {}: {error}", dir.display()))
        .map(|entry| {
            let entry = entry.unwrap_or_else(|error| {
                panic!("read migration entry in {}: {error}", dir.display())
            });
            entry
                .file_name()
                .to_str()
                .unwrap_or_else(|| {
                    panic!(
                        "migration file name is not valid UTF-8 in {}",
                        dir.display()
                    )
                })
                .to_owned()
        })
        .filter(|name| name.ends_with(".sql"))
        .collect()
}

async fn apply_runledger_migrations_before_cutover(pool: &PgPool) {
    apply_runledger_migrations_through(pool, ENQUEUE_REQUEST_CUTOVER_VERSION - 1).await;
}

async fn apply_runledger_migrations_through(pool: &PgPool, latest_version: i64) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");
    (*conn)
        .ensure_migrations_table()
        .await
        .expect("create sqlx migrations table");

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version <= latest_version)
    {
        (*conn).apply(migration).await.unwrap_or_else(|error| {
            panic!(
                "apply Runledger migration {} through {latest_version}: {error}",
                migration.version
            )
        });
    }
}

async fn execute_migration_sql_without_history(pool: &PgPool, migration_version: i64) {
    let migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_up_migration() && migration.version == migration_version
        })
        .unwrap_or_else(|| panic!("migration {migration_version} should exist"));

    sqlx::raw_sql(migration.sql.as_ref())
        .execute(pool)
        .await
        .unwrap_or_else(|error| {
            panic!("execute migration {migration_version} without history: {error}")
        });
}

async fn migration_history_contains(pool: &PgPool, migration_version: i64) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM _sqlx_migrations
            WHERE version = $1 AND success
         )",
    )
    .bind(migration_version)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("read migration history for {migration_version}: {error}"))
}

async fn apply_enqueue_request_cutover_migration(pool: &PgPool) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");
    let migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version == ENQUEUE_REQUEST_CUTOVER_VERSION
        })
        .expect("enqueue request cutover migration should exist");

    (*conn)
        .apply(migration)
        .await
        .expect("apply enqueue request cutover migration");
}

async fn apply_runledger_migrations_after_cutover(pool: &PgPool) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version > ENQUEUE_REQUEST_CUTOVER_VERSION)
    {
        (*conn).apply(migration).await.unwrap_or_else(|error| {
            panic!(
                "apply post-cutover Runledger migration {}: {error}",
                migration.version
            )
        });
    }
}

fn assert_legacy_idempotency_snapshot_error(
    error: &SchemaCompatibilityError,
    job_count: i64,
    workflow_count: i64,
) {
    assert!(
        matches!(
            error,
            SchemaCompatibilityError::LegacyIdempotencySnapshotsMissing {
                job_count: actual_job_count,
                workflow_count: actual_workflow_count,
            } if *actual_job_count == job_count && *actual_workflow_count == workflow_count
        ),
        "unexpected schema compatibility error: {error}"
    );
}

async fn seed_runledger_migration_history(pool: &PgPool, version: i64) {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS runledger_migration_history (
    version BIGINT PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
        "#,
    )
    .execute(pool)
    .await
    .expect("create runledger migration history table");

    sqlx::query(
        r#"
INSERT INTO runledger_migration_history (version)
VALUES ($1)
        "#,
    )
    .bind(version)
    .execute(pool)
    .await
    .expect("insert runledger migration history");
}

async fn idempotency_cutover_constraints_valid(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT COUNT(*) FILTER (WHERE c.convalidated) = 2
         FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         WHERE (t.relname, c.conname) IN (
             ('job_queue', 'ck_job_queue_idempotency_enqueue_request'),
             ('workflow_runs', 'ck_workflow_runs_idempotency_enqueue_request')
         )",
    )
    .fetch_one(pool)
    .await
    .expect("check idempotency cutover constraint validation")
}

async fn seed_legacy_idempotency_rows(pool: &PgPool) {
    seed_legacy_job_definition(pool).await;
    insert_legacy_job_row(pool, "legacy-job")
        .await
        .expect("insert legacy job row");
    insert_legacy_workflow_row(pool, "legacy-workflow")
        .await
        .expect("insert legacy workflow row");
}

async fn seed_legacy_job_definition(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ('jobs.test.legacy_cutover', 1, 3, 30, 100, true)",
    )
    .execute(pool)
    .await
    .expect("insert job definition");
}

async fn insert_legacy_job_row(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO job_queue (
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            idempotency_key
         )
         VALUES (
            'jobs.test.legacy_cutover',
            '{}'::jsonb,
            100,
            3,
            30,
            $1
         )",
    )
    .bind(idempotency_key)
    .execute(pool)
    .await
}

async fn insert_legacy_workflow_row(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO workflow_runs (
            workflow_type,
            idempotency_key,
            metadata
         )
         VALUES (
            'workflow.test.legacy_cutover',
            $1,
            '{}'::jsonb
         )",
    )
    .bind(idempotency_key)
    .execute(pool)
    .await
}

async fn insert_workflow_row_with_pre_result_snapshot(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO workflow_runs (
            workflow_type,
            idempotency_key,
            metadata,
            enqueue_request
         )
         VALUES (
            'workflow.test.pre_result_snapshot',
            'legacy-result-snapshot',
            '{}'::jsonb,
            $1::jsonb
         )",
    )
    .bind(json!({
        "metadata": {},
        "steps": []
    }))
    .execute(pool)
    .await
    .expect("insert pre-result workflow snapshot");
}

async fn seed_unrelated_sqlx_migration(pool: &PgPool, version: i64, success: bool) {
    seed_sqlx_migration(
        pool,
        version,
        "host app schema",
        success,
        vec![1_u8, 2, 3, 4],
    )
    .await;
}

async fn seed_sqlx_migration(
    pool: &PgPool,
    version: i64,
    description: &str,
    success: bool,
    checksum: Vec<u8>,
) {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
)
        "#,
    )
    .execute(pool)
    .await
    .expect("create shared sqlx migrations table");

    sqlx::query(
        r#"
INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
VALUES ($1, $2, $3, $4, 0)
        "#,
    )
    .bind(version)
    .bind(description)
    .bind(success)
    .bind(checksum)
    .execute(pool)
    .await
    .expect("insert sqlx migration history");
}
