use super::{OBSERVE, Observation};
use runledger_postgres::{PgSessionProfile, RunledgerDatabase};
use runledger_test_support::{
    EphemeralDatabase, setup_unmigrated_ephemeral_pool, teardown_ephemeral_pool,
};
use sqlx::{PgConnection, PgPool, postgres::PgPoolOptions};
use std::time::Duration;

const SCHEMA: &str = "fast serving";
const CONTAMINATE: &str = "RESET ROLE; SET search_path = pg_temp; SET statement_timeout = 0; SET lock_timeout = 0; SET app.tenant = 'wrong'";

struct Fixture {
    admin: PgPool,
    ephemeral: EphemeralDatabase,
    database: RunledgerDatabase,
    role: String,
    expected: Observation,
}

impl Fixture {
    async fn new() -> Self {
        let (admin, ephemeral) = setup_unmigrated_ephemeral_pool("profile_fast", 2).await;
        let (version, major): (String, i32) = sqlx::query_as(
            "SELECT current_setting('server_version'), current_setting('server_version_num')::int / 10000",
        ).fetch_one(&admin).await.expect("PostgreSQL version");
        assert_eq!(major, 18);
        eprintln!("fast acquisition PostgreSQL {version}");
        let role = format!("fast_{}", sqlx::types::Uuid::new_v4().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role}; CREATE SCHEMA \"{SCHEMA}\"; GRANT USAGE ON SCHEMA \"{SCHEMA}\" TO {role}"
        ))).execute(&admin).await.expect("provision profile fixture");
        let options = admin.connect_options();
        let login = options.get_username().to_owned();
        let profile = PgSessionProfile::new(
            &login,
            &role,
            vec![SCHEMA.into()],
            Duration::from_secs(2),
            Duration::from_millis(250),
        )
        .expect("profile")
        .with_setting("app.tenant", "tenant-one")
        .expect("tenant");
        let database = RunledgerDatabase::connect_lazy(
            (*options).clone(),
            profile,
            PgPoolOptions::new()
                .max_connections(1)
                .min_connections(0)
                .idle_timeout(None)
                .max_lifetime(None),
        )
        .expect("configured one-slot pool");
        let expected = (
            login,
            role.clone(),
            SCHEMA.into(),
            "2s".into(),
            "250ms".into(),
            "tenant-one".into(),
        );
        Self {
            admin,
            ephemeral,
            database,
            role,
            expected,
        }
    }

    async fn finish(self) {
        self.database.pool().close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {}; DROP ROLE {}",
            self.role, self.role,
        )))
        .execute(&self.admin)
        .await
        .expect("remove fixture role");
        teardown_ephemeral_pool(self.admin, self.ephemeral).await;
    }
}

async fn wait_for_state(pool: &PgPool, ready: fn(&PgPool) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready(pool) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("pool did not reach expected release state");
}

async fn observe(connection: &mut PgConnection) -> (Observation, i32) {
    let observation = sqlx::query_as(OBSERVE)
        .fetch_one(&mut *connection)
        .await
        .expect("observe serving profile");
    let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("backend PID");
    let path: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(connection)
        .await
        .expect("trusted search path");
    assert_eq!(path, "\"fast serving\", pg_temp");
    (observation, pid)
}

#[derive(Clone, Copy, Debug)]
enum FastPath {
    Acquire,
    Begin,
    BeginWith,
}

async fn observe_fast_path(pool: &PgPool, path: FastPath) -> (Observation, i32) {
    match path {
        FastPath::Acquire => {
            let mut connection = pool
                .try_acquire()
                .expect("released session is immediately available");
            observe(&mut connection).await
        }
        FastPath::Begin | FastPath::BeginWith => {
            let mut tx = match path {
                FastPath::Begin => pool.try_begin().await,
                FastPath::BeginWith => pool.try_begin_with("BEGIN READ ONLY").await,
                FastPath::Acquire => unreachable!(),
            }
            .expect("fast transaction begin")
            .expect("released session is immediately available");
            let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
                .fetch_one(&mut *tx)
                .await
                .expect("requested transaction mode");
            assert_eq!(
                read_only,
                if matches!(path, FastPath::BeginWith) {
                    "on"
                } else {
                    "off"
                }
            );
            let result = observe(&mut tx).await;
            tx.rollback()
                .await
                .expect("acknowledge transaction rollback");
            result
        }
    }
}

async fn check_fast_path(path: FastPath) {
    let fixture = Fixture::new().await;
    let pool = fixture.database.pool();
    for state in ["idle", "open transaction", "aborted transaction"] {
        let mut connection = pool.acquire().await.expect("ordinary profiled acquisition");
        let (ordinary, original_pid) = observe(&mut connection).await;
        assert_eq!(ordinary, fixture.expected, "ordinary acquisition control");
        sqlx::raw_sql(CONTAMINATE)
            .execute(&mut *connection)
            .await
            .expect("contaminate session");
        if state != "idle" {
            sqlx::raw_sql("BEGIN")
                .execute(&mut *connection)
                .await
                .expect("leave raw transaction open");
        }
        if state == "aborted transaction" {
            sqlx::query("SELECT 1 / 0")
                .execute(&mut *connection)
                .await
                .expect_err("abort raw transaction");
        }
        drop(connection);
        // Do not repair the session through an asynchronous acquisition here.
        wait_for_state(pool, |pool| pool.num_idle() == 1).await;
        let (observed, pid) = observe_fast_path(pool, path).await;
        assert_eq!(
            observed, fixture.expected,
            "{path:?} inherited {state} contamination"
        );
        assert_eq!(
            pid, original_pid,
            "valid restored sessions must remain reusable"
        );
    }
    fixture.finish().await;
}

#[tokio::test]
async fn try_acquire_restores_cross_borrower_profile() {
    check_fast_path(FastPath::Acquire).await;
}

#[tokio::test]
async fn try_begin_restores_cross_borrower_profile() {
    check_fast_path(FastPath::Begin).await;
}

#[tokio::test]
async fn try_begin_with_restores_cross_borrower_profile() {
    check_fast_path(FastPath::BeginWith).await;
}

#[tokio::test]
async fn failed_release_profile_verification_discards_connection() {
    let fixture = Fixture::new().await;
    let pool = fixture.database.pool();
    let mut connection = pool.acquire().await.expect("initial acquisition");
    let (_, original_pid) = observe(&mut connection).await;
    sqlx::raw_sql(CONTAMINATE)
        .execute(&mut *connection)
        .await
        .expect("contaminate session");
    // The socket is healthy, but applying the declared profile can no longer
    // verify schema access. Only failed profile restoration should retire it.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "REVOKE USAGE ON SCHEMA \"{SCHEMA}\" FROM {}",
        fixture.role,
    )))
    .execute(&fixture.admin)
    .await
    .expect("invalidate schema policy");
    drop(connection);
    wait_for_state(pool, |pool| pool.size() == 0).await;
    assert_eq!(pool.num_idle(), 0);
    assert!(pool.try_acquire().is_none());
    assert!(pool.try_begin().await.expect("empty pool").is_none());
    assert!(
        pool.try_begin_with("BEGIN")
            .await
            .expect("empty pool")
            .is_none()
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "GRANT USAGE ON SCHEMA \"{SCHEMA}\" TO {}",
        fixture.role,
    )))
    .execute(&fixture.admin)
    .await
    .expect("restore schema policy");
    let mut replacement = pool
        .acquire()
        .await
        .expect("capacity is reusable after failed release");
    let (observed, pid) = observe(&mut replacement).await;
    assert_eq!(observed, fixture.expected);
    assert_ne!(
        pid, original_pid,
        "failed restoration must not reuse the session"
    );
    drop(replacement);
    fixture.finish().await;
}
