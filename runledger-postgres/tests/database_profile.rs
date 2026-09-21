use runledger_postgres::{
    PgSessionProfile, RunledgerDatabase, ensure_schema_compatible_after_idempotency_cutover,
    migrate_after_idempotency_cutover, run_atomic,
};
use runledger_test_support::{setup_unmigrated_ephemeral_pool, teardown_ephemeral_pool};
use std::time::Duration;

#[path = "database_profile/fast_paths.rs"]
mod fast_paths;
mod support;

const OBSERVE: &str = "SELECT session_user::text, current_user::text, pg_catalog.current_schema()::text, pg_catalog.current_setting('statement_timeout'), pg_catalog.current_setting('lock_timeout'), pg_catalog.current_setting('app.tenant')";
type Observation = (String, String, String, String, String, String);

#[tokio::test]
async fn profile_setup_failure_is_redacted_but_native_cause_is_inspectable() {
    let (admin, fixture) = setup_unmigrated_ephemeral_pool("profile_redaction", 2).await;
    let options = admin.connect_options();
    let login = options.get_username();
    let profile = PgSessionProfile::new(
        login,
        login,
        vec!["public".into()],
        Duration::ZERO,
        Duration::ZERO,
    )
    .expect("valid profile declaration");
    let marker = "runledger-profile-secret-invalid-timezone";
    let error = RunledgerDatabase::connect(
        (*options).clone(),
        profile
            .clone()
            .with_setting("TimeZone", marker)
            .expect("valid setting name"),
        1,
    )
    .await
    .expect_err("invalid timezone must fail before database access");
    assert!(!format!("{error} {error:?}").contains(marker));
    let sqlx::Error::Configuration(cause) = error else {
        panic!("profile error must retain safe-display boundary");
    };
    let retained = cause
        .downcast_ref::<batter_sqlx::SqlxFailure>()
        .expect("native cause wrapper");
    let native = retained
        .native()
        .as_database_error()
        .expect("original database error");
    assert_eq!(native.code().as_deref(), Some("22023"));
    assert!(native.message().contains(marker));
    let database = RunledgerDatabase::connect(
        (*options).clone(),
        profile
            .with_setting("TimeZone", "UTC")
            .expect("valid timezone"),
        1,
    )
    .await
    .expect("valid profile still connects");
    let timezone: String = sqlx::query_scalar("SELECT current_setting('timezone')")
        .fetch_one(database.pool())
        .await
        .expect("ordinary pool access");
    assert_eq!(timezone, "UTC");
    database.pool().close().await;
    teardown_ephemeral_pool(admin, fixture).await;
}

#[tokio::test]
async fn authority_schema_and_timeouts_agree_across_all_paths() {
    let (admin, fixture) = setup_unmigrated_ephemeral_pool("database_profile", 5).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&admin)
        .await
        .expect("version");
    eprintln!("database profile PostgreSQL {version}");
    let major: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int / 10000")
            .fetch_one(&admin)
            .await
            .expect("major");
    assert_eq!(major, 18);
    let role = format!("profile_{}", sqlx::types::Uuid::new_v4().simple());
    let schema = "tenant application's \"data\"";
    let quoted_schema = "\"tenant application's \"\"data\"\"\"";
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE ROLE {role}; CREATE SCHEMA {quoted_schema}; GRANT USAGE ON SCHEMA {quoted_schema} TO {role}"))).execute(&admin).await.expect("provision role/schema fixture");
    let options = admin.connect_options();
    let login = options.get_username();
    let profile = |role: &str| {
        PgSessionProfile::new(
            login,
            role,
            vec![schema.into()],
            Duration::from_secs(2),
            Duration::from_millis(250),
        )
        .expect("profile")
        .with_setting("app.tenant", "tenant-one")
        .expect("tenant policy")
    };
    let migration_db = RunledgerDatabase::connect((*options).clone(), profile(login), 2)
        .await
        .expect("migration profile");
    migrate_after_idempotency_cutover(&migration_db)
        .await
        .expect("migrate authoritative custom schema");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("GRANT ALL ON ALL TABLES IN SCHEMA {quoted_schema} TO {role}; GRANT ALL ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {role}"))).execute(&admin).await.expect("serving grants");

    legacy_hook_control(&options, &role, schema, quoted_schema).await;

    let serving = RunledgerDatabase::connect((*options).clone(), profile(&role), 1)
        .await
        .expect("serving profile");
    let expected: Observation = (
        login.into(),
        role.clone(),
        schema.into(),
        "2s".into(),
        "250ms".into(),
        "tenant-one".into(),
    );
    let ordinary: Observation = sqlx::query_as(OBSERVE)
        .fetch_one(serving.pool())
        .await
        .expect("ordinary profile");
    let atomic: Observation = run_atomic(&serving, async |mut scope| {
        scope
            .application(async |sql| sqlx::query_as(OBSERVE).fetch_one(sql.executor()).await)
            .await
    })
    .await
    .expect("atomic profile");
    let snapshot: Observation = batter_sqlx::PgReadOnlySnapshot::inspect_profiled(
        serving.pool(),
        serving.profile(),
        async |sql| sqlx::query_as(OBSERVE).fetch_one(sql.executor()).await,
    )
    .await
    .expect("snapshot profile");
    assert_eq!(ordinary, expected);
    assert_eq!(atomic, expected);
    assert_eq!(snapshot, expected);
    let witness = ensure_schema_compatible_after_idempotency_cutover(&serving)
        .await
        .expect("same schema and authority verification");
    assert_eq!(witness.schema(), schema);
    check_reset_and_tampering(&serving, &expected).await;
    let wrong_login = PgSessionProfile::new(
        "not_the_authenticated_role",
        &role,
        vec![schema.into()],
        Duration::ZERO,
        Duration::ZERO,
    )
    .expect("syntactically valid but wrong profile");
    let mut called = false;
    let rejected = batter_sqlx::run_atomic_profiled(serving.pool(), &wrong_login, async |_| {
        called = true;
        Ok::<_, ()>(())
    })
    .await;
    assert!(matches!(
        rejected,
        Err(batter_sqlx::PgAtomicError::Begin(_))
    ));
    assert!(!called, "profile failure must precede consumer work");
    support::register_test_job_definition(serving.pool(), "test.profile.job").await;
    let payload = serde_json::json!({});
    support::enqueue_test_job(serving.pool(), "test.profile.job", None, &payload).await;
    let public_history: bool =
        sqlx::query_scalar("SELECT to_regclass('public._sqlx_migrations') IS NOT NULL")
            .fetch_one(&admin)
            .await
            .expect("public schema untouched");
    assert!(!public_history);
    serving.pool().close().await;
    migration_db.pool().close().await;
    // Role is cluster-wide; explicitly remove its grants/ownership before drop.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role}"
    )))
    .execute(&admin)
    .await
    .expect("remove fixture role");
    teardown_ephemeral_pool(admin, fixture).await;
}

async fn check_reset_and_tampering(database: &RunledgerDatabase, expected: &Observation) {
    // Return a contaminated session through the ordinary SQLx surface. The
    // mandatory next-acquisition hook must restore the full declared policy.
    sqlx::raw_sql("RESET ROLE; SET search_path = pg_temp; SET statement_timeout = 0; SET lock_timeout = 0; SET app.tenant = 'wrong'")
        .execute(database.pool()).await.expect("contaminate idle pool session");
    let next: Observation = sqlx::query_as(OBSERVE)
        .fetch_one(database.pool())
        .await
        .expect("reset on ordinary acquisition");
    assert_eq!(&next, expected);
    let tampered = run_atomic(database, async |mut scope| {
        scope
            .application(async |sql| {
                sqlx::raw_sql("RESET ROLE").execute(sql.executor()).await?;
                Ok::<_, sqlx::Error>(())
            })
            .await
    })
    .await;
    assert!(matches!(
        tampered,
        Err(runledger_postgres::PgAtomicError::Uncertain(
            runledger_postgres::PgAtomicUncertainty::ScopeLost { .. }
        ))
    ));
    let next: Observation = sqlx::query_as(OBSERVE)
        .fetch_one(database.pool())
        .await
        .expect("tampered scope retired");
    assert_eq!(&next, expected);
}

async fn legacy_hook_control(
    options: &sqlx::postgres::PgConnectOptions,
    role: &str,
    schema: &str,
    quoted_schema: &str,
) {
    let login = options.get_username();
    // Reproduce the old hook/reset mismatch as a control: ordinary pool work is
    // downgraded, but the unprofiled foundation runner resets to the login role.
    let set_role = format!(
        "SET ROLE {role}; SET search_path = {quoted_schema}, public; SET statement_timeout = '2s'; SET lock_timeout = '250ms'; SET app.tenant = 'tenant-one'"
    );
    let legacy = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |conn, _| {
            let statement = set_role.to_owned();
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                    .execute(conn)
                    .await
                    .map(|_| ())
            })
        })
        .connect_with((*options).clone())
        .await
        .expect("legacy hook pool");
    let ordinary: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&legacy)
        .await
        .expect("legacy role");
    assert_eq!(ordinary, role);
    let hook_policy: Observation = sqlx::query_as(OBSERVE)
        .fetch_one(&legacy)
        .await
        .expect("legacy hook policy");
    assert_eq!(
        hook_policy,
        (
            login.into(),
            role.to_owned(),
            schema.into(),
            "2s".into(),
            "250ms".into(),
            "tenant-one".into()
        )
    );
    let reset: String = batter_sqlx::run_atomic(&legacy, async |scope| {
        scope
            .application(async |sql| {
                sqlx::query_scalar("SELECT current_user::text")
                    .fetch_one(sql.executor())
                    .await
            })
            .await
    })
    .await
    .expect("legacy reset control");
    assert_eq!(reset, login);
    legacy.close().await;
}
