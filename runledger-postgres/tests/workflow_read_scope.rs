use runledger_core::jobs::{
    StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    WorkflowRunDbRecord, WorkflowRunReadCountFilter, WorkflowRunReadListFilter,
    WorkflowRunReadScope, count_workflow_runs, count_workflow_runs_with_scope,
    count_workflow_step_dependencies_with_scope, count_workflow_steps_with_scope,
    enqueue_workflow_run, get_latest_workflow_run_by_type_with_scope, get_workflow_run_by_id,
    get_workflow_run_by_id_with_scope, list_workflow_runs, list_workflow_runs_with_scope,
    list_workflow_step_dependencies_page_with_scope, list_workflow_step_dependencies_with_scope,
    list_workflow_steps_page_with_scope, list_workflow_steps_with_scope,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;
use std::collections::BTreeSet;

const WORKFLOW_TYPE: &str = "workflow.test.read_scope";

macro_rules! assert_read_len {
    ($query:expr, $expected:expr, $message:literal) => {
        assert_eq!(($query).await.expect($message).len(), $expected, $message,);
    };
}

macro_rules! assert_read_count {
    ($query:expr, $expected:expr, $message:literal) => {
        assert_eq!(($query).await.expect($message), $expected, $message);
    };
}

async fn record_postgres_server_version(pool: &DbPool) {
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "workflow read-scope regression PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(
        server_version_num / 10_000,
        18,
        "workflow read-scope regression must run on PostgreSQL 18"
    );
}

async fn enqueue_scoped_workflow(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    label: &str,
) -> WorkflowRunDbRecord {
    let payload = json!({"scope": label});
    let metadata = json!({"scope": label});
    let root = WorkflowStepEnqueueBuilder::new_external(StepKey::new("root"), &payload)
        .try_build()
        .expect("build root external step");
    let dependent = WorkflowStepEnqueueBuilder::new_external(StepKey::new("dependent"), &payload)
        .depends_on_terminal(&[StepKey::new("root")])
        .try_build()
        .expect("build dependent external step");
    let builder = WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata);
    let builder = match organization_id {
        Some(organization_id) => builder.organization_id(organization_id),
        None => builder,
    };
    let workflow = builder
        .step(root)
        .step(dependent)
        .try_build()
        .expect("build scoped workflow");

    enqueue_workflow_run(pool, &workflow)
        .await
        .expect("enqueue scoped workflow")
}

fn run_ids(runs: Vec<WorkflowRunDbRecord>) -> BTreeSet<Uuid> {
    runs.into_iter().map(|run| run.id).collect()
}

fn list_filter(scope: WorkflowRunReadScope) -> WorkflowRunReadListFilter<'static> {
    WorkflowRunReadListFilter {
        scope,
        status: None,
        workflow_type: Some(WORKFLOW_TYPE),
        limit: 10,
        offset: 0,
    }
}

fn count_filter(scope: WorkflowRunReadScope) -> WorkflowRunReadCountFilter<'static> {
    WorkflowRunReadCountFilter {
        scope,
        status: None,
        workflow_type: Some(WORKFLOW_TYPE),
    }
}

struct WorkflowReadScopeFixture {
    global_run: WorkflowRunDbRecord,
    organization_run: WorkflowRunDbRecord,
    other_organization_run: WorkflowRunDbRecord,
    global: WorkflowRunReadScope,
    organization: WorkflowRunReadScope,
    wrong_organization: WorkflowRunReadScope,
    admin: WorkflowRunReadScope,
}

async fn setup_workflow_read_scope_fixture(pool: &DbPool) -> WorkflowReadScopeFixture {
    let organization_id = Uuid::from_u128(22_001);
    let other_organization_id = Uuid::from_u128(22_002);
    let wrong_organization_id = Uuid::from_u128(22_003);
    WorkflowReadScopeFixture {
        global_run: enqueue_scoped_workflow(pool, None, "global").await,
        organization_run: enqueue_scoped_workflow(pool, Some(organization_id), "organization")
            .await,
        other_organization_run: enqueue_scoped_workflow(
            pool,
            Some(other_organization_id),
            "other-organization",
        )
        .await,
        global: WorkflowRunReadScope::Global,
        organization: WorkflowRunReadScope::Organization(organization_id),
        wrong_organization: WorkflowRunReadScope::Organization(wrong_organization_id),
        admin: WorkflowRunReadScope::Admin,
    }
}

async fn assert_scoped_workflow_gets(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_eq!(
        get_workflow_run_by_id_with_scope(pool, global, global_run.id)
            .await
            .expect("load global run")
            .map(|run| run.id),
        Some(global_run.id),
        "global scope reads only the exact global run"
    );
    assert!(
        get_workflow_run_by_id_with_scope(pool, global, organization_run.id)
            .await
            .expect("reject organization run from global scope")
            .is_none(),
        "global scope must not read an organization run"
    );
    assert_eq!(
        get_workflow_run_by_id_with_scope(pool, organization, organization_run.id)
            .await
            .expect("load organization run")
            .map(|run| run.id),
        Some(organization_run.id),
        "organization scope reads its exact organization run"
    );
    assert!(
        get_workflow_run_by_id_with_scope(pool, wrong_organization, organization_run.id)
            .await
            .expect("reject wrong organization")
            .is_none(),
        "wrong organization scope must not read another organization run"
    );
    assert_eq!(
        get_workflow_run_by_id_with_scope(pool, admin, organization_run.id)
            .await
            .expect("admin loads organization run")
            .map(|run| run.id),
        Some(organization_run.id),
        "admin scope reads organization runs"
    );

    assert_eq!(
        get_latest_workflow_run_by_type_with_scope(pool, global, WorkflowType::new(WORKFLOW_TYPE))
            .await
            .expect("load latest global run")
            .map(|run| run.id),
        Some(global_run.id),
        "latest global read is exact"
    );
    assert_eq!(
        get_latest_workflow_run_by_type_with_scope(
            pool,
            organization,
            WorkflowType::new(WORKFLOW_TYPE),
        )
        .await
        .expect("load latest organization run")
        .map(|run| run.id),
        Some(organization_run.id),
        "latest organization read is exact"
    );
    assert!(
        get_latest_workflow_run_by_type_with_scope(
            pool,
            wrong_organization,
            WorkflowType::new(WORKFLOW_TYPE),
        )
        .await
        .expect("reject latest wrong organization")
        .is_none(),
        "wrong organization has no latest run"
    );
    assert!(
        get_latest_workflow_run_by_type_with_scope(pool, admin, WorkflowType::new(WORKFLOW_TYPE))
            .await
            .expect("admin loads a latest run")
            .is_some(),
        "admin can read across organizations"
    );
}

async fn assert_scoped_workflow_lists_and_counts(
    pool: &DbPool,
    fixture: &WorkflowReadScopeFixture,
) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_eq!(
        run_ids(
            list_workflow_runs_with_scope(pool, &list_filter(global))
                .await
                .expect("list global workflow runs"),
        ),
        BTreeSet::from([global_run.id]),
        "global list is exact"
    );
    assert_eq!(
        run_ids(
            list_workflow_runs_with_scope(pool, &list_filter(organization))
                .await
                .expect("list organization workflow runs"),
        ),
        BTreeSet::from([organization_run.id]),
        "organization list is exact"
    );
    assert_eq!(
        run_ids(
            list_workflow_runs_with_scope(pool, &list_filter(admin))
                .await
                .expect("list workflow runs as admin"),
        ),
        BTreeSet::from([
            global_run.id,
            organization_run.id,
            other_organization_run.id
        ]),
        "admin list spans global and organization runs"
    );
    assert!(
        list_workflow_runs_with_scope(pool, &list_filter(wrong_organization))
            .await
            .expect("list workflow runs for wrong organization")
            .is_empty(),
        "wrong organization list must be empty"
    );

    assert_read_count!(
        count_workflow_runs_with_scope(pool, &count_filter(global)),
        1,
        "global count is exact"
    );
    assert_read_count!(
        count_workflow_runs_with_scope(pool, &count_filter(organization)),
        1,
        "organization count is exact"
    );
    assert_read_count!(
        count_workflow_runs_with_scope(pool, &count_filter(admin)),
        3,
        "admin count spans global and organization runs"
    );
    assert_read_count!(
        count_workflow_runs_with_scope(pool, &count_filter(wrong_organization)),
        0,
        "wrong organization count is empty"
    );
}

async fn assert_scoped_workflow_step_full_lists(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_len!(
        list_workflow_steps_with_scope(pool, global, global_run.id),
        2,
        "global scope lists global steps"
    );
    assert_read_len!(
        list_workflow_steps_with_scope(pool, global, organization_run.id),
        0,
        "global scope rejects organization steps"
    );
    assert_read_len!(
        list_workflow_steps_with_scope(pool, organization, organization_run.id),
        2,
        "organization scope lists its steps"
    );
    assert_read_len!(
        list_workflow_steps_with_scope(pool, admin, organization_run.id),
        2,
        "admin scope lists organization steps"
    );
    assert_read_len!(
        list_workflow_steps_with_scope(pool, wrong_organization, organization_run.id),
        0,
        "wrong organization cannot list steps"
    );
}

async fn assert_scoped_workflow_step_pages(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_len!(
        list_workflow_steps_page_with_scope(pool, global, global_run.id, 10, 0),
        2,
        "global scope lists a global step page"
    );
    assert_read_len!(
        list_workflow_steps_page_with_scope(pool, global, organization_run.id, 10, 0),
        0,
        "global scope rejects an organization step page"
    );
    assert_read_len!(
        list_workflow_steps_page_with_scope(pool, organization, organization_run.id, 10, 0),
        2,
        "organization scope lists its step page"
    );
    assert_read_len!(
        list_workflow_steps_page_with_scope(pool, admin, organization_run.id, 10, 0),
        2,
        "admin scope lists an organization step page"
    );
    assert_read_len!(
        list_workflow_steps_page_with_scope(pool, wrong_organization, organization_run.id, 10, 0,),
        0,
        "wrong organization cannot list a step page"
    );
}

async fn assert_scoped_workflow_step_lists(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    assert_scoped_workflow_step_full_lists(pool, fixture).await;
    assert_scoped_workflow_step_pages(pool, fixture).await;
}

async fn assert_scoped_workflow_step_counts(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_count!(
        count_workflow_steps_with_scope(pool, global, global_run.id),
        2,
        "global scope counts global steps"
    );
    assert_read_count!(
        count_workflow_steps_with_scope(pool, global, organization_run.id),
        0,
        "global scope rejects organization step count"
    );
    assert_read_count!(
        count_workflow_steps_with_scope(pool, organization, organization_run.id),
        2,
        "organization scope counts its steps"
    );
    assert_read_count!(
        count_workflow_steps_with_scope(pool, admin, organization_run.id),
        2,
        "admin scope counts organization steps"
    );
    assert_read_count!(
        count_workflow_steps_with_scope(pool, wrong_organization, organization_run.id),
        0,
        "wrong organization cannot count steps"
    );
}

async fn assert_scoped_workflow_steps(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    assert_scoped_workflow_step_lists(pool, fixture).await;
    assert_scoped_workflow_step_counts(pool, fixture).await;
}

async fn assert_scoped_workflow_dependency_full_lists(
    pool: &DbPool,
    fixture: &WorkflowReadScopeFixture,
) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_len!(
        list_workflow_step_dependencies_with_scope(pool, global, global_run.id),
        1,
        "global scope lists global dependencies"
    );
    assert_read_len!(
        list_workflow_step_dependencies_with_scope(pool, global, organization_run.id),
        0,
        "global scope rejects organization dependencies"
    );
    assert_read_len!(
        list_workflow_step_dependencies_with_scope(pool, organization, organization_run.id),
        1,
        "organization scope lists its dependencies"
    );
    assert_read_len!(
        list_workflow_step_dependencies_with_scope(pool, admin, organization_run.id),
        1,
        "admin scope lists organization dependencies"
    );
    assert_read_len!(
        list_workflow_step_dependencies_with_scope(pool, wrong_organization, organization_run.id),
        0,
        "wrong organization cannot list dependencies"
    );
}

async fn assert_scoped_workflow_dependency_pages(
    pool: &DbPool,
    fixture: &WorkflowReadScopeFixture,
) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_len!(
        list_workflow_step_dependencies_page_with_scope(pool, global, global_run.id, 10, 0),
        1,
        "global scope lists a global dependency page"
    );
    assert_read_len!(
        list_workflow_step_dependencies_page_with_scope(pool, global, organization_run.id, 10, 0),
        0,
        "global scope rejects an organization dependency page"
    );
    assert_read_len!(
        list_workflow_step_dependencies_page_with_scope(
            pool,
            organization,
            organization_run.id,
            10,
            0,
        ),
        1,
        "organization scope lists its dependency page"
    );
    assert_read_len!(
        list_workflow_step_dependencies_page_with_scope(pool, admin, organization_run.id, 10, 0),
        1,
        "admin scope lists an organization dependency page"
    );
    assert_read_len!(
        list_workflow_step_dependencies_page_with_scope(
            pool,
            wrong_organization,
            organization_run.id,
            10,
            0,
        ),
        0,
        "wrong organization cannot list a dependency page"
    );
}

async fn assert_scoped_workflow_dependency_lists(
    pool: &DbPool,
    fixture: &WorkflowReadScopeFixture,
) {
    assert_scoped_workflow_dependency_full_lists(pool, fixture).await;
    assert_scoped_workflow_dependency_pages(pool, fixture).await;
}

async fn assert_scoped_workflow_dependency_counts(
    pool: &DbPool,
    fixture: &WorkflowReadScopeFixture,
) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run: _,
        global,
        organization,
        wrong_organization,
        admin,
    } = fixture;
    let (global, organization, wrong_organization, admin) =
        (*global, *organization, *wrong_organization, *admin);

    assert_read_count!(
        count_workflow_step_dependencies_with_scope(pool, global, global_run.id),
        1,
        "global scope counts global dependencies"
    );
    assert_read_count!(
        count_workflow_step_dependencies_with_scope(pool, global, organization_run.id),
        0,
        "global scope rejects organization dependency count"
    );
    assert_read_count!(
        count_workflow_step_dependencies_with_scope(pool, organization, organization_run.id),
        1,
        "organization scope counts its dependencies"
    );
    assert_read_count!(
        count_workflow_step_dependencies_with_scope(pool, admin, organization_run.id),
        1,
        "admin scope counts organization dependencies"
    );
    assert_read_count!(
        count_workflow_step_dependencies_with_scope(pool, wrong_organization, organization_run.id),
        0,
        "wrong organization cannot count dependencies"
    );
}

async fn assert_scoped_workflow_dependencies(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    assert_scoped_workflow_dependency_lists(pool, fixture).await;
    assert_scoped_workflow_dependency_counts(pool, fixture).await;
}

async fn assert_legacy_workflow_scope_wildcards(pool: &DbPool, fixture: &WorkflowReadScopeFixture) {
    let WorkflowReadScopeFixture {
        global_run,
        organization_run,
        other_organization_run,
        global: _,
        organization: _,
        wrong_organization: _,
        admin: _,
    } = fixture;

    assert!(
        get_workflow_run_by_id(pool, None, organization_run.id)
            .await
            .expect("legacy get workflow run")
            .is_some(),
        "legacy None remains an admin wildcard"
    );
    assert_eq!(
        run_ids(
            list_workflow_runs(
                pool,
                &runledger_postgres::jobs::WorkflowRunListFilter {
                    organization_id: None,
                    status: None,
                    workflow_type: Some(WORKFLOW_TYPE),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .expect("legacy list workflow runs"),
        ),
        BTreeSet::from([
            global_run.id,
            organization_run.id,
            other_organization_run.id
        ]),
        "legacy None list remains an admin wildcard"
    );
    assert_read_count!(
        count_workflow_runs(
            pool,
            &runledger_postgres::jobs::WorkflowRunCountFilter {
                organization_id: None,
                status: None,
                workflow_type: Some(WORKFLOW_TYPE),
            },
        ),
        3,
        "legacy None count remains an admin wildcard"
    );
}

#[tokio::test]
async fn workflow_read_scope_is_exact_for_get_list_and_count_apis() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_read_scope", 4).await;
    record_postgres_server_version(&pool).await;
    let fixture = setup_workflow_read_scope_fixture(&pool).await;

    assert_scoped_workflow_gets(&pool, &fixture).await;
    assert_scoped_workflow_lists_and_counts(&pool, &fixture).await;
    assert_scoped_workflow_steps(&pool, &fixture).await;
    assert_scoped_workflow_dependencies(&pool, &fixture).await;
    assert_legacy_workflow_scope_wildcards(&pool, &fixture).await;

    teardown_ephemeral_pool(pool, database).await;
}
