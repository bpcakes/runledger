use std::collections::BTreeSet;

use runledger_core::jobs::{JobStatus, JobType};
use runledger_postgres::jobs::JOB_LIST_PAGE_LIMIT_MAX;
use runledger_postgres::prelude::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

mod support;

const JOB_TYPE: &str = "jobs.test.read_scope";

struct Fixture {
    organization_id: Option<Uuid>,
    job_id: Uuid,
    intent_id: Uuid,
}

async fn fixtures(pool: &DbPool) -> Vec<Fixture> {
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("server version");
    let version_num: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(pool)
        .await
        .expect("server version number");
    eprintln!("job read scope: PostgreSQL {version} ({version_num})");
    assert_eq!(version_num / 10_000, 18);
    support::register_test_job_definition(pool, JOB_TYPE).await;
    let mut fixtures = Vec::new();
    for organization_id in [None, Some(Uuid::now_v7()), Some(Uuid::now_v7())] {
        let payload = json!({"organization_id": organization_id});
        let job_id = support::enqueue_test_job(pool, JOB_TYPE, organization_id, &payload).await;
        for message in ["first", "second"] {
            insert_job_log(
                pool,
                &JobLogRecordInput {
                    job_id,
                    run_number: 1,
                    attempt: None,
                    level: "INFO".into(),
                    message: message.into(),
                    payload: payload.clone(),
                },
            )
            .await
            .expect("insert log");
        }
        let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "scope-intent");
        let intent = match organization_id {
            Some(id) => intent.with_organization_id(id),
            None => intent,
        };
        let intent_id = record_job_enqueue_intent(pool, &intent)
            .await
            .expect("record intent")
            .intent_id;
        fixtures.push(Fixture {
            organization_id,
            job_id,
            intent_id,
        });
    }
    fixtures
}

fn list_filter(scope: JobReadScope) -> JobReadListFilter<'static> {
    JobReadListFilter {
        scope,
        status: None,
        job_type: None,
        limit: 100,
        offset: 0,
    }
}

async fn assert_point_reads(pool: &DbPool, scope: JobReadScope, fixture: &Fixture, visible: bool) {
    let job = get_job_by_id_with_scope(pool, scope, fixture.job_id)
        .await
        .expect("read job");
    assert_eq!(
        job.as_ref().map(|row| row.id),
        visible.then_some(fixture.job_id)
    );
    let intent = get_job_enqueue_intent_by_id_with_scope(pool, scope, fixture.intent_id)
        .await
        .expect("read intent");
    assert_eq!(
        intent.as_ref().map(|row| row.id),
        visible.then_some(fixture.intent_id)
    );
    let events = list_job_events_with_scope(pool, scope, fixture.job_id, 100, None)
        .await
        .expect("read events");
    assert_eq!(events.len(), usize::from(visible));
    assert!(events.iter().all(|row| row.job_id == fixture.job_id));
    let logs = list_job_logs_with_scope(pool, scope, fixture.job_id, 100, None)
        .await
        .expect("read logs");
    assert_eq!(logs.len(), if visible { 2 } else { 0 });
    assert!(logs.iter().all(|row| row.job_id == fixture.job_id));
}

#[tokio::test]
async fn exact_scopes_isolate_jobs_events_logs_and_intents() {
    let (pool, database) = setup_ephemeral_pool("job_read_scope", 4).await;
    let fixtures = fixtures(&pool).await;
    // Expected indices are independent of the SQL predicate and scope conversion.
    let cases = [
        (JobReadScope::Global, vec![0]),
        (
            JobReadScope::Organization(fixtures[1].organization_id.expect("tenant A")),
            vec![1],
        ),
        (
            JobReadScope::Organization(fixtures[2].organization_id.expect("tenant B")),
            vec![2],
        ),
        (JobReadScope::Organization(Uuid::now_v7()), vec![]),
        (JobReadScope::Admin, vec![0, 1, 2]),
    ];
    for (scope, visible) in cases {
        for (index, fixture) in fixtures.iter().enumerate() {
            assert_point_reads(&pool, scope, fixture, visible.contains(&index)).await;
        }
        let jobs = list_jobs_with_scope(&pool, &list_filter(scope))
            .await
            .expect("list jobs");
        assert_eq!(
            jobs.iter().map(|row| row.id).collect::<BTreeSet<_>>(),
            visible.iter().map(|&i| fixtures[i].job_id).collect()
        );
        let intents = list_job_enqueue_intents_with_scope(
            &pool,
            &JobEnqueueIntentReadListFilter::new(scope, 100, 0),
        )
        .await
        .expect("list intents");
        assert_eq!(
            intents.iter().map(|row| row.id).collect::<BTreeSet<_>>(),
            visible.iter().map(|&i| fixtures[i].intent_id).collect()
        );
        assert_point_reads(
            &pool,
            scope,
            &Fixture {
                organization_id: None,
                job_id: Uuid::now_v7(),
                intent_id: Uuid::now_v7(),
            },
            false,
        )
        .await;
    }
    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_legacy_point_reads(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    fixture: &Fixture,
    visible: bool,
) {
    assert_eq!(
        get_job_by_id(pool, organization_id, fixture.job_id)
            .await
            .expect("legacy job")
            .map(|row| row.id),
        visible.then_some(fixture.job_id)
    );
    assert_eq!(
        get_job_enqueue_intent_by_id(pool, organization_id, fixture.intent_id)
            .await
            .expect("legacy intent")
            .map(|row| row.id),
        visible.then_some(fixture.intent_id)
    );
    assert_eq!(
        list_job_events(pool, organization_id, fixture.job_id, 100, None)
            .await
            .expect("legacy events")
            .len(),
        usize::from(visible)
    );
    assert_eq!(
        list_job_logs(pool, organization_id, fixture.job_id, 100, None)
            .await
            .expect("legacy logs")
            .len(),
        if visible { 2 } else { 0 }
    );
}

#[tokio::test]
async fn legacy_none_remains_wildcard_and_some_remains_tenant_only() {
    let (pool, database) = setup_ephemeral_pool("job_legacy_read_scope", 4).await;
    let fixtures = fixtures(&pool).await;
    for (organization_id, visible) in [
        (None, vec![0, 1, 2]),
        (fixtures[1].organization_id, vec![1]),
        (fixtures[2].organization_id, vec![2]),
        (Some(Uuid::now_v7()), vec![]),
    ] {
        for (index, fixture) in fixtures.iter().enumerate() {
            assert_legacy_point_reads(&pool, organization_id, fixture, visible.contains(&index))
                .await;
        }
        let jobs = list_jobs(
            &pool,
            &JobListFilter {
                organization_id,
                status: None,
                job_type: None,
                limit: 100,
                offset: 0,
            },
        )
        .await
        .expect("legacy list");
        assert_eq!(
            jobs.iter().map(|row| row.id).collect::<BTreeSet<_>>(),
            visible.iter().map(|&i| fixtures[i].job_id).collect()
        );
        let filter = JobEnqueueIntentListFilter::new(100, 0);
        let filter = match organization_id {
            Some(id) => filter.with_organization_id(id),
            None => filter,
        };
        let intents = list_job_enqueue_intents(&pool, &filter)
            .await
            .expect("legacy intent list");
        assert_eq!(
            intents.iter().map(|row| row.id).collect::<BTreeSet<_>>(),
            visible.iter().map(|&i| fixtures[i].intent_id).collect()
        );
    }
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn scoped_lists_preserve_filters_order_and_pagination() {
    let (pool, database) = setup_ephemeral_pool("job_scope_pagination", 4).await;
    let fixtures = fixtures(&pool).await;
    for scope in [JobReadScope::Global, JobReadScope::Admin] {
        let expected: Vec<_> = if scope == JobReadScope::Global {
            vec![0]
        } else {
            vec![2, 1, 0]
        };
        for (offset, &index) in expected.iter().enumerate() {
            let jobs = list_jobs_with_scope(
                &pool,
                &JobReadListFilter {
                    status: Some(JobStatus::Pending),
                    job_type: Some("TEST.READ_SCOPE"),
                    limit: 1,
                    offset: offset as i64,
                    ..list_filter(scope)
                },
            )
            .await
            .expect("filtered job page");
            assert_eq!(
                jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
                vec![fixtures[index].job_id]
            );
            let intents = list_job_enqueue_intents_with_scope(
                &pool,
                &JobEnqueueIntentReadListFilter::new(scope, 1, offset as i64)
                    .with_status(JobEnqueueIntentStatus::Pending)
                    .with_job_type_query("TEST.READ_SCOPE"),
            )
            .await
            .expect("filtered intent page");
            assert_eq!(
                intents.iter().map(|row| row.id).collect::<Vec<_>>(),
                vec![fixtures[index].intent_id]
            );
        }
        for (status, job_type, offset) in [
            (Some(JobStatus::Succeeded), None, 0),
            (None, Some("missing-type"), 0),
            (None, None, expected.len() as i64),
        ] {
            assert!(
                list_jobs_with_scope(
                    &pool,
                    &JobReadListFilter {
                        status,
                        job_type,
                        offset,
                        ..list_filter(scope)
                    }
                )
                .await
                .expect("empty job filter")
                .is_empty()
            );
        }
        for filter in [
            JobEnqueueIntentReadListFilter::new(scope, 100, 0)
                .with_status(JobEnqueueIntentStatus::Promoted),
            JobEnqueueIntentReadListFilter::new(scope, 100, 0).with_job_type_query("missing-type"),
            JobEnqueueIntentReadListFilter::new(scope, 100, expected.len() as i64),
        ] {
            assert!(
                list_job_enqueue_intents_with_scope(&pool, &filter)
                    .await
                    .expect("empty intent filter")
                    .is_empty()
            );
        }
    }
    assert_stream_pages(&pool, &fixtures[0]).await;
    assert_invalid_pages(&pool, fixtures[0].job_id).await;
    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_stream_pages(pool: &DbPool, fixture: &Fixture) {
    let scope = JobReadScope::Global;
    let first = list_job_logs_with_scope(pool, scope, fixture.job_id, 1, None)
        .await
        .expect("first log");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].message, "first");
    let second = list_job_logs_with_scope(pool, scope, fixture.job_id, 1, Some(first[0].id))
        .await
        .expect("second log");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].message, "second");
    assert!(second[0].id > first[0].id);
    assert!(
        list_job_logs_with_scope(pool, scope, fixture.job_id, 1, Some(second[0].id))
            .await
            .expect("end of logs")
            .is_empty()
    );
    let events = list_job_events_with_scope(pool, scope, fixture.job_id, 1, None)
        .await
        .expect("first event");
    assert_eq!(events.len(), 1);
    assert!(
        list_job_events_with_scope(pool, scope, fixture.job_id, 1, Some(events[0].id))
            .await
            .expect("end of events")
            .is_empty()
    );
}

async fn assert_invalid_pages(pool: &DbPool, job_id: Uuid) {
    for scope in [
        JobReadScope::Global,
        JobReadScope::Organization(Uuid::now_v7()),
        JobReadScope::Admin,
    ] {
        for (limit, offset) in [(0, 0), (JOB_LIST_PAGE_LIMIT_MAX + 1, 0), (1, -1)] {
            assert!(
                list_jobs_with_scope(
                    pool,
                    &JobReadListFilter {
                        limit,
                        offset,
                        ..list_filter(scope)
                    }
                )
                .await
                .is_err()
            );
            assert!(
                list_job_enqueue_intents_with_scope(
                    pool,
                    &JobEnqueueIntentReadListFilter::new(scope, limit, offset),
                )
                .await
                .is_err()
            );
            if offset == 0 {
                assert!(
                    list_job_events_with_scope(pool, scope, job_id, limit, None)
                        .await
                        .is_err()
                );
                assert!(
                    list_job_logs_with_scope(pool, scope, job_id, limit, None)
                        .await
                        .is_err()
                );
            }
        }
    }
}
