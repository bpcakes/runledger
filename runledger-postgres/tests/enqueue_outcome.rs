use runledger_core::jobs::{JobEventType, JobStatus, JobType};
use runledger_postgres::jobs::{
    CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobEnqueue, JobEnqueueDisposition,
    JobRequeueStatePolicy, JobScope, RequeueableJobStatus, cancel_job, compare_and_requeue_job_tx,
    enqueue_job_tx, enqueue_job_with_outcome_tx, get_job_by_id, list_job_events,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use std::time::Duration;
use tokio::time::timeout;

mod support;

use support::register_test_job_definition;

const JOB_TYPE: &str = "jobs.test.enqueue_outcome";

#[tokio::test]
async fn transactional_enqueue_reports_inserted_or_existing_state() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_outcome", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "same-request"});
    let request = JobEnqueue {
        job_type: JobType::new(JOB_TYPE),
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("enqueue-outcome-key"),
        stage: None,
    };

    let mut insert_tx = pool.begin().await.expect("begin inserted enqueue");
    let inserted = enqueue_job_with_outcome_tx(&mut insert_tx, &request)
        .await
        .expect("insert keyed job");
    assert_eq!(inserted.status, JobStatus::Pending);
    assert_eq!(inserted.run_number, 1);
    assert_eq!(inserted.disposition, JobEnqueueDisposition::Inserted);
    insert_tx.commit().await.expect("commit inserted enqueue");

    let (stored_key, stored_request) = sqlx::query_as::<_, (Option<String>, Option<Value>)>(
        "SELECT idempotency_key, enqueue_request FROM job_queue WHERE id = $1",
    )
    .bind(inserted.job_id)
    .fetch_one(&pool)
    .await
    .expect("load keyed enqueue state");
    assert_eq!(stored_key.as_deref(), Some("enqueue-outcome-key"));
    assert_eq!(
        stored_request,
        Some(json!({
            "payload": {"target": "same-request"},
            "priority": null,
            "max_attempts": null,
            "timeout_seconds": null,
            "next_run_at": null,
            "stage": "queued"
        }))
    );

    cancel_job(&pool, None, inserted.job_id, Some("observe current state"))
        .await
        .expect("cancel inserted job");

    let mut existing_tx = pool.begin().await.expect("begin existing enqueue");
    let existing = enqueue_job_with_outcome_tx(&mut existing_tx, &request)
        .await
        .expect("resolve existing keyed job");
    assert_eq!(existing.job_id, inserted.job_id);
    assert_eq!(existing.status, JobStatus::Canceled);
    assert_eq!(existing.run_number, 1);
    assert_eq!(existing.disposition, JobEnqueueDisposition::Existing);
    existing_tx.commit().await.expect("commit existing enqueue");

    let mut compatibility_tx = pool.begin().await.expect("begin compatibility enqueue");
    let compatibility_id = enqueue_job_tx(&mut compatibility_tx, &request)
        .await
        .expect("UUID compatibility wrapper");
    assert_eq!(compatibility_id, inserted.job_id);
    compatibility_tx
        .commit()
        .await
        .expect("commit compatibility enqueue");

    let enqueue_event_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM job_events
         WHERE job_id = $1 AND event_type = 'ENQUEUED'",
    )
    .bind(inserted.job_id)
    .fetch_one(&pool)
    .await
    .expect("count enqueue events");
    assert_eq!(enqueue_event_count, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn unkeyed_transactional_enqueue_always_reports_inserted() {
    let (pool, database) = setup_ephemeral_pool("postgres_unkeyed_enqueue_outcome", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "unkeyed"});
    let request = JobEnqueue {
        job_type: JobType::new(JOB_TYPE),
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: None,
        stage: None,
    };

    let mut tx = pool.begin().await.expect("begin unkeyed enqueue");
    let first = enqueue_job_with_outcome_tx(&mut tx, &request)
        .await
        .expect("first unkeyed enqueue");
    let second = enqueue_job_with_outcome_tx(&mut tx, &request)
        .await
        .expect("second unkeyed enqueue");
    assert_ne!(first.job_id, second.job_id);
    assert_eq!(first.disposition, JobEnqueueDisposition::Inserted);
    assert_eq!(second.disposition, JobEnqueueDisposition::Inserted);
    tx.commit().await.expect("commit unkeyed enqueues");

    for job_id in [first.job_id, second.job_id] {
        let has_no_correlated_state = sqlx::query_scalar::<_, bool>(
            "SELECT idempotency_key IS NULL AND enqueue_request IS NULL
             FROM job_queue
             WHERE id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("load unkeyed enqueue state");
        assert!(has_no_correlated_state);
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn legacy_keyed_enqueue_keeps_concurrency_and_composes_with_requeue() {
    let (pool, database) = setup_ephemeral_pool("postgres_legacy_enqueue_requeue_lock", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    for (scope_name, organization_id, idempotency_key) in [
        ("global", None, "legacy-global-requeue-lock-key"),
        (
            "organization",
            Some(Uuid::from_u128(42)),
            "legacy-organization-requeue-lock-key",
        ),
    ] {
        let payload = json!({"target": "legacy-requeue-lock", "scope": scope_name});
        let request = JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(idempotency_key),
            stage: None,
        };

        let mut seed_tx = pool.begin().await.expect("begin seed enqueue");
        let job_id = enqueue_job_tx(&mut seed_tx, &request)
            .await
            .expect("seed keyed job");
        seed_tx.commit().await.expect("commit seed enqueue");
        cancel_job(
            &pool,
            organization_id,
            job_id,
            Some("prepare legacy recovery"),
        )
        .await
        .expect("cancel seeded job");

        let mut tx_a = pool.begin().await.expect("begin legacy enqueue A");
        assert_eq!(
            enqueue_job_tx(&mut tx_a, &request)
                .await
                .expect("legacy enqueue A"),
            job_id
        );

        let mut tx_b = pool.begin().await.expect("begin legacy enqueue B");
        let job_id_b = timeout(Duration::from_secs(1), enqueue_job_tx(&mut tx_b, &request))
            .await
            .expect("legacy enqueue B must coexist with A's retained keyed-row lock")
            .expect("legacy enqueue B");
        assert_eq!(job_id_b, job_id);

        // B must be able to take the mutation lock while A retains the legacy
        // keyed-enqueue lock. FOR SHARE followed by FOR UPDATE blocks here and
        // two callers attempting the same composition form an upgrade deadlock.
        let mut task_b = tokio::spawn(async move {
            let outcome = compare_and_requeue_job_tx(
                &mut tx_b,
                CompareAndRequeueJob {
                    scope: job_scope(organization_id),
                    job_id,
                    expected_status: RequeueableJobStatus::Canceled,
                    expected_run_number: 1,
                    state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                    reason: "rolled-back transaction B recovery",
                },
            )
            .await
            .map_err(|error| format!("transaction B compare-and-requeue: {error}"))?;
            if !matches!(outcome, CompareAndRequeueJobOutcome::Requeued { .. }) {
                return Err(format!("transaction B expected Requeued, got {outcome:?}"));
            }
            tx_b.rollback()
                .await
                .map_err(|error| format!("rollback transaction B: {error}"))?;
            Ok::<(), String>(())
        });
        match timeout(Duration::from_secs(2), &mut task_b).await {
            Ok(result) => result
                .expect("transaction B task must not panic")
                .expect("transaction B must requeue and roll back"),
            Err(_) => {
                task_b.abort();
                let _ = task_b.await;
                tx_a.rollback()
                    .await
                    .expect("rollback transaction A after timeout");
                teardown_ephemeral_pool(pool, database).await;
                panic!("legacy {scope_name} enqueue lock must compose with compare-and-requeue");
            }
        }

        let outcome_a = timeout(
            Duration::from_secs(2),
            compare_and_requeue_job_tx(
                &mut tx_a,
                CompareAndRequeueJob {
                    scope: job_scope(organization_id),
                    job_id,
                    expected_status: RequeueableJobStatus::Canceled,
                    expected_run_number: 1,
                    state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                    reason: "committed transaction A recovery",
                },
            ),
        )
        .await
        .expect("transaction A compare-and-requeue must not hang")
        .expect("transaction A compare-and-requeue");
        assert!(matches!(
            outcome_a,
            CompareAndRequeueJobOutcome::Requeued { .. }
        ));
        tx_a.commit().await.expect("commit transaction A recovery");

        let final_job = get_job_by_id(&pool, organization_id, job_id)
            .await
            .expect("load final job")
            .expect("final job exists");
        assert_eq!(final_job.status, JobStatus::Pending);
        assert_eq!(final_job.run_number, 2);

        let requeued_events = list_job_events(&pool, organization_id, job_id, 20, None)
            .await
            .expect("list final job events")
            .into_iter()
            .filter(|event| event.event_type == JobEventType::Requeued)
            .collect::<Vec<_>>();
        assert_eq!(requeued_events.len(), 1);
        assert_eq!(
            requeued_events[0]
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("committed transaction A recovery")
        );
    }

    teardown_ephemeral_pool(pool, database).await;
}

fn job_scope(organization_id: Option<Uuid>) -> JobScope {
    match organization_id {
        Some(organization_id) => JobScope::Organization(organization_id),
        None => JobScope::Global,
    }
}
