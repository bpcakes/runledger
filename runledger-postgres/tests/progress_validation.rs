use runledger_core::jobs::JobProgressValidationError;
use runledger_postgres::Error;
use runledger_postgres::prelude::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;

mod support;

#[tokio::test]
async fn competing_partial_progress_updates_validate_against_the_locked_row() {
    let (pool, database) = setup_ephemeral_pool("competing_progress", 4).await;
    support::register_test_job_definition(&pool, "jobs.test.progress_validation").await;
    let id =
        support::enqueue_test_job(&pool, "jobs.test.progress_validation", None, &json!({})).await;
    let job = support::claim_one_job(&pool, "progress-worker").await;
    let identity = JobLeaseIdentity::new(id, job.run_number, job.attempt, "progress-worker");
    update_job_ordinary_progress_for_lease(
        &pool,
        identity,
        &JobOrdinaryProgressUpdate {
            progress_done: Some(1),
            progress_total: Some(3),
            checkpoint: Some(&json!("initial")),
        },
    )
    .await
    .expect("initial progress");
    let done_checkpoint = json!("done");
    let total_checkpoint = json!("total");
    let done = JobOrdinaryProgressUpdate {
        progress_done: Some(3),
        progress_total: None,
        checkpoint: Some(&done_checkpoint),
    };
    let total = JobOrdinaryProgressUpdate {
        progress_done: None,
        progress_total: Some(2),
        checkpoint: Some(&total_checkpoint),
    };
    // Both are valid against the starting row; only one remains valid once the
    // other commits. The loser must return a typed validation error, not a CHECK.
    let (done_result, total_result) = tokio::join!(
        update_job_ordinary_progress_for_lease(&pool, identity, &done),
        update_job_ordinary_progress_for_lease(&pool, identity, &total),
    );
    assert_ne!(done_result.is_ok(), total_result.is_ok());
    let saved = get_job_by_id(&pool, None, id)
        .await
        .expect("read")
        .expect("job");
    let error = if done_result.is_ok() {
        assert_eq!(
            (saved.progress_done, saved.progress_total),
            (Some(3), Some(3))
        );
        assert_eq!(saved.checkpoint, Some(done_checkpoint));
        total_result.expect_err("lower total loses")
    } else {
        assert_eq!(
            (saved.progress_done, saved.progress_total),
            (Some(1), Some(2))
        );
        assert_eq!(saved.checkpoint, Some(total_checkpoint));
        done_result.expect_err("higher done loses")
    };
    let Error::QueryError(error) = error else {
        panic!("expected domain query error")
    };
    assert_eq!(error.category(), QueryErrorCategory::Validation);
    assert_eq!(error.code(), "job.invalid_progress");
    assert_eq!(
        error.progress_validation_error(),
        Some(JobProgressValidationError::DoneExceedsTotal { done: 3, total: 2 })
    );
    assert_eq!(
        error.sqlstate(),
        None,
        "validation happens before the CHECK constraint"
    );
    let events = list_job_events(&pool, None, id, 100, None)
        .await
        .expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == runledger_core::jobs::JobEventType::Progress)
            .count(),
        2
    );
    // A rejected write must release its transaction and leave the lease usable.
    update_job_ordinary_progress_for_lease(
        &pool,
        identity,
        &JobOrdinaryProgressUpdate {
            progress_done: Some(3),
            progress_total: Some(4),
            checkpoint: None,
        },
    )
    .await
    .expect("later valid update");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn progress_cannot_write_after_its_lease_expires_while_waiting_for_the_row() {
    let (pool, database) = setup_ephemeral_pool("progress_expiry_during_lock", 3).await;
    support::register_test_job_definition(&pool, "jobs.test.progress_expiry").await;
    let id = support::enqueue_test_job(&pool, "jobs.test.progress_expiry", None, &json!({})).await;
    let job = support::claim_one_job(&pool, "expiring-progress-worker").await;
    sqlx::query("UPDATE job_queue SET lease_expires_at = clock_timestamp() + interval '1 second' WHERE id = $1")
        .bind(id).execute(&pool).await.expect("short live lease");
    let mut holder = pool.begin().await.expect("holder transaction");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *holder)
        .await
        .expect("hold job row");
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        update_job_ordinary_progress_for_lease(
            &writer_pool,
            JobLeaseIdentity::new(id, job.run_number, job.attempt, "expiring-progress-worker"),
            &JobOrdinaryProgressUpdate {
                progress_done: Some(1),
                progress_total: Some(2),
                checkpoint: Some(&json!("too late")),
            },
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'
                   AND query LIKE '%FROM job_queue%')",
            )
            .fetch_one(&pool)
            .await
            .expect("observe lock waiter");
            if blocked {
                break;
            }
            assert!(!writer.is_finished(), "writer must wait on the live row");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT lease_expires_at <= clock_timestamp() FROM job_queue WHERE id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("database clock expiry");
            if expired {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("writer blocks before database lease expiry");
    holder.rollback().await.expect("release expired row");
    let error = writer
        .await
        .expect("writer joins")
        .expect_err("expired lease cannot write");
    assert!(
        matches!(error, Error::QueryError(ref query) if query.code() == "job.lease_owner_mismatch")
    );
    let saved = get_job_by_id(&pool, None, id)
        .await
        .expect("read")
        .expect("job");
    assert_eq!(saved.checkpoint, None);
    assert_eq!(saved.progress_done, None);
    assert!(
        !list_job_events(&pool, None, id, 100, None)
            .await
            .expect("events")
            .iter()
            .any(|event| event.event_type == runledger_core::jobs::JobEventType::Progress)
    );
    teardown_ephemeral_pool(pool, database).await;
}
