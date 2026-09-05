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
