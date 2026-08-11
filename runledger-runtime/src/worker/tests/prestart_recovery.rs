use super::*;

#[tokio::test]
async fn process_claimed_job_aborts_when_running_progress_cannot_be_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_persist_fail", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-persistence-failure"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute once the worker can no longer persist running state"
    );

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(
        persisted.stage,
        runledger_core::jobs::JobStage::Queued,
        "job should remain queued because running state was never durably recorded"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_not_applicable_after_running_persists() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_not_applicable", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-not-applicable"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-release-not-applicable").await;
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        claimed_job
            .worker_id
            .as_deref()
            .expect("worker id is set on claimed job"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("persist running stage");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-not-applicable",
        "TEST_NOT_APPLICABLE",
        0,
    )
    .await
    .expect_err("release should no longer apply once running persists");

    assert_eq!(
        query_error_code(&error),
        Some("job.unstarted_claim_release_not_applicable")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_owner_mismatch_for_other_worker() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_owner_mismatch", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-owner-mismatch"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-release-owner-a").await;
    sqlx::query(
        "UPDATE job_queue
         SET worker_id = 'worker-release-owner-b'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("switch lease ownership");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-owner-a",
        "TEST_OWNER_MISMATCH",
        0,
    )
    .await
    .expect_err("release should fail when another worker owns the lease");

    assert_eq!(query_error_code(&error), Some("job.lease_owner_mismatch"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_requeues_unstarted_claim_when_running_progress_never_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_reaper_requeue", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-reaper-requeue"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute before the job can durably enter RUNNING"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired leases");
    assert_eq!(reaped, 1, "reaper should reclaim the stranded lease");

    let recovered = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load recovered job")
        .expect("job exists");
    assert_eq!(recovered.status, JobStatus::Pending);
    assert_eq!(
        recovered.attempt, 0,
        "reaper must return the unstarted claim without consuming an attempt"
    );

    let recovered_job = claim_one_job(&pool, "worker-persistence-retry").await;
    let runs_after_recovery = Arc::new(AtomicUsize::new(0));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(PersistenceFailureHandler {
        runs: runs_after_recovery.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    assert_eq!(
        runs_after_recovery.load(Ordering::SeqCst),
        1,
        "job should still be executable after reaper recovery"
    );

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(
        completed.attempt, 1,
        "successful execution should use the first real attempt after recovery"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_does_not_burn_retry_attempt_when_later_attempt_never_started() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_retry_attempt_not_burned", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.retry_then_success"),
            version: 1,
            max_attempts: 2,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.retry_then_success"),
            organization_id: None,
            payload: &json!({"kind":"retry-attempt-pre-run-failure"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let first_runs = Arc::new(AtomicUsize::new(0));
    let mut first_registry = JobRegistry::new();
    first_registry.register(RetryThenSuccessHandler {
        runs: first_runs.clone(),
    });

    let first_claimed_job = claim_one_job(&pool, "worker-retry-1").await;
    process_claimed_job(
        pool.clone(),
        Arc::new(first_registry),
        first_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        first_runs.load(Ordering::SeqCst),
        1,
        "first attempt should execute and fail retryably"
    );

    let after_first_attempt = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after first attempt")
        .expect("job exists");
    assert_eq!(after_first_attempt.status, JobStatus::Pending);
    assert_eq!(after_first_attempt.attempt, 1);

    sqlx::query(
        "UPDATE job_queue
         SET next_run_at = now()
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("make second attempt claimable immediately");

    let second_claimed_job = claim_one_job(&pool, "worker-retry-2").await;
    let failing_worker_pool = connect_closed_pool(&database.url).await;

    let second_runs = Arc::new(AtomicUsize::new(0));
    let mut second_registry = JobRegistry::new();
    second_registry.register(RetryThenSuccessHandler {
        runs: second_runs.clone(),
    });

    process_claimed_job(
        failing_worker_pool,
        Arc::new(second_registry),
        second_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        second_runs.load(Ordering::SeqCst),
        0,
        "second attempt must fail before handler execution"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired lease");
    assert_eq!(reaped, 1);

    let after_reap = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after reap")
        .expect("job exists");
    assert_eq!(
        after_reap.status,
        JobStatus::Pending,
        "later attempt that never started should be released back to pending"
    );
    assert_eq!(
        after_reap.attempt, 1,
        "reaper should preserve the earlier consumed attempt count"
    );

    let recovery_runs = Arc::new(AtomicUsize::new(1));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(RetryThenSuccessHandler {
        runs: recovery_runs.clone(),
    });

    let recovered_job = claim_one_job(&pool, "worker-retry-3").await;
    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed recovered job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.attempt, 2);

    teardown_ephemeral_pool(pool, database).await;
}
