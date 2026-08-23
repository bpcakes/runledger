use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobEventType, JobStage, JobStatus, JobType};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobLeaseIdentity, JobOrdinaryProgressUpdate, JobQueueRecord,
    JobRunningUpdate, claim_jobs, enqueue_job, get_job_by_id, list_job_events,
    mark_job_running_for_lease, update_job_ordinary_progress, upsert_job_definition_tx,
};
#[allow(
    deprecated,
    reason = "the integration test validates the explicit stage-bearing compatibility bridge"
)]
use runledger_postgres::jobs::{
    JobProgressUpdate, update_job_progress, update_job_progress_for_lease,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.test.running_progress";

async fn record_postgres_server_version(pool: &DbPool, diagnostic: &str) {
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
        "{diagnostic} PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
    );
}

async fn register_job_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit definition transaction");
}

async fn enqueue_and_claim(pool: &DbPool, worker_id: &str) -> JobQueueRecord {
    let payload = json!({"case": "running-progress"});
    let job_id = enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed = claim_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim job")
        .pop()
        .expect("job should be claimed");
    assert_eq!(claimed.id, job_id);
    claimed
}

async fn load_job(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    get_job_by_id(pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists")
}

async fn execution_started_persisted_at(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
) -> Option<DateTime<Utc>> {
    sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "SELECT execution_started_persisted_at
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
    )
    .bind(identity.job_id)
    .bind(identity.run_number)
    .bind(identity.attempt)
    .fetch_one(pool)
    .await
    .expect("load execution-start marker")
}

async fn event_types(pool: &DbPool, job_id: Uuid) -> Vec<JobEventType> {
    list_job_events(pool, None, job_id, 10, None)
        .await
        .expect("list job events")
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

async fn fail_stage_changed_insert_for_job(pool: &DbPool, job_id: Uuid) {
    let function_sql = format!(
        "CREATE FUNCTION fail_running_stage_event_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.event_type = 'STAGE_CHANGED'::job_event_type
                AND NEW.job_id = '{job_id}'::uuid THEN
                 RAISE EXCEPTION 'injected running stage event failure';
             END IF;
             RETURN NEW;
         END;
         $$"
    );
    sqlx::query(&function_sql)
        .execute(pool)
        .await
        .expect("create running-stage failure function");
    sqlx::query(
        "CREATE TRIGGER trg_fail_running_stage_event_for_test
         BEFORE INSERT ON job_events
         FOR EACH ROW
         EXECUTE FUNCTION fail_running_stage_event_for_test()",
    )
    .execute(pool)
    .await
    .expect("create running-stage failure trigger");
}

#[tokio::test]
async fn running_transition_commits_checkpoint_progress_marker_and_audit_together() {
    let (pool, database) = setup_ephemeral_pool("postgres_running_progress_atomic", 4).await;
    record_postgres_server_version(&pool, "running transition atomic progress regression").await;
    register_job_definition(&pool).await;

    let claimed = enqueue_and_claim(&pool, "worker-running-progress").await;
    let worker_id = claimed
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let identity = JobLeaseIdentity::new(
        claimed.id,
        claimed.run_number,
        claimed.attempt,
        worker_id.as_str(),
    );
    let checkpoint = json!({"cursor": 42, "source": "running-transition"});

    mark_job_running_for_lease(
        &pool,
        identity,
        &JobRunningUpdate {
            progress_done: Some(4),
            progress_total: Some(9),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect("persist running transition with resume state");

    let persisted = load_job(&pool, claimed.id).await;
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.stage, JobStage::Running);
    assert_eq!(persisted.progress_done, Some(4));
    assert_eq!(persisted.progress_total, Some(9));
    assert_eq!(persisted.checkpoint, Some(checkpoint));
    assert!(
        execution_started_persisted_at(&pool, identity)
            .await
            .is_some(),
        "the running transition must persist the execution-start marker"
    );

    let events = list_job_events(&pool, None, claimed.id, 10, None)
        .await
        .expect("list running transition events");
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::StageChanged,
            JobEventType::Progress,
        ]
    );
    assert_eq!(events[2].stage, Some(JobStage::Running));
    assert_eq!(events[3].progress_done, Some(4));
    assert_eq!(events[3].progress_total, Some(9));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn running_transition_rolls_back_as_one_crash_safe_unit_when_audit_fails() {
    let (pool, database) = setup_ephemeral_pool("postgres_running_progress_rollback", 4).await;
    record_postgres_server_version(&pool, "running transition rollback regression").await;
    register_job_definition(&pool).await;

    let claimed = enqueue_and_claim(&pool, "worker-running-rollback").await;
    let worker_id = claimed
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let identity = JobLeaseIdentity::new(
        claimed.id,
        claimed.run_number,
        claimed.attempt,
        worker_id.as_str(),
    );
    let checkpoint = json!({"cursor": "must-not-commit"});
    fail_stage_changed_insert_for_job(&pool, claimed.id).await;

    mark_job_running_for_lease(
        &pool,
        identity,
        &JobRunningUpdate {
            progress_done: Some(1),
            progress_total: Some(2),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect_err("an audit failure must abort the running transaction");

    let persisted = load_job(&pool, claimed.id).await;
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.stage, JobStage::Queued);
    assert_eq!(persisted.progress_done, None);
    assert_eq!(persisted.progress_total, None);
    assert_eq!(persisted.checkpoint, None);
    assert_eq!(
        execution_started_persisted_at(&pool, identity).await,
        None,
        "the execution marker must roll back with the stage and resume state"
    );
    assert_eq!(
        event_types(&pool, claimed.id).await,
        vec![JobEventType::Enqueued, JobEventType::Leased],
        "no partial running or progress audit record may survive a failed transaction"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn ordinary_progress_is_stage_free_and_does_not_mark_execution_started() {
    let (pool, database) = setup_ephemeral_pool("postgres_ordinary_progress_stage_free", 4).await;
    record_postgres_server_version(&pool, "ordinary stage-free progress regression").await;
    register_job_definition(&pool).await;

    let claimed = enqueue_and_claim(&pool, "worker-ordinary-progress").await;
    let worker_id = claimed
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let identity = JobLeaseIdentity::new(
        claimed.id,
        claimed.run_number,
        claimed.attempt,
        worker_id.as_str(),
    );
    let checkpoint = json!({"cursor": 17});

    update_job_ordinary_progress(
        &pool,
        claimed.id,
        claimed.run_number,
        claimed.attempt,
        worker_id.as_str(),
        &JobOrdinaryProgressUpdate {
            progress_done: Some(2),
            progress_total: Some(5),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect("persist ordinary progress");

    let persisted = load_job(&pool, claimed.id).await;
    assert_eq!(persisted.stage, JobStage::Queued);
    assert_eq!(persisted.progress_done, Some(2));
    assert_eq!(persisted.progress_total, Some(5));
    assert_eq!(persisted.checkpoint, Some(checkpoint));
    assert_eq!(execution_started_persisted_at(&pool, identity).await, None);
    assert_eq!(
        event_types(&pool, claimed.id).await,
        vec![
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Progress,
        ]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[allow(
    deprecated,
    reason = "the test confirms the staged compatibility API retains historical stage semantics"
)]
#[tokio::test]
async fn stage_bearing_progress_compatibility_path_preserves_arbitrary_stage_writes() {
    let (pool, database) = setup_ephemeral_pool("postgres_stage_progress_compatibility", 4).await;
    record_postgres_server_version(&pool, "stage-bearing progress compatibility regression").await;
    register_job_definition(&pool).await;

    let claimed = enqueue_and_claim(&pool, "worker-stage-progress-compatibility").await;
    let worker_id = claimed
        .worker_id
        .clone()
        .expect("claimed job has worker id");

    update_job_progress(
        &pool,
        claimed.id,
        claimed.run_number,
        claimed.attempt,
        worker_id.as_str(),
        &JobProgressUpdate {
            stage: Some(JobStage::Scheduled),
            progress_done: Some(3),
            progress_total: Some(8),
            checkpoint: None,
        },
    )
    .await
    .expect("legacy stage-bearing progress remains supported during migration");

    let persisted = load_job(&pool, claimed.id).await;
    assert_eq!(persisted.stage, JobStage::Scheduled);
    assert_eq!(persisted.progress_done, Some(3));
    assert_eq!(persisted.progress_total, Some(8));
    assert_eq!(
        event_types(&pool, claimed.id).await,
        vec![
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::StageChanged,
            JobEventType::Progress,
        ]
    );

    let claimed_for_lease = enqueue_and_claim(&pool, "worker-stage-progress-for-lease").await;
    let for_lease_worker_id = claimed_for_lease
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let for_lease_identity = JobLeaseIdentity::new(
        claimed_for_lease.id,
        claimed_for_lease.run_number,
        claimed_for_lease.attempt,
        for_lease_worker_id.as_str(),
    );
    update_job_progress_for_lease(
        &pool,
        for_lease_identity,
        &JobProgressUpdate {
            stage: Some(JobStage::Running),
            progress_done: Some(1),
            progress_total: Some(1),
            checkpoint: None,
        },
    )
    .await
    .expect("legacy lease-bearing running update remains supported during migration");

    let legacy_running = load_job(&pool, claimed_for_lease.id).await;
    assert_eq!(legacy_running.stage, JobStage::Running);
    assert_eq!(legacy_running.progress_done, Some(1));
    assert_eq!(legacy_running.progress_total, Some(1));
    assert!(
        execution_started_persisted_at(&pool, for_lease_identity)
            .await
            .is_some(),
        "the legacy lease-bearing RUNNING update preserves its execution marker"
    );

    teardown_ephemeral_pool(pool, database).await;
}
