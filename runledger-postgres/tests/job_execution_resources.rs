use std::time::Duration;

use runledger_core::jobs::{JobFailureKind, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobCompletionUpdate, JobContinuationUpdate, JobDefinitionUpsert, JobEnqueue, JobFailureUpdate,
    JobLeaseIdentity, cancel_job, claim_jobs, claim_jobs_for_types, claim_prestart_jobs,
    complete_job_continuation, complete_job_failure, complete_job_success, enqueue_job,
    enqueue_job_with_execution_resource, get_job_by_id, heartbeat_job, reap_expired_leases,
    reap_expired_leases_with_diagnostics, release_unstarted_job_claim, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::task::JoinSet;

const JOB_TYPE: &str = "jobs.test.execution_resource";
const FILTERED_JOB_TYPE: &str = "jobs.test.execution_resource.filtered";
const RESOURCE: &str = "provider-account:one";

async fn register_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    for job_type in [JOB_TYPE, FILTERED_JOB_TYPE] {
        upsert_job_definition_tx(
            &mut tx,
            &JobDefinitionUpsert {
                job_type: JobType::new(job_type),
                version: 1,
                max_attempts: 3,
                default_timeout_seconds: 60,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert definition");
    }
    tx.commit().await.expect("commit definition transaction");
}

async fn enqueue_resource_job(pool: &DbPool, sequence: usize) -> Uuid {
    enqueue_resource_job_with(pool, sequence, None, RESOURCE, None).await
}

async fn enqueue_resource_job_with(
    pool: &DbPool,
    sequence: usize,
    organization_id: Option<Uuid>,
    resource: &str,
    priority: Option<i32>,
) -> Uuid {
    let payload = json!({"sequence": sequence});
    enqueue_job_with_execution_resource(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id,
            payload: &payload,
            priority,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
        resource,
    )
    .await
    .expect("enqueue resource job")
    .job_id
}

async fn enqueue_typed_resource_job(
    pool: &DbPool,
    job_type: &str,
    sequence: usize,
    resource: &str,
    priority: i32,
) -> Uuid {
    let payload = json!({"sequence": sequence});
    enqueue_job_with_execution_resource(
        pool,
        &JobEnqueue {
            job_type: JobType::new(job_type),
            organization_id: None,
            payload: &payload,
            priority: Some(priority),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
        resource,
    )
    .await
    .expect("enqueue typed resource job")
    .job_id
}

async fn claim_count(pool: &DbPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM job_execution_resource_claims")
        .fetch_one(pool)
        .await
        .expect("count execution resource claims")
}

#[tokio::test]
async fn database_rejects_whitespace_only_execution_resource_keys() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_whitespace", 4).await;
    register_definition(&pool).await;
    let payload = json!({"sequence": 1});
    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue unconstrained job");

    for whitespace in ["\t", "\u{00a0}"] {
        let error = sqlx::query(
            "UPDATE job_queue
             SET execution_resource_key = $2
             WHERE id = $1",
        )
        .bind(job_id)
        .bind(whitespace)
        .execute(&pool)
        .await
        .expect_err("database must reject whitespace-only resource keys");
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("chk_job_queue_execution_resource_key")
        );
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn database_rejects_a_resource_lease_without_its_durable_claim() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_execution_resource_legacy_lease_fence", 4).await;
    register_definition(&pool).await;
    let job_id = enqueue_resource_job_with(&pool, 1, None, RESOURCE, Some(100)).await;

    let error = sqlx::query(
        "UPDATE job_queue
         SET status = 'LEASED',
             attempt = attempt + 1,
             worker_id = 'legacy-worker',
             lease_expires_at = clock_timestamp() + interval '30 seconds',
             last_heartbeat_at = clock_timestamp()
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect_err("legacy-style resource lease must fail without a durable claim");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("chk_job_queue_execution_resource_claim_required")
    );

    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load fenced resource job")
        .expect("resource job exists");
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.attempt, 0);

    let claim = claim_jobs(&pool, "current-worker", 30, 1)
        .await
        .expect("current claim path creates the durable claim")
        .pop()
        .expect("resource job remains claimable");
    assert_eq!(claim.id, job_id);
    assert_eq!(claim_count(&pool).await, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn one_of_one_hundred_concurrent_claimers_owns_the_resource() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_race", 32).await;
    register_definition(&pool).await;
    let mut job_ids = Vec::new();
    for sequence in 0..100 {
        job_ids.push(enqueue_resource_job(&pool, sequence).await);
    }

    let mut claimers = JoinSet::new();
    for sequence in 0..100 {
        let pool = pool.clone();
        claimers.spawn(async move {
            claim_jobs(&pool, &format!("worker-{sequence}"), 30, 1)
                .await
                .expect("concurrent claim")
        });
    }
    let mut claims = Vec::new();
    while let Some(result) = claimers.join_next().await {
        claims.extend(result.expect("claim task"));
    }

    assert_eq!(claims.len(), 1);
    assert_eq!(claim_count(&pool).await, 1);
    let owner = claims.pop().expect("one owner");
    heartbeat_job(
        &pool,
        owner.id,
        owner.run_number,
        owner.attempt,
        owner.worker_id.as_deref().expect("worker id"),
        60,
    )
    .await
    .expect("extend owner lease");
    let extended = get_job_by_id(&pool, None, owner.id)
        .await
        .expect("load extended owner")
        .expect("owner exists");
    let persisted_owner =
        sqlx::query_as::<_, (Uuid, i32, i32, String, chrono::DateTime<chrono::Utc>)>(
            "SELECT job_id, run_number, attempt, worker_id, lease_expires_at
         FROM job_execution_resource_claims
         WHERE resource_key = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .expect("load execution resource owner");
    assert_eq!(
        persisted_owner,
        (
            owner.id,
            owner.run_number,
            owner.attempt,
            owner.worker_id.clone().expect("worker id"),
            extended.lease_expires_at.expect("extended lease")
        )
    );

    let blocked_attempts =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM job_queue WHERE attempt <> 0")
            .fetch_one(&pool)
            .await
            .expect("count attempted jobs");
    assert_eq!(blocked_attempts, 1);

    let free_payload = json!({"free": true});
    let free_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &free_payload,
            priority: Some(-1),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue unconstrained job");
    let free_claim = claim_jobs(&pool, "worker-free", 30, 1)
        .await
        .expect("claim behind blocked work")
        .pop()
        .expect("unconstrained job should use the worker slot");
    assert_eq!(free_claim.id, free_job_id);

    complete_job_success(
        &pool,
        owner.id,
        owner.run_number,
        owner.attempt,
        owner.worker_id.as_deref().expect("owner worker"),
        Some(&JobCompletionUpdate {
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            output: None,
        }),
    )
    .await
    .expect("complete owner");
    assert_eq!(claim_count(&pool).await, 0);
    assert!(
        !claim_jobs(&pool, "worker-next", 30, 1)
            .await
            .expect("claim next resource job")
            .is_empty()
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn duplicate_resource_keys_do_not_underfill_a_mixed_claim_batch() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_batch", 8).await;
    register_definition(&pool).await;

    let duplicate_ids = [
        enqueue_resource_job_with(&pool, 1, None, "resource:a", Some(300)).await,
        enqueue_resource_job_with(&pool, 2, None, "resource:a", Some(300)).await,
        enqueue_resource_job_with(&pool, 3, None, "resource:a", Some(300)).await,
    ];
    let resource_b = enqueue_resource_job_with(&pool, 4, None, "resource:b", Some(200)).await;
    let resource_c = enqueue_resource_job_with(&pool, 5, None, "resource:c", Some(100)).await;

    let claims = claim_jobs_for_types(&pool, "worker-batch", 30, 3, &[JobType::new(JOB_TYPE)])
        .await
        .expect("claim mixed resource batch");
    let claimed_ids = claims.iter().map(|job| job.id).collect::<Vec<_>>();

    assert_eq!(claims.len(), 3);
    assert!(claimed_ids.contains(&resource_b));
    assert!(claimed_ids.contains(&resource_c));
    assert_eq!(
        duplicate_ids
            .iter()
            .filter(|job_id| claimed_ids.contains(job_id))
            .count(),
        1
    );
    assert_eq!(claim_count(&pool).await, 3);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn a_claimed_dense_key_does_not_starve_an_unrelated_resource() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_execution_resource_dense_claimed_key", 8).await;
    register_definition(&pool).await;

    let _ = enqueue_resource_job_with(&pool, 0, None, "resource:dense", Some(300)).await;
    let dense_owner = claim_jobs(&pool, "worker-dense-owner", 30, 1)
        .await
        .expect("claim dense resource owner")
        .pop()
        .expect("dense owner");

    sqlx::query(
        "INSERT INTO job_queue (
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            execution_resource_key
         )
         SELECT
            $1,
            jsonb_build_object('dense_sequence', sequence),
            300,
            3,
            60,
            'resource:dense'
         FROM generate_series(1, 1024) AS sequence",
    )
    .bind(JOB_TYPE)
    .execute(&pool)
    .await
    .expect("insert a full minimum resource-head window for the claimed key");
    let unrelated_id =
        enqueue_resource_job_with(&pool, 2_000, None, "resource:unrelated", Some(100)).await;

    let unrelated = claim_jobs(&pool, "worker-unrelated", 30, 1)
        .await
        .expect("claimed keys must not consume the resource-head window")
        .pop()
        .expect("unrelated resource must remain claimable");
    assert_eq!(unrelated.id, unrelated_id);
    assert_ne!(unrelated.id, dense_owner.id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn filtered_claimers_acquire_resource_keys_in_one_deadlock_free_order() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_lock_order", 12).await;
    register_definition(&pool).await;

    for round in 0..10 {
        let resource_a = format!("resource:lock-order:{round}:a");
        let resource_b = format!("resource:lock-order:{round}:b");
        enqueue_typed_resource_job(&pool, JOB_TYPE, round * 4, &resource_a, 400).await;
        enqueue_typed_resource_job(&pool, JOB_TYPE, round * 4 + 1, &resource_b, 100).await;
        enqueue_typed_resource_job(&pool, FILTERED_JOB_TYPE, round * 4 + 2, &resource_a, 100).await;
        enqueue_typed_resource_job(&pool, FILTERED_JOB_TYPE, round * 4 + 3, &resource_b, 400).await;

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first_pool = pool.clone();
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            claim_jobs_for_types(
                &first_pool,
                &format!("worker-lock-order-{round}-a"),
                30,
                2,
                &[JobType::new(JOB_TYPE)],
            )
            .await
        });
        let second_pool = pool.clone();
        let second_barrier = barrier.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            claim_jobs_for_types(
                &second_pool,
                &format!("worker-lock-order-{round}-b"),
                30,
                2,
                &[JobType::new(FILTERED_JOB_TYPE)],
            )
            .await
        });

        let first_claims = first
            .await
            .expect("first filtered claimer task")
            .expect("first filtered claimer must not deadlock");
        let second_claims = second
            .await
            .expect("second filtered claimer task")
            .expect("second filtered claimer must not deadlock");
        assert_eq!(first_claims.len() + second_claims.len(), 2);
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn keyed_enqueue_rejects_a_changed_execution_resource() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_idempotency", 4).await;
    register_definition(&pool).await;
    let payload = json!({"request": "stable"});
    let enqueue = JobEnqueue {
        job_type: JobType::new(JOB_TYPE),
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("resource-idempotency"),
        stage: None,
    };
    enqueue_job_with_execution_resource(&pool, &enqueue, "resource:original")
        .await
        .expect("enqueue original resource request");

    let error = enqueue_job_with_execution_resource(&pool, &enqueue, "resource:changed")
        .await
        .expect_err("changed resource key must conflict with keyed enqueue");
    let Error::QueryError(error) = error else {
        panic!("expected resource idempotency query error");
    };
    assert_eq!(error.code(), "job.idempotency_conflict");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn identical_resource_keys_coordinate_globally_across_organizations() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_global", 8).await;
    register_definition(&pool).await;

    let organization_a = Uuid::from_u128(1);
    let organization_b = Uuid::from_u128(2);
    let first_id = enqueue_resource_job_with(&pool, 1, Some(organization_a), RESOURCE, None).await;
    let second_id = enqueue_resource_job_with(&pool, 2, Some(organization_b), RESOURCE, None).await;

    let first_claims = claim_jobs(&pool, "worker-global-one", 30, 2)
        .await
        .expect("claim globally coordinated jobs");
    assert_eq!(first_claims.len(), 1);
    assert!([first_id, second_id].contains(&first_claims[0].id));
    assert_eq!(claim_count(&pool).await, 1);

    let first = &first_claims[0];
    complete_job_success(
        &pool,
        first.id,
        first.run_number,
        first.attempt,
        first.worker_id.as_deref().expect("worker id"),
        None,
    )
    .await
    .expect("release globally coordinated resource");

    let second_claims = claim_jobs(&pool, "worker-global-two", 30, 2)
        .await
        .expect("claim second globally coordinated job");
    assert_eq!(second_claims.len(), 1);
    assert_ne!(second_claims[0].id, first.id);
    assert!([first_id, second_id].contains(&second_claims[0].id));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn an_expired_lease_cannot_leave_its_execution_resource_blocked() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_expired", 8).await;
    register_definition(&pool).await;
    let first_id = enqueue_resource_job(&pool, 1).await;
    let second_id = enqueue_resource_job(&pool, 2).await;

    let first = claim_jobs(&pool, "worker-expired-resource", 30, 1)
        .await
        .expect("claim first resource owner")
        .pop()
        .expect("first resource owner");
    assert!([first_id, second_id].contains(&first.id));
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() - interval '1 millisecond'
         WHERE id = $1",
    )
    .bind(first.id)
    .execute(&pool)
    .await
    .expect("expire resource owner lease");
    assert_eq!(
        reap_expired_leases(&pool, 10, 1)
            .await
            .expect("reap expired resource owner"),
        1
    );
    assert_eq!(claim_count(&pool).await, 0);

    let replacement = claim_jobs(&pool, "worker-replacement-resource", 30, 1)
        .await
        .expect("claim after resource lease expiry")
        .pop()
        .expect("replacement resource owner");
    assert!([first_id, second_id].contains(&replacement.id));
    assert_eq!(claim_count(&pool).await, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn bounded_reaper_does_not_release_a_resource_before_its_owner_is_reaped() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_execution_resource_bounded_reaper", 8).await;
    register_definition(&pool).await;
    let first_resource_id = enqueue_resource_job_with(&pool, 1, None, RESOURCE, Some(200)).await;
    let second_resource_id = enqueue_resource_job_with(&pool, 2, None, RESOURCE, Some(100)).await;
    let free_payload = json!({"free": true});
    let free_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &free_payload,
            priority: Some(300),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue unconstrained reaper-head job");

    let claims = claim_jobs(&pool, "worker-bounded-reaper", 30, 2)
        .await
        .expect("claim resource owner and unconstrained job");
    assert_eq!(claims.len(), 2);
    assert!(claims.iter().any(|job| job.id == first_resource_id));
    assert!(claims.iter().any(|job| job.id == free_job_id));

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = CASE
             WHEN id = $1 THEN clock_timestamp() - interval '2 seconds'
             ELSE clock_timestamp() - interval '1 second'
         END
         WHERE id = ANY($2::uuid[])",
    )
    .bind(free_job_id)
    .bind(&[free_job_id, first_resource_id][..])
    .execute(&pool)
    .await
    .expect("expire both leases in deterministic order");

    let first_reap = reap_expired_leases_with_diagnostics(&pool, 1, 1)
        .await
        .expect("reap only the unconstrained head");
    assert_eq!(first_reap.summary.processed, 1);
    assert_eq!(first_reap.execution_resource_claims_released, 0);
    assert!(first_reap.cleanup_errors.is_empty());
    sqlx::query(
        "UPDATE job_queue
         SET next_run_at = clock_timestamp() + interval '1 hour'
         WHERE id = $1
           AND status = 'PENDING'",
    )
    .bind(free_job_id)
    .execute(&pool)
    .await
    .expect("keep the reaped unconstrained job out of the resource assertion");
    assert_eq!(claim_count(&pool).await, 1);
    assert!(
        claim_jobs(&pool, "worker-still-blocked", 30, 1)
            .await
            .expect("poll while resource owner awaits reaping")
            .is_empty()
    );

    let second_reap = reap_expired_leases_with_diagnostics(&pool, 1, 1)
        .await
        .expect("reap the resource owner");
    assert_eq!(second_reap.summary.processed, 1);
    assert!(second_reap.cleanup_errors.is_empty());
    let next = claim_jobs(&pool, "worker-after-owner-reap", 30, 1)
        .await
        .expect("claim resource successor after owner reaping")
        .pop()
        .expect("resource successor should become claimable");
    assert!([first_resource_id, second_resource_id].contains(&next.id));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn every_non_cancellation_lease_exit_releases_the_exact_resource_owner() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_release", 8).await;
    register_definition(&pool).await;

    let _ = enqueue_resource_job(&pool, 1).await;
    let released = claim_prestart_jobs(&pool, "worker-release", 30, 1)
        .await
        .expect("claim for release")
        .pop()
        .expect("claim");
    release_unstarted_job_claim(
        &pool,
        JobLeaseIdentity::new(
            released.id,
            released.run_number,
            released.attempt,
            released.worker_id.as_deref().expect("worker"),
        ),
        "worker shutdown",
        1,
    )
    .await
    .expect("release unstarted claim");
    assert_eq!(claim_count(&pool).await, 0);

    let continued = claim_jobs(&pool, "worker-continuation", 30, 1)
        .await
        .expect("claim for continuation")
        .pop()
        .expect("claim");
    complete_job_continuation(
        &pool,
        continued.id,
        continued.run_number,
        continued.attempt,
        continued.worker_id.as_deref().expect("worker"),
        &JobContinuationUpdate {
            delay: Duration::ZERO,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("continue job");
    assert_eq!(claim_count(&pool).await, 0);

    let failed = claim_jobs(&pool, "worker-failure", 30, 1)
        .await
        .expect("claim for failure")
        .pop()
        .expect("claim");
    complete_job_failure(
        &pool,
        failed.id,
        failed.run_number,
        failed.attempt,
        failed.worker_id.as_deref().expect("worker"),
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "provider.retryable",
            "retry",
            Some(1),
        ),
    )
    .await
    .expect("fail job");
    assert_eq!(claim_count(&pool).await, 0);

    sqlx::query("UPDATE job_queue SET next_run_at = statement_timestamp() WHERE id = $1")
        .bind(failed.id)
        .execute(&pool)
        .await
        .expect("make retry due");
    let reaped = claim_jobs(&pool, "worker-reaper", 30, 1)
        .await
        .expect("claim for reaper")
        .pop()
        .expect("claim");
    sqlx::query(
        "UPDATE job_attempts
         SET execution_started_persisted_at = statement_timestamp()
         WHERE job_id = $1 AND run_number = $2 AND attempt = $3",
    )
    .bind(reaped.id)
    .bind(reaped.run_number)
    .bind(reaped.attempt)
    .execute(&pool)
    .await
    .expect("mark execution started");
    sqlx::query("UPDATE job_queue SET lease_expires_at = statement_timestamp() - interval '1 ms' WHERE id = $1")
        .bind(reaped.id)
        .execute(&pool)
        .await
        .expect("expire lease");
    assert_eq!(
        reap_expired_leases(&pool, 10, 1)
            .await
            .expect("reap expired lease"),
        1
    );
    assert_eq!(claim_count(&pool).await, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn live_cancellation_holds_resource_until_the_lease_is_quiescent() {
    let (pool, database) = setup_ephemeral_pool("postgres_execution_resource_cancel", 8).await;
    register_definition(&pool).await;
    let _ = enqueue_resource_job(&pool, 1).await;
    let claim = claim_jobs(&pool, "worker-cancel", 30, 1)
        .await
        .expect("claim for cancel")
        .pop()
        .expect("claim");

    let canceled = cancel_job(&pool, None, claim.id, Some("operator cancellation"))
        .await
        .expect("cancel live job");
    assert_eq!(canceled.status, JobStatus::Canceled);
    assert_eq!(claim_count(&pool).await, 1);
    let release_after = sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
        "SELECT release_after
         FROM job_execution_resource_claims
         WHERE resource_key = $1",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await
    .expect("load delayed release");
    assert_eq!(release_after, claim.lease_expires_at);

    let _ = enqueue_resource_job(&pool, 2).await;
    assert!(
        claim_jobs(&pool, "worker-blocked", 30, 1)
            .await
            .expect("blocked claim")
            .is_empty()
    );
    sqlx::query(
        "UPDATE job_execution_resource_claims
         SET release_after = statement_timestamp() - interval '1 ms'
         WHERE resource_key = $1",
    )
    .bind(RESOURCE)
    .execute(&pool)
    .await
    .expect("simulate quiescence");
    assert_eq!(
        reap_expired_leases(&pool, 10, 1)
            .await
            .expect("release quiesced cancellation"),
        0
    );
    assert_eq!(claim_count(&pool).await, 0);
    assert_eq!(
        claim_jobs(&pool, "worker-after-cancel", 30, 1)
            .await
            .expect("claim after quiescence")
            .len(),
        1
    );

    let pending = get_job_by_id(&pool, None, claim.id)
        .await
        .expect("load canceled job")
        .expect("canceled job exists");
    assert_eq!(pending.status, JobStatus::Canceled);

    teardown_ephemeral_pool(pool, database).await;
}
