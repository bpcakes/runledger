use runledger_core::jobs::{
    JobEventType, JobStage, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::jobs::{
    CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome, DecodedJobEventPayload,
    JobCompletionUpdate, JobEnqueue, JobEnqueueDisposition, JobPayloadUuidArrayFieldUpdate,
    JobScope, claim_jobs, compare_and_replay_succeeded_job, compare_and_replay_succeeded_job_tx,
    complete_job_success, enqueue_job, enqueue_job_with_execution_resource, enqueue_workflow_run,
    get_job_by_id, list_job_events, list_workflow_steps, update_job_payload_uuid_array_field,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::{Row, types::Uuid};
use tokio::sync::Barrier;

mod support;

use support::{claim_one_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.succeeded_replay";
const EXECUTION_RESOURCE: &str = "provider-account:successful-replay";

async fn enqueue_keyed_source(
    pool: &DbPool,
    organization_id: Uuid,
    payload: &Value,
    idempotency_key: &str,
) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: Some(organization_id),
            payload,
            priority: Some(77),
            max_attempts: Some(5),
            timeout_seconds: Some(123),
            next_run_at: None,
            idempotency_key: Some(idempotency_key),
            stage: Some(JobStage::Queued),
        },
    )
    .await
    .expect("enqueue keyed replay source")
}

async fn complete_source_with_result(
    pool: &DbPool,
    source_job_id: Uuid,
    checkpoint: &Value,
    output: &Value,
) {
    let claim = claim_one_job(pool, "worker-successful-replay-source").await;
    assert_eq!(claim.id, source_job_id);
    complete_job_success(
        pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("claim has worker id"),
        Some(&JobCompletionUpdate {
            progress_done: Some(8),
            progress_total: Some(8),
            checkpoint: Some(checkpoint),
            output: Some(output),
        }),
    )
    .await
    .expect("complete replay source");
}

fn replay_request<'a>(
    organization_id: Uuid,
    source_job_id: Uuid,
    replay_request_key: &'a str,
    reason: &'a str,
) -> CompareAndReplaySucceededJob<'a> {
    CompareAndReplaySucceededJob {
        scope: JobScope::Organization(organization_id),
        source_job_id,
        expected_run_number: 1,
        replay_request_key,
        reason,
    }
}

#[tokio::test]
async fn successful_replay_preserves_source_and_idempotently_creates_fresh_job() {
    let (pool, database) = setup_ephemeral_pool("postgres_successful_job_replay", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(501);
    let payload = json!({"customer_id": "customer-501", "operation": "sync"});
    let checkpoint = json!({"cursor": 8});
    let output = json!({"artifact_id": "artifact-501"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-501").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let first = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-501",
            "operator requested a fresh execution",
        ),
    )
    .await
    .expect("create successful-job replay");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        source_job_id: observed_source_id,
        source_run_number,
        replay: first_replay,
    } = first
    else {
        panic!("expected replay outcome");
    };
    assert_eq!(observed_source_id, source_job_id);
    assert_eq!(source_run_number, 1);
    assert_eq!(first_replay.disposition, JobEnqueueDisposition::Inserted);
    assert_ne!(first_replay.job_id, source_job_id);
    assert_eq!(first_replay.status, JobStatus::Pending);
    assert_eq!(first_replay.run_number, 1);

    let source = get_job_by_id(&pool, Some(organization_id), source_job_id)
        .await
        .expect("load successful source")
        .expect("source exists");
    assert_eq!(source.status, JobStatus::Succeeded);
    assert_eq!(source.run_number, 1);
    assert_eq!(source.progress_done, Some(8));
    assert_eq!(source.progress_total, Some(8));
    assert_eq!(source.checkpoint, Some(checkpoint.clone()));
    assert_eq!(source.output, Some(output.clone()));
    assert_eq!(
        source.idempotency_key.as_deref(),
        Some("original-operation-501")
    );

    let replay = get_job_by_id(&pool, Some(organization_id), first_replay.job_id)
        .await
        .expect("load replay job")
        .expect("replay exists");
    assert_eq!(replay.status, JobStatus::Pending);
    assert_eq!(replay.run_number, 1);
    assert_eq!(replay.attempt, 0);
    assert_eq!(replay.stage, JobStage::Queued);
    assert_eq!(replay.payload, payload);
    assert_eq!(replay.priority, 77);
    assert_eq!(replay.max_attempts, 5);
    assert_eq!(replay.timeout_seconds, 123);
    assert!(replay.progress_done.is_none());
    assert!(replay.progress_total.is_none());
    assert!(replay.checkpoint.is_none());
    assert!(replay.output.is_none());
    assert!(replay.idempotency_key.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, Option<Value>>(
            "SELECT enqueue_request FROM job_queue WHERE id = $1",
        )
        .bind(replay.id)
        .fetch_one(&pool)
        .await
        .expect("load replay enqueue snapshot"),
        None,
        "fresh unkeyed replay must keep ordinary unkeyed snapshot semantics"
    );

    let related_id = Uuid::from_u128(50_100);
    let payload_update = update_job_payload_uuid_array_field(
        &pool,
        organization_id,
        replay.id,
        JobType::new(JOB_TYPE),
        "related_ids",
        &[related_id],
    )
    .await
    .expect("update fresh replay payload");
    assert_eq!(payload_update, JobPayloadUuidArrayFieldUpdate::Updated);
    let replay_after_payload_update = get_job_by_id(&pool, Some(organization_id), replay.id)
        .await
        .expect("reload replay after payload update")
        .expect("replay exists after payload update");
    assert_eq!(
        replay_after_payload_update.payload.get("related_ids"),
        Some(&json!([related_id]))
    );

    let replay_events = list_job_events(&pool, Some(organization_id), replay.id, 10, None)
        .await
        .expect("list replay events");
    assert_eq!(replay_events.len(), 1);
    assert_eq!(replay_events[0].event_type, JobEventType::Enqueued);
    assert_eq!(
        replay_events[0].payload,
        json!({
            "job_type": JOB_TYPE,
            "replayed_from_job_id": source_job_id,
            "replayed_from_run_number": 1,
            "replay_request_key": "operator-action-501",
            "reason": "operator requested a fresh execution"
        })
    );
    match replay_events[0].decoded_payload() {
        DecodedJobEventPayload::SuccessfulReplayEnqueued(payload) => {
            assert_eq!(payload.replayed_from_job_id, source_job_id);
            assert_eq!(payload.replayed_from_run_number, 1);
            assert_eq!(payload.replay_request_key, "operator-action-501");
            assert_eq!(payload.reason, "operator requested a fresh execution");
        }
        payload => panic!("expected decoded successful-replay payload, got {payload:?}"),
    }

    let stored = sqlx::query(
        "SELECT source_job_id, source_run_number, replay_request_key, reason
         FROM job_replays
         WHERE replay_job_id = $1",
    )
    .bind(replay.id)
    .fetch_one(&pool)
    .await
    .expect("load replay lineage");
    assert_eq!(stored.get::<Uuid, _>("source_job_id"), source_job_id);
    assert_eq!(stored.get::<i32, _>("source_run_number"), 1);
    assert_eq!(
        stored.get::<String, _>("replay_request_key"),
        "operator-action-501"
    );

    let repeated = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-501",
            "operator requested a fresh execution",
        ),
    )
    .await
    .expect("retry successful-job replay");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: repeated_replay,
        ..
    } = repeated
    else {
        panic!("expected existing replay outcome");
    };
    assert_eq!(repeated_replay.job_id, first_replay.job_id);
    assert_eq!(repeated_replay.disposition, JobEnqueueDisposition::Existing);

    let conflict = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-501",
            "changed reason under same replay key",
        ),
    )
    .await
    .expect_err("changed replay request must conflict");
    let Error::QueryError(conflict) = conflict else {
        panic!("expected classified replay conflict");
    };
    assert_eq!(conflict.category(), QueryErrorCategory::Conflict);
    assert_eq!(conflict.code(), "job.replay_idempotency_conflict");

    let original_retry =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-501").await;
    assert_eq!(original_retry, source_job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn successful_replay_preserves_the_source_execution_resource() {
    let (pool, database) = setup_ephemeral_pool("postgres_resource_successful_replay", 6).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(502);
    let source_payload = json!({"operation": "resource-source"});
    let source_job_id = enqueue_job_with_execution_resource(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: Some(organization_id),
            payload: &source_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
        EXECUTION_RESOURCE,
    )
    .await
    .expect("enqueue resource-constrained replay source")
    .job_id;
    complete_source_with_result(&pool, source_job_id, &json!({}), &json!({})).await;

    let owner_payload = json!({"operation": "resource-owner"});
    let owner_job_id = enqueue_job_with_execution_resource(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: Some(organization_id),
            payload: &owner_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
        EXECUTION_RESOURCE,
    )
    .await
    .expect("enqueue current resource owner")
    .job_id;
    let owner = claim_one_job(&pool, "worker-successful-replay-resource-owner").await;
    assert_eq!(owner.id, owner_job_id);

    let replay = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "resource-replay",
            "replay with the original resource fence",
        ),
    )
    .await
    .expect("replay resource-constrained source");
    let CompareAndReplaySucceededJobOutcome::Replayed { replay, .. } = replay else {
        panic!("expected replay outcome");
    };
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT execution_resource_key FROM job_queue WHERE id = $1",
        )
        .bind(replay.job_id)
        .fetch_one(&pool)
        .await
        .expect("load replay resource key")
        .as_deref(),
        Some(EXECUTION_RESOURCE)
    );
    assert!(
        claim_jobs(&pool, "worker-blocked-resource-replay", 30, 1)
            .await
            .expect("attempt claim while resource is owned")
            .is_empty()
    );

    complete_job_success(
        &pool,
        owner.id,
        owner.run_number,
        owner.attempt,
        owner.worker_id.as_deref().expect("owner worker id"),
        None,
    )
    .await
    .expect("complete current resource owner");
    let claimed_replay = claim_one_job(&pool, "worker-resource-replay").await;
    assert_eq!(claimed_replay.id, replay.job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn replay_retention_preserves_idempotency_until_the_source_is_deleted() {
    let (pool, database) = setup_ephemeral_pool("postgres_successful_replay_retention", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(505);
    let payload = json!({"customer_id": "customer-505"});
    let checkpoint = json!({"cursor": 1});
    let output = json!({"artifact_id": "artifact-505"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-505").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let first = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "retention-safe-replay",
            "retain replay idempotency",
        ),
    )
    .await
    .expect("create replay for retention test");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: first_replay,
        ..
    } = first
    else {
        panic!("expected replay outcome");
    };

    let replay_claim = claim_one_job(&pool, "worker-successful-replay-retention").await;
    assert_eq!(replay_claim.id, first_replay.job_id);
    complete_job_success(
        &pool,
        replay_claim.id,
        replay_claim.run_number,
        replay_claim.attempt,
        replay_claim
            .worker_id
            .as_deref()
            .expect("replay claim has worker id"),
        None,
    )
    .await
    .expect("complete replay before retention");

    let delete_error = sqlx::query("DELETE FROM job_queue WHERE id = $1")
        .bind(first_replay.job_id)
        .execute(&pool)
        .await
        .expect_err("replay-only retention must preserve the idempotency lineage");
    assert_eq!(
        delete_error
            .as_database_error()
            .and_then(|database_error| database_error.constraint()),
        Some("fk_job_replays_replay_job")
    );

    let repeated = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "retention-safe-replay",
            "retain replay idempotency",
        ),
    )
    .await
    .expect("retry retained replay request");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: repeated_replay,
        ..
    } = repeated
    else {
        panic!("expected retained replay outcome");
    };
    assert_eq!(repeated_replay.job_id, first_replay.job_id);
    assert_eq!(repeated_replay.status, JobStatus::Succeeded);
    assert_eq!(repeated_replay.disposition, JobEnqueueDisposition::Existing);

    let deleted = sqlx::query("DELETE FROM job_queue WHERE id = ANY($1::uuid[])")
        .bind(vec![source_job_id, first_replay.job_id])
        .execute(&pool)
        .await
        .expect("bulk retention may delete the source and replay together");
    assert_eq!(deleted.rows_affected(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_replays
             WHERE source_job_id = $1
               AND source_run_number = 1
               AND replay_request_key = 'retention-safe-replay'",
        )
        .bind(source_job_id)
        .fetch_one(&pool)
        .await
        .expect("count replay lineage after source retention"),
        0
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn transactional_replay_rolls_back_and_exact_observations_do_not_mutate() {
    let (pool, database) = setup_ephemeral_pool("postgres_successful_job_replay_tx", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(502);
    let payload = json!({"customer_id": "customer-502"});
    let checkpoint = json!({"cursor": 1});
    let output = json!({"artifact_id": "artifact-502"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-502").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let wrong_scope = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id,
            expected_run_number: 1,
            replay_request_key: "wrong-scope",
            reason: "must not disclose another scope",
        },
    )
    .await
    .expect("wrong scope is a normal outcome");
    assert!(matches!(
        wrong_scope,
        CompareAndReplaySucceededJobOutcome::NotFound
    ));
    let wrong_tenant = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Organization(Uuid::from_u128(9_502)),
            source_job_id,
            expected_run_number: 1,
            replay_request_key: "wrong-tenant",
            reason: "must not disclose another tenant",
        },
    )
    .await
    .expect("wrong tenant is a normal outcome");
    assert!(matches!(
        wrong_tenant,
        CompareAndReplaySucceededJobOutcome::NotFound
    ));

    let stale = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Organization(organization_id),
            source_job_id,
            expected_run_number: 99,
            replay_request_key: "stale-run",
            reason: "stale observation",
        },
    )
    .await
    .expect("stale run is a normal outcome");
    let CompareAndReplaySucceededJobOutcome::ExpectationMismatch { actual } = stale else {
        panic!("expected replay expectation mismatch");
    };
    assert_eq!(actual.status, JobStatus::Succeeded);
    assert_eq!(actual.run_number, 1);

    let mut incompatible_tx = pool.begin().await.expect("begin incompatible replay tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *incompatible_tx)
        .await
        .expect("set incompatible replay isolation");
    let isolation_error = compare_and_replay_succeeded_job_tx(
        &mut incompatible_tx,
        replay_request(
            organization_id,
            source_job_id,
            "wrong-isolation-action",
            "prove replay isolation validation",
        ),
    )
    .await
    .expect_err("caller-owned replay must reject repeatable read");
    let Error::QueryError(isolation_error) = isolation_error else {
        panic!("expected replay isolation validation error");
    };
    assert_eq!(isolation_error.category(), QueryErrorCategory::Validation);
    assert_eq!(
        isolation_error.code(),
        "job.compare_and_replay_unsupported_isolation"
    );
    incompatible_tx
        .rollback()
        .await
        .expect("rollback incompatible replay transaction");

    let mut tx = pool.begin().await.expect("begin replay transaction");
    let inserted = compare_and_replay_succeeded_job_tx(
        &mut tx,
        replay_request(
            organization_id,
            source_job_id,
            "rolled-back-action",
            "prove caller-owned rollback",
        ),
    )
    .await
    .expect("create replay in caller transaction");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: rolled_back_replay,
        ..
    } = inserted
    else {
        panic!("expected inserted replay");
    };
    tx.rollback().await.expect("rollback replay transaction");
    assert!(
        get_job_by_id(&pool, Some(organization_id), rolled_back_replay.job_id)
            .await
            .expect("query rolled-back replay")
            .is_none()
    );

    let committed = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "rolled-back-action",
            "prove caller-owned rollback",
        ),
    )
    .await
    .expect("retry rolled-back replay");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: committed_replay,
        ..
    } = committed
    else {
        panic!("expected committed replay");
    };
    assert_eq!(
        committed_replay.disposition,
        JobEnqueueDisposition::Inserted
    );
    assert_ne!(committed_replay.job_id, rolled_back_replay.job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn idempotent_existing_replay_holds_its_status_and_run_lock_until_commit() {
    let (pool, database) = setup_ephemeral_pool("postgres_existing_replay_lock", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(505);
    let payload = json!({"customer_id": "customer-505"});
    let checkpoint = json!({"cursor": 1});
    let output = json!({"artifact_id": "artifact-505"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-505").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let inserted = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-505",
            "prove existing replay lock",
        ),
    )
    .await
    .expect("insert replay before lock test");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: inserted_replay,
        ..
    } = inserted
    else {
        panic!("expected inserted replay outcome");
    };
    assert_eq!(inserted_replay.disposition, JobEnqueueDisposition::Inserted);

    let mut tx = pool
        .begin()
        .await
        .expect("begin existing replay transaction");
    let existing = compare_and_replay_succeeded_job_tx(
        &mut tx,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-505",
            "prove existing replay lock",
        ),
    )
    .await
    .expect("load existing replay under caller transaction");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: existing_replay,
        ..
    } = existing
    else {
        panic!("expected existing replay outcome");
    };
    assert_eq!(existing_replay.job_id, inserted_replay.job_id);
    assert_eq!(existing_replay.status, JobStatus::Pending);
    assert_eq!(existing_replay.run_number, 1);
    assert_eq!(existing_replay.disposition, JobEnqueueDisposition::Existing);

    let claims_while_locked = claim_jobs(&pool, "worker-existing-replay-lock", 30, 1)
        .await
        .expect("attempt claim while existing replay is locked");
    assert!(
        claims_while_locked.is_empty(),
        "worker claim must skip the replay while its returned status/run snapshot is locked"
    );

    tx.commit()
        .await
        .expect("commit existing replay transaction");

    let claims_after_commit = claim_jobs(&pool, "worker-existing-replay-lock", 30, 1)
        .await
        .expect("claim replay after existing transaction commits");
    assert_eq!(claims_after_commit.len(), 1);
    assert_eq!(claims_after_commit[0].id, existing_replay.job_id);
    assert_eq!(claims_after_commit[0].status, JobStatus::Leased);
    assert_eq!(claims_after_commit[0].run_number, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn conflicting_replay_request_does_not_lock_the_existing_replay() {
    let (pool, database) = setup_ephemeral_pool("postgres_replay_conflict_no_lock", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(506);
    let payload = json!({"customer_id": "customer-506"});
    let checkpoint = json!({"cursor": 1});
    let output = json!({"artifact_id": "artifact-506"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-506").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let inserted = compare_and_replay_succeeded_job(
        &pool,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-506",
            "original replay reason",
        ),
    )
    .await
    .expect("insert replay before conflict test");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: inserted_replay,
        ..
    } = inserted
    else {
        panic!("expected inserted replay outcome");
    };

    let mut conflict_tx = pool.begin().await.expect("begin conflicting replay tx");
    let error = compare_and_replay_succeeded_job_tx(
        &mut conflict_tx,
        replay_request(
            organization_id,
            source_job_id,
            "operator-action-506",
            "changed replay reason",
        ),
    )
    .await
    .expect_err("changed reason under one replay key must conflict");
    let Error::QueryError(error) = error else {
        panic!("expected classified replay conflict");
    };
    assert_eq!(error.code(), "job.replay_idempotency_conflict");

    let claims = claim_jobs(&pool, "worker-replay-conflict-no-lock", 30, 1)
        .await
        .expect("claim replay while conflict transaction remains open");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].id, inserted_replay.job_id);
    conflict_tx
        .rollback()
        .await
        .expect("rollback conflict transaction");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_managed_success_cannot_be_replayed_as_a_direct_job() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_successful_replay", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"kind": "workflow-step"});
    let metadata = json!({"test": "workflow-successful-replay"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("source"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.successful-replay"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");
    let source_job_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .and_then(|step| step.job_id)
        .expect("workflow job was released");
    let claim = claim_one_job(&pool, "worker-workflow-successful-replay").await;
    assert_eq!(claim.id, source_job_id);
    complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("claim has worker id"),
        None,
    )
    .await
    .expect("complete workflow step");

    let error = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id,
            expected_run_number: 1,
            replay_request_key: "workflow-replay",
            reason: "must reject direct workflow step replay",
        },
    )
    .await
    .expect_err("workflow-managed success must be rejected");
    let Error::QueryError(error) = error else {
        panic!("expected workflow replay validation error");
    };
    assert_eq!(error.code(), "job.workflow_requeue_not_supported");
    assert_eq!(
        error.client_message(),
        "Workflow-managed jobs cannot be requeued directly."
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_retries_resolve_to_one_replay_job() {
    let (pool, database) = setup_ephemeral_pool("postgres_concurrent_successful_replay", 6).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(503);
    let payload = json!({"customer_id": "customer-503"});
    let checkpoint = json!({"cursor": 1});
    let output = json!({"artifact_id": "artifact-503"});
    let source_job_id =
        enqueue_keyed_source(&pool, organization_id, &payload, "original-operation-503").await;
    complete_source_with_result(&pool, source_job_id, &checkpoint, &output).await;

    let barrier = std::sync::Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let pool = pool.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            compare_and_replay_succeeded_job(
                &pool,
                replay_request(
                    organization_id,
                    source_job_id,
                    "concurrent-operator-action",
                    "one action retried concurrently",
                ),
            )
            .await
            .expect("concurrent replay request")
        }));
    }

    let mut replay_ids = Vec::new();
    let mut dispositions = Vec::new();
    for task in tasks {
        let CompareAndReplaySucceededJobOutcome::Replayed { replay, .. } =
            task.await.expect("join concurrent replay request")
        else {
            panic!("expected replay outcome");
        };
        replay_ids.push(replay.job_id);
        dispositions.push(replay.disposition);
    }

    assert_eq!(replay_ids[0], replay_ids[1]);
    dispositions.sort_by_key(|disposition| match disposition {
        JobEnqueueDisposition::Inserted => 0,
        JobEnqueueDisposition::Existing => 1,
        _ => 2,
    });
    assert_eq!(
        dispositions,
        vec![
            JobEnqueueDisposition::Inserted,
            JobEnqueueDisposition::Existing
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_replays
             WHERE source_job_id = $1
               AND source_run_number = 1
               AND replay_request_key = 'concurrent-operator-action'",
        )
        .bind(source_job_id)
        .fetch_one(&pool)
        .await
        .expect("count concurrent replay lineage"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn replay_request_identity_is_validated_before_source_lookup() {
    let (pool, database) = setup_ephemeral_pool("postgres_successful_replay_validation", 2).await;
    let missing_source_job_id = Uuid::from_u128(504);

    for (key, reason, expected_code) in [
        (" ", "operator action", "job.replay_request_key_blank"),
        ("operator-action", "\t", "job.replay_reason_blank"),
    ] {
        let error = compare_and_replay_succeeded_job(
            &pool,
            CompareAndReplaySucceededJob {
                scope: JobScope::Global,
                source_job_id: missing_source_job_id,
                expected_run_number: 1,
                replay_request_key: key,
                reason,
            },
        )
        .await
        .expect_err("invalid replay request must fail before lookup");
        let Error::QueryError(error) = error else {
            panic!("expected replay validation error");
        };
        assert_eq!(error.category(), QueryErrorCategory::Validation);
        assert_eq!(error.code(), expected_code);
    }

    let oversized_key = "x".repeat(513);
    let error = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: missing_source_job_id,
            expected_run_number: 1,
            replay_request_key: &oversized_key,
            reason: "operator action",
        },
    )
    .await
    .expect_err("oversized replay request key must fail before lookup");
    let Error::QueryError(error) = error else {
        panic!("expected replay key length validation error");
    };
    assert_eq!(error.category(), QueryErrorCategory::Validation);
    assert_eq!(error.code(), "job.replay_request_key_too_long");

    let maximum_key = "x".repeat(512);
    let outcome = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: missing_source_job_id,
            expected_run_number: 1,
            replay_request_key: &maximum_key,
            reason: "operator action",
        },
    )
    .await
    .expect("maximum-length replay key is valid");
    assert!(matches!(
        outcome,
        CompareAndReplaySucceededJobOutcome::NotFound
    ));

    teardown_ephemeral_pool(pool, database).await;
}
