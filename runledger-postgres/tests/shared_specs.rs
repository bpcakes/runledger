//! API-only acceptance: this target imports no runtime or provider clients.
use runledger_core::jobs::{JobContract, JobDefinitionSettings, JobSpec, JobSpecs, JobType};
use runledger_postgres::jobs::{
    JobDefinitionCatalogSyncMode, JobDefinitionUpsert, JobEnqueue, JobEnqueueDisposition,
    enqueue_job_with_outcome, sync_catalog_job_definitions_exact_tx,
    sync_catalog_job_definitions_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Serialize, Deserialize)]
struct Payload {
    request_id: String,
}
struct Send;
impl JobContract for Send {
    type Payload = Payload;
    fn spec() -> JobSpec {
        JobSpec::new(JobType::new("producer.send"))
            .expect("static spec")
            .with_settings(
                JobDefinitionSettings::new()
                    .max_attempts(5)
                    .timeout_seconds(60),
            )
            .expect("settings")
    }
}

async fn sync(pool: &runledger_postgres::DbPool, spec: JobSpec) {
    let specs = JobSpecs::new([spec]).expect("producer specs");
    let definitions: Vec<_> = specs.iter().map(JobDefinitionUpsert::from).collect();
    let mut tx = pool.begin().await.expect("begin");
    sync_catalog_job_definitions_tx(
        &mut tx,
        &definitions,
        JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions,
    )
    .await
    .expect("sync");
    tx.commit().await.expect("commit");
}

#[tokio::test]
async fn api_only_submission_preserves_snapshot_outcomes_and_operator_disables() {
    let (pool, database) = setup_ephemeral_pool("shared_specs", 3).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("version");
    eprintln!("shared specs server_version={version}");
    assert!(
        version.starts_with("18."),
        "repository baseline is PostgreSQL 18: {version}"
    );
    sync(&pool, Send::spec()).await;
    let request = Send::submit(&Payload {
        request_id: "legacy-id".into(),
    })
    .expect("typed submit")
    .idempotency_key("same-request");
    let inserted = enqueue_job_with_outcome(&pool, &JobEnqueue::from(&request))
        .await
        .expect("insert");
    assert_eq!(inserted.disposition, JobEnqueueDisposition::Inserted);
    let stored: (Value, i32, i32) = sqlx::query_as(
        "SELECT enqueue_request, max_attempts, timeout_seconds FROM job_queue WHERE id = $1",
    )
    .bind(inserted.job_id)
    .fetch_one(&pool)
    .await
    .expect("stored row");
    assert_eq!(
        stored,
        (
            json!({"payload":{"request_id":"legacy-id"},"priority":null,"max_attempts":null,"timeout_seconds":null,"next_run_at":null,"stage":"queued"}),
            5,
            60
        )
    );
    let updated = Send::spec()
        .with_settings(
            JobDefinitionSettings::new()
                .version(9)
                .max_attempts(8)
                .timeout_seconds(90),
        )
        .expect("updated definition");
    sync(&pool, updated).await;
    let existing = enqueue_job_with_outcome(&pool, &JobEnqueue::from(&request))
        .await
        .expect("retry across settings change");
    assert_eq!(existing.disposition, JobEnqueueDisposition::Existing);
    assert_eq!(existing.job_id, inserted.job_id);
    let mut changed = request.clone();
    changed.payload = json!({"request_id":"different"});
    let error = enqueue_job_with_outcome(&pool, &JobEnqueue::from(&changed))
        .await
        .expect_err("strict conflict");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(ref error) if error.code() == "job.idempotency_conflict"),
        "{error:?}"
    );
    let overridden = request.clone().max_attempts(5);
    assert!(
        enqueue_job_with_outcome(&pool, &JobEnqueue::from(&overridden))
            .await
            .is_err()
    );
    sqlx::query("UPDATE job_definitions SET is_enabled = false WHERE job_type = 'producer.send'")
        .execute(&pool)
        .await
        .expect("operator disable");
    sync(&pool, updated).await;
    let enabled: bool = sqlx::query_scalar(
        "SELECT is_enabled FROM job_definitions WHERE job_type = 'producer.send'",
    )
    .fetch_one(&pool)
    .await
    .expect("enabled state");
    assert!(!enabled, "additive sync must preserve operator disable");
    let fresh = request.clone().idempotency_key("fresh");
    assert!(
        enqueue_job_with_outcome(&pool, &JobEnqueue::from(&fresh))
            .await
            .is_err()
    );
    // Explicit exact mode restores code-owned enabled state, just as the legacy catalog does.
    let mut tx = pool.begin().await.expect("begin exact");
    sync_catalog_job_definitions_exact_tx(
        &mut tx,
        &[JobDefinitionUpsert::from(&updated)],
        &[runledger_core::jobs::JobTypeName::new(updated.job_type().as_str()).expect("scope")],
    )
    .await
    .expect("exact sync");
    tx.commit().await.expect("commit exact");
    assert_eq!(
        enqueue_job_with_outcome(&pool, &JobEnqueue::from(&fresh))
            .await
            .expect("restored")
            .disposition,
        JobEnqueueDisposition::Inserted
    );
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_events WHERE job_id = $1 AND event_type = 'ENQUEUED'",
    )
    .bind(inserted.job_id)
    .fetch_one(&pool)
    .await
    .expect("events");
    assert_eq!(
        events, 1,
        "retries and rejected conflicts must not add enqueue events"
    );
    teardown_ephemeral_pool(pool, database).await;
}
