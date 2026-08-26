use super::*;

const JOB_TYPE: JobType<'static> = JobType::new("jobs.test.heartbeat_progress_lock_race");

struct ProgressDuringHeartbeatHandler {
    pool: PgPool,
}

#[async_trait::async_trait]
impl JobHandler for ProgressDuringHeartbeatHandler {
    fn job_type(&self) -> JobType<'static> {
        JOB_TYPE
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        update_job_ordinary_progress(
            &self.pool,
            context.job_id,
            context.run_number,
            context.attempt,
            &context.worker_id,
            &JobOrdinaryProgressUpdate {
                progress_done: Some(1),
                progress_total: Some(2),
                checkpoint: None,
            },
        )
        .await
        .expect("persist progress while heartbeat is due");

        Ok(JobCompletion::success())
    }
}

#[tokio::test]
async fn pending_heartbeat_keeps_polling_handler_that_owns_the_job_row_lock() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_progress_lock", 8).await;
    record_postgres_server_version(&pool, "heartbeat/progress row-lock regression").await;

    sqlx::query(
        "CREATE FUNCTION runledger_test_delay_progress_update()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.progress_done IS DISTINCT FROM OLD.progress_done
                AND NEW.progress_done = 1 THEN
                 PERFORM pg_sleep(3);
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create progress delay trigger function");
    sqlx::query(
        "CREATE TRIGGER runledger_test_delay_progress_update
         BEFORE UPDATE OF progress_done ON job_queue
         FOR EACH ROW
         EXECUTE FUNCTION runledger_test_delay_progress_update()",
    )
    .execute(&pool)
    .await
    .expect("create progress delay trigger");

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JOB_TYPE,
        3,
        json!({"kind": "heartbeat-progress-lock-race"}),
        "worker-heartbeat-progress-lock-race",
    )
    .await;
    let mut registry = JobRegistry::new();
    registry.register(ProgressDuringHeartbeatHandler { pool: pool.clone() });

    // A three-second lease TTL makes the first heartbeat due after one second,
    // while the trigger keeps the progress UPDATE holding the job-row lock for
    // three seconds. The pg_stat_activity observation proves the heartbeat and
    // progress operations overlap on that exact lock.
    let process_pool = pool.clone();
    let mut process_task = tokio::spawn(async move {
        process_claimed_job(process_pool, Arc::new(registry), claimed_job, 3).await;
    });
    wait_for_heartbeat_to_block_on_job_lock(&pool).await;

    await_spawned_task(
        &mut process_task,
        Duration::from_secs(10),
        "worker self-deadlocked while progress held the heartbeat job-row lock",
        "job processing task should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after heartbeat/progress race")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.progress_done, Some(1));
    assert_eq!(persisted.progress_total, Some(2));

    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let stranded_sessions = sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND (
                    state = 'idle in transaction'
                    OR (
                        wait_event_type = 'Lock'
                        AND query LIKE '%UPDATE job_queue%'
                        AND query LIKE '%make_interval%'
                    )
               )",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect heartbeat/progress cleanup");
        if stranded_sessions == 0 {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "heartbeat/progress race left {stranded_sessions} blocked or idle-in-transaction session(s)"
        );
        sleep(Duration::from_millis(20)).await;
    }

    teardown_ephemeral_pool(pool, database).await;
}
