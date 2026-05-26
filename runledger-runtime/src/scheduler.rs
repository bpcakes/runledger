use std::str::FromStr;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use cron::Schedule;
use runledger_postgres::jobs::{self, JobEnqueue};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::JobsConfig;
use crate::{Result, RuntimeLoopExit, SchedulerError};

const FAILED_SCHEDULE_RETRY_DELAY_SECONDS: i64 = 30;
const CREATE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str = "SAVEPOINT materialize_due_schedule";
const ROLLBACK_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str =
    "ROLLBACK TO SAVEPOINT materialize_due_schedule";
const RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str =
    "RELEASE SAVEPOINT materialize_due_schedule";

pub async fn run_scheduler_loop(
    pool: runledger_postgres::DbPool,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    loop {
        if shutdown_requested_or_closed(&shutdown) {
            return scheduler_shutdown_complete();
        }

        if let Err(error) = materialize_due_schedules(&pool, config.claim_batch_size).await {
            warn!(%error, "schedule materialization failed");
        }

        if wait_for_shutdown_or_poll(&mut shutdown, config.schedule_poll_interval).await {
            return scheduler_shutdown_complete();
        }
    }
}

fn scheduler_shutdown_complete() -> RuntimeLoopExit {
    info!("scheduler shutdown complete");
    RuntimeLoopExit::Shutdown
}

fn shutdown_requested_or_closed(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

async fn wait_for_shutdown_or_poll(
    shutdown: &mut watch::Receiver<bool>,
    poll_interval: std::time::Duration,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        _ = sleep(poll_interval) => false,
    }
}

async fn materialize_due_schedules(
    pool: &runledger_postgres::DbPool,
    batch_size: i64,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| SchedulerError::BeginTransaction {
            source: runledger_postgres::Error::ConnectionError(error.to_string()),
        })?;

    let now = Utc::now();
    materialize_due_schedules_tx(&mut tx, now, batch_size).await?;

    tx.commit()
        .await
        .map_err(|error| SchedulerError::CommitTransaction {
            source: runledger_postgres::Error::ConnectionError(error.to_string()),
        })?;
    Ok(())
}

fn savepoint_error_variant(
    statement: &'static str,
    source: runledger_postgres::Error,
) -> SchedulerError {
    match statement {
        CREATE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            SchedulerError::SavepointCreate { statement, source }
        }
        ROLLBACK_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            SchedulerError::SavepointRollback { statement, source }
        }
        RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            SchedulerError::SavepointRelease { statement, source }
        }
        _ => unreachable!("unexpected savepoint statement: {statement}"),
    }
}

async fn execute_savepoint_sql_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    statement: &'static str,
) -> Result<()> {
    match statement {
        CREATE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            sqlx::query!("SAVEPOINT materialize_due_schedule")
                .execute(&mut **tx)
                .await
                .map_err(|error| {
                    savepoint_error_variant(
                        statement,
                        runledger_postgres::Error::ConnectionError(error.to_string()),
                    )
                })?;
        }
        ROLLBACK_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            sqlx::query!("ROLLBACK TO SAVEPOINT materialize_due_schedule")
                .execute(&mut **tx)
                .await
                .map_err(|error| {
                    savepoint_error_variant(
                        statement,
                        runledger_postgres::Error::ConnectionError(error.to_string()),
                    )
                })?;
        }
        RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL => {
            sqlx::query!("RELEASE SAVEPOINT materialize_due_schedule")
                .execute(&mut **tx)
                .await
                .map_err(|error| {
                    savepoint_error_variant(
                        statement,
                        runledger_postgres::Error::ConnectionError(error.to_string()),
                    )
                })?;
        }
        _ => unreachable!("unexpected savepoint statement: {statement}"),
    }

    Ok(())
}

async fn materialize_due_schedules_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    now: DateTime<Utc>,
    batch_size: i64,
) -> Result<()> {
    let schedules = jobs::claim_due_schedules_tx(tx, now, batch_size)
        .await
        .map_err(|source| SchedulerError::ClaimDueSchedules { source })?;
    for schedule in schedules {
        execute_savepoint_sql_tx(tx, CREATE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL).await?;

        if let Err(error) = materialize_schedule_tx(tx, &schedule, now).await {
            warn!(
                %error,
                schedule_id=%schedule.id,
                schedule_name=%schedule.name,
                "schedule materialization failed; skipping"
            );
            execute_savepoint_sql_tx(tx, ROLLBACK_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL).await?;
            execute_savepoint_sql_tx(tx, RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL).await?;

            // Push failed schedules out of the immediate due window to avoid
            // repeatedly selecting the same failing rows and starving valid schedules.
            let retry_at = failed_schedule_retry_at(now);
            defer_failed_schedule_tx(tx, schedule.id, retry_at).await?;
            continue;
        }

        execute_savepoint_sql_tx(tx, RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL).await?;
    }

    Ok(())
}

async fn defer_failed_schedule_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    schedule_id: uuid::Uuid,
    next_fire_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_schedules
         SET next_fire_at = $2,
             updated_at = now()
         WHERE id = $1",
        schedule_id,
        next_fire_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| SchedulerError::DeferFailedSchedule {
        schedule_id,
        source: runledger_postgres::Error::from_query_sqlx_with_context(
            "defer failed schedule",
            error,
        ),
    })?;

    Ok(())
}

async fn materialize_schedule_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    schedule: &jobs::JobScheduleRecord,
    now: DateTime<Utc>,
) -> Result<()> {
    let next_fire_at = compute_next_fire_at_utc(
        &schedule.cron_expr,
        now,
        schedule.id,
        schedule.max_jitter_seconds,
    )
    .ok_or_else(|| invalid_schedule_cron_error(schedule))?;

    let mut payload = schedule.payload_template.clone();
    merge_schedule_metadata(
        &mut payload,
        schedule.id,
        &schedule.name,
        schedule.next_fire_at,
    );

    let enqueue_payload = JobEnqueue {
        job_type: schedule.job_type.as_borrowed(),
        organization_id: schedule.organization_id,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: Some(now),
        idempotency_key: None,
        stage: Some(runledger_core::jobs::JobStage::Scheduled),
    };

    jobs::enqueue_job_tx(tx, &enqueue_payload)
        .await
        .map_err(|source| SchedulerError::EnqueueScheduledJob {
            schedule_id: schedule.id,
            job_type: schedule.job_type.to_string(),
            source,
        })?;

    jobs::mark_schedule_fired_tx(tx, schedule.id, now, next_fire_at)
        .await
        .map_err(|source| SchedulerError::MarkScheduleFired {
            schedule_id: schedule.id,
            source,
        })?;
    Ok(())
}

fn invalid_schedule_cron_error(schedule: &jobs::JobScheduleRecord) -> SchedulerError {
    SchedulerError::InvalidCronExpression {
        schedule_id: schedule.id,
        schedule_name: schedule.name.clone(),
        cron_expr: schedule.cron_expr.clone(),
    }
}

fn merge_schedule_metadata(
    payload: &mut Value,
    schedule_id: uuid::Uuid,
    schedule_name: &str,
    scheduled_for: DateTime<Utc>,
) {
    let metadata = json!({
        "schedule_id": schedule_id,
        "schedule_name": schedule_name,
        "scheduled_for": scheduled_for.to_rfc3339_opts(SecondsFormat::AutoSi, true),
    });

    match payload {
        Value::Object(map) => {
            map.insert("_schedule".to_string(), metadata);
        }
        _ => {
            let original_payload = std::mem::take(payload);
            *payload = json!({
                "payload": original_payload,
                "_schedule": metadata,
            });
        }
    }
}

/// Scheduling semantics are UTC-only across the jobs framework.
fn compute_next_fire_at_utc(
    cron_expr: &str,
    from: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    let schedule = Schedule::from_str(cron_expr).ok()?;
    let next = schedule.upcoming(Utc).find(|next| *next > from)?;
    let jitter = schedule_jitter_seconds(schedule_id, next, max_jitter_seconds);
    Some(next + Duration::seconds(jitter))
}

fn schedule_jitter_seconds(
    schedule_id: uuid::Uuid,
    next_fire_at: DateTime<Utc>,
    max_jitter_seconds: i32,
) -> i64 {
    if max_jitter_seconds <= 0 {
        return 0;
    }

    let max_range = max_jitter_seconds as u128 + 1;
    let next_millis = next_fire_at.timestamp_millis() as u128;
    ((schedule_id.as_u128() ^ next_millis) % max_range) as i64
}

fn failed_schedule_retry_at(now: DateTime<Utc>) -> DateTime<Utc> {
    now + Duration::seconds(FAILED_SCHEDULE_RETRY_DELAY_SECONDS)
}

#[cfg(test)]
mod tests;
