use std::str::FromStr;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use cron::Schedule;
use runledger_postgres::jobs::{self, JobEnqueue};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::JobsConfig;
use crate::shutdown;
use crate::{Result, RuntimeLoopExit, SchedulerError};

const FAILED_SCHEDULE_RETRY_DELAY_SECONDS: i64 = 30;
const SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT: usize =
    jobs::JOB_SCHEDULE_MAX_JITTER_SECONDS as usize + 2;
const CREATE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str = "SAVEPOINT materialize_due_schedule";
const ROLLBACK_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str =
    "ROLLBACK TO SAVEPOINT materialize_due_schedule";
const RELEASE_MATERIALIZE_DUE_SCHEDULE_SAVEPOINT_SQL: &str =
    "RELEASE SAVEPOINT materialize_due_schedule";

/// Runs cron schedule materialization until shutdown is requested.
///
/// The loop claims due schedules in batches using
/// [`JobsConfig::claim_batch_size`], materializes each due schedule into a job,
/// advances the schedule's next UTC fire cursor, then waits for either shutdown
/// or [`JobsConfig::schedule_poll_interval`] before polling again. Individual
/// materialization failures are logged and deferred so one bad schedule does not
/// permanently starve other due schedules.
///
/// When a schedule is stale, one missed fire is materialized with its original
/// `scheduled_for` metadata and the cursor is then coalesced to the first future
/// fire after the scheduler's current clock. This bounds outage catch-up instead
/// of replaying every missed cron tick.
///
/// Shutdown is requested by sending `true` on `shutdown` or by dropping the
/// watch sender. This function returns [`RuntimeLoopExit::Shutdown`] after the
/// shutdown signal is observed.
///
/// This lower-level loop remains public for custom runtime orchestration.
/// Prefer [`crate::Supervisor`] for ordinary worker processes so scheduler,
/// worker, and reaper tasks are started, monitored, and shut down together.
pub async fn run_scheduler_loop(
    pool: runledger_postgres::DbPool,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_scheduler_loop() {
        warn!(%error, "invalid jobs config; stopping scheduler loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    loop {
        if shutdown::is_requested_or_closed(&shutdown) {
            return scheduler_shutdown_complete();
        }

        if let Err(error) = materialize_due_schedules(&pool, config.claim_batch_size).await {
            warn!(%error, "schedule materialization failed");
        }

        if shutdown::wait_for_request_or_timeout(&mut shutdown, config.schedule_poll_interval).await
        {
            return scheduler_shutdown_complete();
        }
    }
}

fn scheduler_shutdown_complete() -> RuntimeLoopExit {
    info!("scheduler shutdown complete");
    RuntimeLoopExit::Shutdown
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
    materialize_claimed_schedules_tx(tx, now, schedules).await
}

async fn materialize_claimed_schedules_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    now: DateTime<Utc>,
    schedules: Vec<jobs::JobScheduleRecord>,
) -> Result<()> {
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

            if matches!(
                error,
                crate::Error::Scheduler(SchedulerError::ClaimedScheduleMissing { .. })
            ) {
                return Err(error);
            }

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
    let updated = sqlx::query!(
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

    if updated.rows_affected() == 0 {
        return Err(SchedulerError::ClaimedScheduleMissing {
            schedule_id,
            operation: "deferring failed schedule",
        }
        .into());
    }

    Ok(())
}

async fn materialize_schedule_tx(
    tx: &mut runledger_postgres::DbTx<'_>,
    schedule: &jobs::JobScheduleRecord,
    now: DateTime<Utc>,
) -> Result<()> {
    let scheduled_for = schedule.next_fire_at;
    let next_fire_at = compute_next_fire_at_with_stale_coalescing_utc(
        &schedule.cron_expr,
        scheduled_for,
        now,
        schedule.id,
        schedule.max_jitter_seconds,
    )
    .ok_or_else(|| invalid_schedule_cron_error(schedule))?;

    let mut payload = schedule.payload_template.clone();
    merge_schedule_metadata(&mut payload, schedule.id, &schedule.name, scheduled_for);

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

    let marked = jobs::mark_schedule_fired_tx(tx, schedule.id, now, next_fire_at)
        .await
        .map_err(|source| SchedulerError::MarkScheduleFired {
            schedule_id: schedule.id,
            source,
        })?;
    if !marked {
        // The enqueue above is still inside the caller's schedule savepoint.
        // Returning an error lets the caller roll it back instead of producing
        // work for a schedule row that is no longer present.
        return Err(SchedulerError::ClaimedScheduleMissing {
            schedule_id: schedule.id,
            operation: "marking schedule as fired",
        }
        .into());
    }
    Ok(())
}

/// Materializes at most one missed fire for a stale schedule, then coalesces the
/// cursor to the first future fire so an outage cannot create unbounded replay.
fn compute_next_fire_at_with_stale_coalescing_utc(
    cron_expr: &str,
    scheduled_for: DateTime<Utc>,
    now: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    let schedule = Schedule::from_str(cron_expr).ok()?;
    let next_after_scheduled =
        next_jittered_fire_after_utc(&schedule, scheduled_for, schedule_id, max_jitter_seconds)?;
    if next_after_scheduled <= now {
        compute_coalesced_next_fire_after_now_utc(&schedule, now, schedule_id, max_jitter_seconds)
    } else {
        Some(next_after_scheduled)
    }
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
#[cfg(test)]
fn compute_next_fire_at_utc(
    cron_expr: &str,
    from: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    let schedule = Schedule::from_str(cron_expr).ok()?;
    next_jittered_fire_after_utc(&schedule, from, schedule_id, max_jitter_seconds)
}

fn compute_coalesced_next_fire_after_now_utc(
    schedule: &Schedule,
    now: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    if max_jitter_seconds <= 0 {
        return next_jittered_fire_after_utc(schedule, now, schedule_id, max_jitter_seconds);
    }

    let jitter_window_start = now
        .checked_sub_signed(Duration::seconds(i64::from(max_jitter_seconds) + 1))
        .unwrap_or(now);
    first_jittered_fire_after_utc(
        schedule,
        jitter_window_start,
        now,
        schedule_id,
        max_jitter_seconds,
    )
    .or_else(|| next_jittered_fire_after_utc(schedule, now, schedule_id, max_jitter_seconds))
}

fn first_jittered_fire_after_utc(
    schedule: &Schedule,
    from: DateTime<Utc>,
    after: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    first_jittered_fire_after_candidates_utc(
        schedule
            .after(&from)
            .take(SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT),
        after,
        schedule_id,
        max_jitter_seconds,
    )
}

fn first_jittered_fire_after_candidates_utc(
    candidates: impl Iterator<Item = DateTime<Utc>>,
    after: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    let mut best = None;
    for base_fire_at in candidates {
        // Cron bases are strictly increasing and jitter is nonnegative. Once a
        // base reaches the best adjusted fire, no remaining candidate can
        // produce an earlier timestamp; an equal timestamp preserves the same
        // observable tie result.
        if best.is_some_and(|best| base_fire_at >= best) {
            break;
        }

        let jittered_fire_at = apply_schedule_jitter(schedule_id, base_fire_at, max_jitter_seconds);
        if jittered_fire_at > after && best.is_none_or(|best| jittered_fire_at < best) {
            best = Some(jittered_fire_at);
        }
    }
    best
}

fn next_jittered_fire_after_utc(
    schedule: &Schedule,
    from: DateTime<Utc>,
    schedule_id: uuid::Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    let next = schedule.after(&from).next()?;
    Some(apply_schedule_jitter(schedule_id, next, max_jitter_seconds))
}

fn apply_schedule_jitter(
    schedule_id: uuid::Uuid,
    next_fire_at: DateTime<Utc>,
    max_jitter_seconds: i32,
) -> DateTime<Utc> {
    // Jitter is intentionally non-negative; the stale coalescing search depends
    // on never moving a cron base earlier than its scheduled time.
    next_fire_at
        + Duration::seconds(schedule_jitter_seconds(
            schedule_id,
            next_fire_at,
            max_jitter_seconds,
        ))
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
