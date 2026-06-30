use std::cmp::min;
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{WorkflowRunEnqueue, WorkflowRunStatus};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::types::Uuid;
use tokio::time::{Instant, sleep_until};

use crate::{DbPool, Error, QueryError, QueryErrorCategory, Result};

use super::super::row_decode::{
    parse_step_key_name, parse_workflow_run_status, parse_workflow_type_name,
};
use super::super::rows::WorkflowRunRow;
use super::super::workflow_types::{
    WorkflowRunDbRecord, WorkflowRunHandle, WorkflowRunHandleError, WorkflowRunHandleScope,
    WorkflowRunResultRecord, WorkflowRunWaitOptions,
};
use super::enqueue::enqueue_workflow_run;
use super::read::get_workflow_run_by_id;
use super::runtime::WORKFLOW_RUN_TERMINAL_CHANNEL;

struct WorkflowRunResultLookupRow {
    id: Uuid,
    workflow_type: String,
    organization_id: Option<Uuid>,
    status: String,
    result_step_key: Option<String>,
    result: Option<Value>,
    finished_at: Option<DateTime<Utc>>,
}

enum WorkflowRunResultLookup {
    Pending,
    Ready(WorkflowRunResultRecord),
}

impl WorkflowRunHandle {
    pub async fn get_status(
        &self,
    ) -> std::result::Result<Option<WorkflowRunStatus>, WorkflowRunHandleError> {
        load_workflow_run_status(&self.pool, self.scope, self.workflow_run_id)
            .await
            .map_err(WorkflowRunHandleError::from)
    }

    pub async fn get_run(
        &self,
    ) -> std::result::Result<Option<WorkflowRunDbRecord>, WorkflowRunHandleError> {
        load_workflow_run_for_scope(&self.pool, self.scope, self.workflow_run_id)
            .await
            .map_err(WorkflowRunHandleError::from)
    }

    pub async fn get_result(
        &self,
        options: WorkflowRunWaitOptions,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        let poll_interval = normalize_poll_interval(options.poll_interval);
        if options.timeout == Some(Duration::ZERO) {
            return match load_workflow_run_result_deadline_probe(
                &self.pool,
                self.scope,
                self.workflow_run_id,
            )
            .await?
            {
                WorkflowRunResultLookup::Ready(record) => Ok(record),
                WorkflowRunResultLookup::Pending => Err(WorkflowRunHandleError::Timeout),
            };
        }

        let deadline = options.timeout.map(instant_after);

        if let WorkflowRunResultLookup::Ready(record) =
            load_workflow_run_result_before_deadline_or_final_probe(
                &self.pool,
                self.scope,
                self.workflow_run_id,
                deadline,
            )
            .await?
        {
            return Ok(record);
        }
        if deadline_has_elapsed(deadline) {
            return Err(WorkflowRunHandleError::Timeout);
        }

        let mut listener = match connect_terminal_listener(&self.pool, deadline).await {
            Ok(listener) => listener,
            Err(WorkflowRunHandleError::Timeout) if deadline_has_elapsed(deadline) => {
                return lookup_ready_or_timeout(
                    load_workflow_run_result_deadline_probe(
                        &self.pool,
                        self.scope,
                        self.workflow_run_id,
                    )
                    .await?,
                );
            }
            Err(error) => return Err(error),
        };
        if deadline_has_elapsed(deadline) {
            return lookup_ready_or_timeout(
                load_workflow_run_result_deadline_probe(
                    &self.pool,
                    self.scope,
                    self.workflow_run_id,
                )
                .await?,
            );
        }

        // Close the initial-read/LISTEN race. A terminal transition committed
        // just before LISTEN may not produce a notification for this connection.
        if let WorkflowRunResultLookup::Ready(record) =
            load_workflow_run_result_before_deadline_or_final_probe(
                &self.pool,
                self.scope,
                self.workflow_run_id,
                deadline,
            )
            .await?
        {
            return Ok(record);
        }
        if deadline_has_elapsed(deadline) {
            return Err(WorkflowRunHandleError::Timeout);
        }

        // The poll wake-up is an absolute instant so notification traffic for
        // other workflow runs cannot keep resetting it and starve the polling
        // backstop.
        let mut next_poll = instant_after(poll_interval);

        loop {
            let wake = next_poll_wake(deadline, next_poll);

            if let Some(active_listener) = listener.as_mut() {
                tokio::select! {
                    notification = active_listener.recv() => {
                        match notification {
                            Ok(notification) => {
                                if listener_event_needs_result_lookup(
                                    notification.payload(),
                                    self.workflow_run_id,
                                    deadline,
                                ) {
                                    if let WorkflowRunResultLookup::Ready(record) =
                                        load_workflow_run_result_after_notification(
                                            &self.pool,
                                            self.scope,
                                            self.workflow_run_id,
                                            deadline,
                                        )
                                        .await?
                                    {
                                        return Ok(record);
                                    }
                                    if deadline_has_elapsed(deadline) {
                                        return Err(WorkflowRunHandleError::Timeout);
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    error = %error,
                                    workflow_run_id = %self.workflow_run_id,
                                    "workflow result listener failed; falling back to polling"
                                );
                                listener = None;
                                if deadline_has_elapsed(deadline) {
                                    if let WorkflowRunResultLookup::Ready(record) =
                                        load_workflow_run_result_deadline_probe(
                                            &self.pool,
                                            self.scope,
                                            self.workflow_run_id,
                                        )
                                        .await?
                                    {
                                        return Ok(record);
                                    }
                                    return Err(WorkflowRunHandleError::Timeout);
                                }
                            }
                        }
                    }
                    () = sleep_until(wake.instant) => {
                        next_poll = instant_after(poll_interval);
                        let lookup = load_workflow_run_result_after_poll_wake(
                            &self.pool,
                            self.scope,
                            self.workflow_run_id,
                            deadline,
                            wake.reached_deadline,
                        )
                        .await?;
                        if let WorkflowRunResultLookup::Ready(record) = lookup {
                            return Ok(record);
                        }
                        if wake.reached_deadline {
                            return Err(WorkflowRunHandleError::Timeout);
                        }
                    }
                }
                continue;
            }

            sleep_until(wake.instant).await;
            next_poll = instant_after(poll_interval);
            let lookup = load_workflow_run_result_after_poll_wake(
                &self.pool,
                self.scope,
                self.workflow_run_id,
                deadline,
                wake.reached_deadline,
            )
            .await?;
            if let WorkflowRunResultLookup::Ready(record) = lookup {
                return Ok(record);
            }
            if wake.reached_deadline {
                return Err(WorkflowRunHandleError::Timeout);
            }
        }
    }
}

pub fn workflow_run_handle(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> WorkflowRunHandle {
    WorkflowRunHandle {
        workflow_run_id,
        scope,
        pool: pool.clone(),
    }
}

pub async fn retrieve_workflow_run_handle(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> std::result::Result<WorkflowRunHandle, WorkflowRunHandleError> {
    let Some(_) = load_workflow_run_status(pool, scope, workflow_run_id).await? else {
        return Err(WorkflowRunHandleError::NotFound);
    };

    Ok(workflow_run_handle(pool, scope, workflow_run_id))
}

pub async fn enqueue_workflow_run_handle(
    pool: &DbPool,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunHandle> {
    let workflow_run = enqueue_workflow_run(pool, payload).await?;
    let scope = workflow_run
        .organization_id
        .map(WorkflowRunHandleScope::Organization)
        .unwrap_or(WorkflowRunHandleScope::Global);

    Ok(workflow_run_handle(pool, scope, workflow_run.id))
}

const DEADLINE_RESULT_LOOKUP_TIMEOUT: Duration = Duration::from_millis(250);

async fn connect_terminal_listener(
    pool: &DbPool,
    deadline: Option<Instant>,
) -> std::result::Result<Option<PgListener>, WorkflowRunHandleError> {
    let mut listener = match await_before_deadline(deadline, PgListener::connect_with(pool)).await?
    {
        Ok(listener) => listener,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "workflow result listener connect failed; using polling"
            );
            return Ok(None);
        }
    };

    if !pool_can_spare_query_connection(pool) {
        let pool_size = pool.size();
        let pool_idle = pool.num_idle();
        let max_connections = pool.options().get_max_connections();
        tracing::debug!(
            pool_size,
            pool_idle,
            max_connections,
            "workflow result listener skipped; pool has no spare connection for race-closing reads"
        );
        return Ok(None);
    }

    if let Err(error) =
        await_before_deadline(deadline, listener.listen(WORKFLOW_RUN_TERMINAL_CHANNEL)).await?
    {
        tracing::warn!(
            error = %error,
            "workflow result listener subscribe failed; using polling"
        );
        return Ok(None);
    }

    Ok(Some(listener))
}

fn pool_can_spare_query_connection(pool: &DbPool) -> bool {
    pool.num_idle() > 0 || pool.size() < pool.options().get_max_connections()
}

fn normalize_poll_interval(poll_interval: Duration) -> Duration {
    poll_interval.max(Duration::from_millis(1))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PollWake {
    instant: Instant,
    reached_deadline: bool,
}

fn next_poll_wake(deadline: Option<Instant>, next_poll: Instant) -> PollWake {
    let Some(deadline) = deadline else {
        return PollWake {
            instant: next_poll,
            reached_deadline: false,
        };
    };

    PollWake {
        instant: min(next_poll, deadline),
        reached_deadline: deadline <= next_poll || deadline <= Instant::now(),
    }
}

fn instant_after(duration: Duration) -> Instant {
    instant_saturating_add(Instant::now(), duration)
}

fn instant_saturating_add(base: Instant, duration: Duration) -> Instant {
    base.checked_add(duration)
        .unwrap_or_else(|| far_future_instant(base))
}

fn far_future_instant(base: Instant) -> Instant {
    let mut seconds = 60 * 60 * 24 * 365 * 100;
    loop {
        if let Some(instant) = base.checked_add(Duration::from_secs(seconds)) {
            return instant;
        }
        seconds /= 2;
    }
}

async fn await_before_deadline<F, T>(
    deadline: Option<Instant>,
    future: F,
) -> std::result::Result<T, WorkflowRunHandleError>
where
    F: Future<Output = T>,
{
    let Some(deadline) = deadline else {
        return Ok(future.await);
    };

    let now = Instant::now();
    if deadline <= now {
        return Err(WorkflowRunHandleError::Timeout);
    }

    tokio::time::timeout(deadline - now, future)
        .await
        .map_err(|_| WorkflowRunHandleError::Timeout)
}

fn notification_matches_workflow_run(payload: &str, workflow_run_id: Uuid) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| {
            value
                .get("workflow_run_id")
                .and_then(Value::as_str)
                .and_then(|raw_id| Uuid::parse_str(raw_id).ok())
        })
        == Some(workflow_run_id)
}

fn listener_event_needs_result_lookup(
    payload: &str,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
) -> bool {
    notification_matches_workflow_run(payload, workflow_run_id) || deadline_has_elapsed(deadline)
}

fn deadline_has_elapsed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| deadline <= Instant::now())
}

async fn load_workflow_run_status(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunStatus>> {
    let status = match scope {
        WorkflowRunHandleScope::Organization(organization_id) => {
            sqlx::query_scalar!(
                "SELECT status::text AS \"status!\"
                 FROM workflow_runs
                 WHERE id = $1
                   AND organization_id = $2
                 LIMIT 1",
                workflow_run_id,
                organization_id,
            )
            .fetch_optional(pool)
            .await
        }
        WorkflowRunHandleScope::Global => {
            sqlx::query_scalar!(
                "SELECT status::text AS \"status!\"
             FROM workflow_runs
             WHERE id = $1
               AND organization_id IS NULL
             LIMIT 1",
                workflow_run_id,
            )
            .fetch_optional(pool)
            .await
        }
        WorkflowRunHandleScope::Admin => {
            sqlx::query_scalar!(
                "SELECT status::text AS \"status!\"
             FROM workflow_runs
             WHERE id = $1
             LIMIT 1",
                workflow_run_id,
            )
            .fetch_optional(pool)
            .await
        }
    }
    .map_err(|error| Error::from_query_sqlx_with_context("load workflow handle status", error))?;

    status.map(parse_workflow_run_status).transpose()
}

async fn load_workflow_run_for_scope(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    match scope {
        WorkflowRunHandleScope::Organization(organization_id) => {
            get_workflow_run_by_id(pool, Some(organization_id), workflow_run_id).await
        }
        WorkflowRunHandleScope::Global => {
            load_global_workflow_run_by_id(pool, workflow_run_id).await
        }
        WorkflowRunHandleScope::Admin => get_workflow_run_by_id(pool, None, workflow_run_id).await,
    }
}

async fn load_global_workflow_run_by_id(
    pool: &DbPool,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    let row = sqlx::query_as!(
        WorkflowRunRow,
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            result_step_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1
           AND organization_id IS NULL
         LIMIT 1",
        workflow_run_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("load global workflow run", error))?;

    row.map(WorkflowRunRow::into_record).transpose()
}

async fn load_workflow_run_result(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    let Some(row) = load_workflow_run_result_row(pool, scope, workflow_run_id).await? else {
        return Err(WorkflowRunHandleError::NotFound);
    };

    let status = parse_workflow_run_status(row.status).map_err(WorkflowRunHandleError::from)?;
    let Some(raw_step_key) = row.result_step_key else {
        return Err(WorkflowRunHandleError::ResultNotDeclared);
    };

    match status {
        WorkflowRunStatus::Running | WorkflowRunStatus::WaitingForExternal => {
            Ok(WorkflowRunResultLookup::Pending)
        }
        WorkflowRunStatus::CompletedWithErrors | WorkflowRunStatus::Canceled => {
            Err(WorkflowRunHandleError::UnsuccessfulTerminal { status })
        }
        WorkflowRunStatus::Succeeded => {
            let Some(result) = row.result else {
                return Err(WorkflowRunHandleError::ResultMissing);
            };
            let Some(finished_at) = row.finished_at else {
                return Err(WorkflowRunHandleError::Storage(handle_internal_error(
                    "workflow result row is succeeded without finished_at",
                )));
            };

            Ok(WorkflowRunResultLookup::Ready(WorkflowRunResultRecord {
                workflow_run_id: row.id,
                workflow_type: parse_workflow_type_name(row.workflow_type)
                    .map_err(WorkflowRunHandleError::from)?,
                organization_id: row.organization_id,
                result_step_key: parse_step_key_name(raw_step_key)
                    .map_err(WorkflowRunHandleError::from)?,
                result,
                finished_at,
            }))
        }
    }
}

async fn load_workflow_run_result_row(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> std::result::Result<Option<WorkflowRunResultLookupRow>, WorkflowRunHandleError> {
    let row = match scope {
        WorkflowRunHandleScope::Organization(organization_id) => {
            sqlx::query_as!(
                WorkflowRunResultLookupRow,
                "SELECT
                    id,
                    workflow_type,
                    organization_id,
                    status::text AS \"status!\",
                    result_step_key,
                    result,
                    finished_at
                 FROM workflow_runs
                 WHERE id = $1
                   AND organization_id = $2
                 LIMIT 1",
                workflow_run_id,
                organization_id,
            )
            .fetch_optional(pool)
            .await
        }
        WorkflowRunHandleScope::Global => {
            sqlx::query_as!(
                WorkflowRunResultLookupRow,
                "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                result_step_key,
                result,
                finished_at
             FROM workflow_runs
             WHERE id = $1
               AND organization_id IS NULL
             LIMIT 1",
                workflow_run_id,
            )
            .fetch_optional(pool)
            .await
        }
        WorkflowRunHandleScope::Admin => {
            sqlx::query_as!(
                WorkflowRunResultLookupRow,
                "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                result_step_key,
                result,
                finished_at
             FROM workflow_runs
             WHERE id = $1
             LIMIT 1",
                workflow_run_id,
            )
            .fetch_optional(pool)
            .await
        }
    }
    .map_err(|error| {
        WorkflowRunHandleError::Storage(Error::from_query_sqlx_with_context(
            "load workflow result row",
            error,
        ))
    })?;

    Ok(row)
}

async fn load_workflow_run_result_before_deadline(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    await_before_deadline(
        deadline,
        load_workflow_run_result(pool, scope, workflow_run_id),
    )
    .await?
}

async fn load_workflow_run_result_after_poll_wake(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
    reached_deadline: bool,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    if reached_deadline {
        return load_workflow_run_result_deadline_probe(pool, scope, workflow_run_id).await;
    }

    load_workflow_run_result_before_deadline_or_final_probe(pool, scope, workflow_run_id, deadline)
        .await
}

async fn load_workflow_run_result_after_notification(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    if deadline_has_elapsed(deadline) {
        return load_workflow_run_result_deadline_probe(pool, scope, workflow_run_id).await;
    }

    load_workflow_run_result_before_deadline_or_final_probe(pool, scope, workflow_run_id, deadline)
        .await
}

async fn load_workflow_run_result_before_deadline_or_final_probe(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    match load_workflow_run_result_before_deadline(pool, scope, workflow_run_id, deadline).await {
        Ok(WorkflowRunResultLookup::Pending) if deadline_has_elapsed(deadline) => {
            load_workflow_run_result_deadline_probe(pool, scope, workflow_run_id).await
        }
        Ok(lookup) => Ok(lookup),
        Err(WorkflowRunHandleError::Timeout) if deadline_has_elapsed(deadline) => {
            load_workflow_run_result_deadline_probe(pool, scope, workflow_run_id).await
        }
        Err(error) => Err(error),
    }
}

async fn load_workflow_run_result_deadline_probe(
    pool: &DbPool,
    scope: WorkflowRunHandleScope,
    workflow_run_id: Uuid,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    tokio::time::timeout(
        DEADLINE_RESULT_LOOKUP_TIMEOUT,
        load_workflow_run_result(pool, scope, workflow_run_id),
    )
    .await
    .map_err(|_| WorkflowRunHandleError::Timeout)?
}

fn lookup_ready_or_timeout(
    lookup: WorkflowRunResultLookup,
) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
    match lookup {
        WorkflowRunResultLookup::Ready(record) => Ok(record),
        WorkflowRunResultLookup::Pending => Err(WorkflowRunHandleError::Timeout),
    }
}

fn handle_internal_error(message: &'static str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "workflow.handle_internal_state",
        "Workflow handle state is invalid.",
        message,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_event_needs_lookup_for_match_or_elapsed_deadline() {
        let workflow_run_id = Uuid::now_v7();
        let matching_payload = serde_json::json!({
            "workflow_run_id": workflow_run_id,
        })
        .to_string();
        let unrelated_payload = serde_json::json!({
            "workflow_run_id": Uuid::now_v7(),
        })
        .to_string();
        let future_deadline = Some(instant_after(Duration::from_secs(60)));
        let elapsed_deadline = Some(Instant::now() - Duration::from_millis(1));

        assert!(listener_event_needs_result_lookup(
            &matching_payload,
            workflow_run_id,
            future_deadline,
        ));
        assert!(!listener_event_needs_result_lookup(
            &unrelated_payload,
            workflow_run_id,
            future_deadline,
        ));
        assert!(listener_event_needs_result_lookup(
            &unrelated_payload,
            workflow_run_id,
            elapsed_deadline,
        ));
    }

    #[test]
    fn next_poll_wake_marks_elapsed_deadline_for_final_probe() {
        let wake = next_poll_wake(
            Some(Instant::now() - Duration::from_millis(1)),
            instant_after(Duration::from_secs(30)),
        );

        assert!(wake.reached_deadline);
        assert!(wake.instant <= Instant::now());
    }

    #[test]
    fn instant_after_saturates_oversized_duration() {
        let start = Instant::now();
        let instant = instant_saturating_add(start, Duration::MAX);

        assert!(instant >= start);
    }
}
