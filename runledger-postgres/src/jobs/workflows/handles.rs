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
use super::super::workflow_types::{
    WorkflowRunDbRecord, WorkflowRunHandle, WorkflowRunHandleError, WorkflowRunReadScope,
    WorkflowRunResultRecord, WorkflowRunWaitOptions,
};
use super::enqueue::enqueue_workflow_run;
use super::errors::workflow_active_key_api_required_error;
use super::read::get_workflow_run_by_id_with_scope;
use super::runtime::WORKFLOW_RUN_TERMINAL_CHANNEL;

#[derive(sqlx::FromRow)]
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

#[derive(Clone, Copy)]
enum WaitStart {
    Immediate,
    Waiting,
}

enum ProbeMode {
    DeadlineFinal,
    BeforeDeadlineOrFinal,
    Notification,
    Poll(PollWake),
}

enum WakeReason {
    Notification { needs_lookup: bool },
    ListenerFailed(sqlx::Error),
    Poll(PollWake),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PollWake {
    Poll { instant: Instant },
    Deadline { instant: Instant },
}

impl PollWake {
    fn instant(self) -> Instant {
        match self {
            Self::Poll { instant } | Self::Deadline { instant } => instant,
        }
    }
}

enum WaitDecision {
    Ready(WorkflowRunResultRecord),
    Continue,
    Timeout,
}

impl WaitDecision {
    fn after_standard_probe(lookup: WorkflowRunResultLookup, deadline: Option<Instant>) -> Self {
        match lookup {
            WorkflowRunResultLookup::Ready(record) => Self::Ready(record),
            WorkflowRunResultLookup::Pending if deadline_has_elapsed(deadline) => Self::Timeout,
            WorkflowRunResultLookup::Pending => Self::Continue,
        }
    }

    fn after_poll_probe(lookup: WorkflowRunResultLookup, wake: PollWake) -> Self {
        match lookup {
            WorkflowRunResultLookup::Ready(record) => Self::Ready(record),
            WorkflowRunResultLookup::Pending if matches!(wake, PollWake::Deadline { .. }) => {
                Self::Timeout
            }
            WorkflowRunResultLookup::Pending => Self::Continue,
        }
    }

    fn after_deadline_probe(lookup: WorkflowRunResultLookup) -> Self {
        match lookup {
            WorkflowRunResultLookup::Ready(record) => Self::Ready(record),
            WorkflowRunResultLookup::Pending => Self::Timeout,
        }
    }

    fn into_wait_result(
        self,
    ) -> std::result::Result<Option<WorkflowRunResultRecord>, WorkflowRunHandleError> {
        match self {
            Self::Ready(record) => Ok(Some(record)),
            Self::Continue => Ok(None),
            Self::Timeout => Err(WorkflowRunHandleError::Timeout),
        }
    }

    fn into_deadline_result(
        self,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        match self {
            Self::Ready(record) => Ok(record),
            Self::Continue | Self::Timeout => Err(WorkflowRunHandleError::Timeout),
        }
    }
}

struct WorkflowResultWaiter<'pool> {
    pool: &'pool DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
    start: WaitStart,
    deadline: Option<Instant>,
    poll_interval: Duration,
    listener: Option<PgListener>,
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
        WorkflowResultWaiter::new(&self.pool, self.scope, self.workflow_run_id, options)
            .wait()
            .await
    }
}

impl<'pool> WorkflowResultWaiter<'pool> {
    fn new(
        pool: &'pool DbPool,
        scope: WorkflowRunReadScope,
        workflow_run_id: Uuid,
        options: WorkflowRunWaitOptions,
    ) -> Self {
        let poll_interval = normalize_poll_interval(options.poll_interval);
        let (start, deadline) = match options.timeout {
            Some(Duration::ZERO) => (WaitStart::Immediate, None),
            timeout => (WaitStart::Waiting, timeout.map(instant_after)),
        };

        Self {
            pool,
            scope,
            workflow_run_id,
            start,
            deadline,
            poll_interval,
            listener: None,
        }
    }

    async fn wait(
        mut self,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        match self.start {
            WaitStart::Immediate => self.final_probe_result().await,
            WaitStart::Waiting => self.wait_until_ready().await,
        }
    }

    async fn wait_until_ready(
        &mut self,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        if let Some(record) = self.standard_probe_decision().await?.into_wait_result()? {
            return Ok(record);
        }

        match self.establish_listener().await {
            Ok(()) => {}
            Err(WorkflowRunHandleError::Timeout) if deadline_has_elapsed(self.deadline) => {
                return self.final_probe_result().await;
            }
            Err(error) => return Err(error),
        }
        if deadline_has_elapsed(self.deadline) {
            return self.final_probe_result().await;
        }

        // Close the initial-read/LISTEN race. A terminal transition committed
        // just before LISTEN may not produce a notification for this connection.
        if let Some(record) = self.standard_probe_decision().await?.into_wait_result()? {
            return Ok(record);
        }

        self.wait_for_wakes().await
    }

    async fn establish_listener(&mut self) -> std::result::Result<(), WorkflowRunHandleError> {
        let mut listener = match await_before_deadline(
            self.deadline,
            PgListener::connect_with(self.pool),
        )
        .await?
        {
            Ok(listener) => listener,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "workflow result listener connect failed; using polling"
                );
                return Ok(());
            }
        };

        if !pool_can_spare_query_connection(self.pool) {
            let pool_size = self.pool.size();
            let pool_idle = self.pool.num_idle();
            let max_connections = self.pool.options().get_max_connections();
            tracing::debug!(
                pool_size,
                pool_idle,
                max_connections,
                "workflow result listener skipped; pool has no spare connection for race-closing reads"
            );
            return Ok(());
        }

        if let Err(error) = await_before_deadline(
            self.deadline,
            listener.listen(WORKFLOW_RUN_TERMINAL_CHANNEL),
        )
        .await?
        {
            tracing::warn!(
                error = %error,
                "workflow result listener subscribe failed; using polling"
            );
            return Ok(());
        }

        self.listener = Some(listener);
        Ok(())
    }

    async fn wait_for_wakes(
        &mut self,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        // The poll wake-up is an absolute instant so notification traffic for
        // other workflow runs cannot keep resetting it and starve the polling
        // backstop.
        let mut next_poll = instant_after(self.poll_interval);

        loop {
            let wake = next_poll_wake(self.deadline, next_poll);
            match self.wait_for_wake(wake).await {
                WakeReason::Notification {
                    needs_lookup: false,
                } => {}
                WakeReason::Notification { needs_lookup: true } => {
                    if let Some(record) = self
                        .notification_probe_decision()
                        .await?
                        .into_wait_result()?
                    {
                        return Ok(record);
                    }
                }
                WakeReason::ListenerFailed(error) => {
                    tracing::warn!(
                        error = %error,
                        workflow_run_id = %self.workflow_run_id,
                        "workflow result listener failed; falling back to polling"
                    );
                    self.listener = None;
                    if deadline_has_elapsed(self.deadline) {
                        return self.final_probe_result().await;
                    }
                }
                WakeReason::Poll(wake) => {
                    next_poll = instant_after(self.poll_interval);
                    if let Some(record) =
                        self.poll_probe_decision(wake).await?.into_wait_result()?
                    {
                        return Ok(record);
                    }
                }
            }
        }
    }

    async fn wait_for_wake(&mut self, wake: PollWake) -> WakeReason {
        let workflow_run_id = self.workflow_run_id;
        let deadline = self.deadline;

        if let Some(active_listener) = self.listener.as_mut() {
            tokio::select! {
                notification = active_listener.recv() => {
                    match notification {
                        Ok(notification) => WakeReason::Notification {
                            needs_lookup: listener_event_needs_result_lookup(
                                notification.payload(),
                                workflow_run_id,
                                deadline,
                            ),
                        },
                        Err(error) => WakeReason::ListenerFailed(error),
                    }
                }
                () = sleep_until(wake.instant()) => WakeReason::Poll(wake),
            }
        } else {
            sleep_until(wake.instant()).await;
            WakeReason::Poll(wake)
        }
    }

    async fn standard_probe_decision(
        &self,
    ) -> std::result::Result<WaitDecision, WorkflowRunHandleError> {
        let lookup = self.probe(ProbeMode::BeforeDeadlineOrFinal).await?;
        Ok(WaitDecision::after_standard_probe(lookup, self.deadline))
    }

    async fn notification_probe_decision(
        &self,
    ) -> std::result::Result<WaitDecision, WorkflowRunHandleError> {
        let lookup = self.probe(ProbeMode::Notification).await?;
        Ok(WaitDecision::after_standard_probe(lookup, self.deadline))
    }

    async fn poll_probe_decision(
        &self,
        wake: PollWake,
    ) -> std::result::Result<WaitDecision, WorkflowRunHandleError> {
        let lookup = self.probe(ProbeMode::Poll(wake)).await?;
        Ok(WaitDecision::after_poll_probe(lookup, wake))
    }

    async fn final_probe_result(
        &self,
    ) -> std::result::Result<WorkflowRunResultRecord, WorkflowRunHandleError> {
        let lookup = self.probe(ProbeMode::DeadlineFinal).await?;
        WaitDecision::after_deadline_probe(lookup).into_deadline_result()
    }

    async fn probe(
        &self,
        mode: ProbeMode,
    ) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
        match mode {
            ProbeMode::DeadlineFinal => self.deadline_probe().await,
            ProbeMode::BeforeDeadlineOrFinal => self.before_deadline_or_final_probe().await,
            ProbeMode::Notification => {
                if deadline_has_elapsed(self.deadline) {
                    self.deadline_probe().await
                } else {
                    self.before_deadline_or_final_probe().await
                }
            }
            ProbeMode::Poll(wake) => match wake {
                PollWake::Deadline { .. } => self.deadline_probe().await,
                PollWake::Poll { .. } => self.before_deadline_or_final_probe().await,
            },
        }
    }

    async fn before_deadline_or_final_probe(
        &self,
    ) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
        match load_workflow_run_result_before_deadline(
            self.pool,
            self.scope,
            self.workflow_run_id,
            self.deadline,
        )
        .await
        {
            Ok(WorkflowRunResultLookup::Pending) if deadline_has_elapsed(self.deadline) => {
                self.deadline_probe().await
            }
            Ok(lookup) => Ok(lookup),
            Err(WorkflowRunHandleError::Timeout) if deadline_has_elapsed(self.deadline) => {
                self.deadline_probe().await
            }
            Err(error) => Err(error),
        }
    }

    async fn deadline_probe(
        &self,
    ) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
        tokio::time::timeout(
            DEADLINE_RESULT_LOOKUP_TIMEOUT,
            load_workflow_run_result(self.pool, self.scope, self.workflow_run_id),
        )
        .await
        .map_err(|_| WorkflowRunHandleError::Timeout)?
    }
}

pub fn workflow_run_handle(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
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
    scope: WorkflowRunReadScope,
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
    if payload.active_key().is_some() {
        return Err(workflow_active_key_api_required_error());
    }
    let workflow_run = enqueue_workflow_run(pool, payload).await?;
    let scope = workflow_run
        .organization_id
        .map(WorkflowRunReadScope::Organization)
        .unwrap_or(WorkflowRunReadScope::Global);

    Ok(workflow_run_handle(pool, scope, workflow_run.id))
}

const DEADLINE_RESULT_LOOKUP_TIMEOUT: Duration = Duration::from_millis(250);

fn pool_can_spare_query_connection(pool: &DbPool) -> bool {
    pool.num_idle() > 0 || pool.size() < pool.options().get_max_connections()
}

fn normalize_poll_interval(poll_interval: Duration) -> Duration {
    poll_interval.max(Duration::from_millis(1))
}

fn next_poll_wake(deadline: Option<Instant>, next_poll: Instant) -> PollWake {
    let Some(deadline) = deadline else {
        return PollWake::Poll { instant: next_poll };
    };

    let instant = min(next_poll, deadline);
    if deadline <= next_poll || deadline <= Instant::now() {
        PollWake::Deadline { instant }
    } else {
        PollWake::Poll { instant }
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
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunStatus>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let status = sqlx::query_scalar::<_, String>(
        "SELECT status::text
         FROM workflow_runs
         WHERE id = $1
           AND ($2::bool OR organization_id IS NOT DISTINCT FROM $3::uuid)
         LIMIT 1",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("load workflow handle status", error))?;

    status.map(parse_workflow_run_status).transpose()
}

async fn load_workflow_run_for_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    get_workflow_run_by_id_with_scope(pool, scope, workflow_run_id).await
}

async fn load_workflow_run_result(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
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
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> std::result::Result<Option<WorkflowRunResultLookupRow>, WorkflowRunHandleError> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let row = sqlx::query_as::<_, WorkflowRunResultLookupRow>(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            result_step_key,
            result,
            finished_at
         FROM workflow_runs
         WHERE id = $1
           AND ($2::bool OR organization_id IS NOT DISTINCT FROM $3::uuid)
         LIMIT 1",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_optional(pool)
    .await
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
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
    deadline: Option<Instant>,
) -> std::result::Result<WorkflowRunResultLookup, WorkflowRunHandleError> {
    await_before_deadline(
        deadline,
        load_workflow_run_result(pool, scope, workflow_run_id),
    )
    .await?
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

        let PollWake::Deadline { instant } = wake else {
            panic!("elapsed deadline must select the final probe wake");
        };
        assert!(instant <= Instant::now());
    }

    #[test]
    fn poll_decision_uses_the_pre_sleep_wake_classification() {
        let wake = PollWake::Poll {
            instant: Instant::now() - Duration::from_millis(1),
        };

        assert!(matches!(
            WaitDecision::after_poll_probe(WorkflowRunResultLookup::Pending, wake),
            WaitDecision::Continue
        ));
    }

    #[test]
    fn standard_probe_pending_after_deadline_times_out() {
        let deadline = Some(Instant::now() - Duration::from_millis(1));

        assert!(matches!(
            WaitDecision::after_standard_probe(WorkflowRunResultLookup::Pending, deadline),
            WaitDecision::Timeout
        ));
    }

    #[test]
    fn instant_after_saturates_oversized_duration() {
        let start = Instant::now();
        let instant = instant_saturating_add(start, Duration::MAX);

        assert!(instant >= start);
    }
}
