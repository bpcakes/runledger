use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::config::Config;
use crate::data::{
    DashboardData, DefinitionsData, JobDetailData, JobsData, QueueStatusFilter, WorkflowDetailData,
    WorkflowsData,
};
use crate::scope::Scope;

pub(crate) mod fetch;
mod input;

use self::fetch::FetchOutcome;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopScreen {
    Dashboard,
    Queue,
    Workflows,
    Definitions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Dashboard,
    Queue,
    JobDetail { job_id: Uuid },
    Workflows,
    WorkflowDetail { run_id: Uuid },
    Definitions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobDetailPane {
    Summary,
    Events,
    Logs,
    Payload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterTarget {
    Job,
    Workflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActiveInput {
    None,
    Organization { text: String },
    Filter { target: FilterTarget, text: String },
    Search { text: String },
    Command { text: String },
}

impl ActiveInput {
    fn allows_fetch(&self) -> bool {
        !matches!(self, Self::Organization { .. } | Self::Filter { .. })
    }

    fn text_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::None => None,
            Self::Organization { text }
            | Self::Filter { text, .. }
            | Self::Search { text }
            | Self::Command { text } => Some(text),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ViewState {
    pub list_selection: usize,
    pub detail_scroll: usize,
    pub job_detail_pane: JobDetailPane,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            list_selection: 0,
            detail_scroll: 0,
            job_detail_pane: JobDetailPane::Summary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScreenFrame {
    pub screen: Screen,
    pub state: ViewState,
}

pub struct App {
    pub config: Config,
    pub scope: Scope,
    pub screen: Screen,
    pub screen_stack: Vec<ScreenFrame>,
    pub top_view_states: [ViewState; 4],
    pub list_selection: usize,
    pub detail_scroll: usize,
    pub payload_visible_rows: usize,
    pub payload_raw: bool,
    pub payload_wrap: bool,
    pub queue_filter: QueueStatusFilter,
    pub job_type_filter: Option<String>,
    pub workflow_type_filter: Option<String>,
    pub job_detail_pane: JobDetailPane,
    pub show_help: bool,
    active_input: ActiveInput,
    pub table_search: Option<String>,
    pub refresh_paused: bool,
    pub dashboard: Option<DashboardData>,
    pub jobs: Option<JobsData>,
    pub job_detail: Option<JobDetailData>,
    pub workflows: Option<WorkflowsData>,
    pub workflow_detail: Option<WorkflowDetailData>,
    pub definitions: Option<DefinitionsData>,
    pub last_error: Option<String>,
    pub notice: Option<String>,
    pub last_refresh: Option<Instant>,
    pub last_fetch_duration: Option<Duration>,
    pub fetching: bool,
    pub should_quit: bool,
    fetch_generation: Arc<AtomicU64>,
}

impl App {
    const TOP_SCREEN_COUNT: usize = 4;

    pub fn new(config: Config, fetch_generation: Arc<AtomicU64>) -> Self {
        let scope = config.org.map(Scope::for_org).unwrap_or_else(Scope::global);
        Self {
            config,
            scope,
            screen: Screen::Dashboard,
            screen_stack: Vec::new(),
            top_view_states: [ViewState::default(); Self::TOP_SCREEN_COUNT],
            list_selection: 0,
            detail_scroll: 0,
            payload_visible_rows: 1,
            payload_raw: false,
            payload_wrap: false,
            queue_filter: QueueStatusFilter::All,
            job_type_filter: None,
            workflow_type_filter: None,
            job_detail_pane: JobDetailPane::Summary,
            show_help: false,
            active_input: ActiveInput::None,
            table_search: None,
            refresh_paused: false,
            dashboard: None,
            jobs: None,
            job_detail: None,
            workflows: None,
            workflow_detail: None,
            definitions: None,
            last_error: None,
            notice: None,
            last_refresh: None,
            last_fetch_duration: None,
            fetching: false,
            should_quit: false,
            fetch_generation,
        }
    }

    fn bump_fetch_generation(&self) {
        self.fetch_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn active_input(&self) -> &ActiveInput {
        &self.active_input
    }

    pub(crate) fn allows_fetch(&self) -> bool {
        self.active_input.allows_fetch()
    }

    pub fn top_screen_index(&self) -> usize {
        match self.top_screen() {
            TopScreen::Dashboard => 0,
            TopScreen::Queue => 1,
            TopScreen::Workflows => 2,
            TopScreen::Definitions => 3,
        }
    }

    pub fn top_screen(&self) -> TopScreen {
        match &self.screen {
            Screen::Dashboard => TopScreen::Dashboard,
            Screen::Queue | Screen::JobDetail { .. } => TopScreen::Queue,
            Screen::Workflows | Screen::WorkflowDetail { .. } => TopScreen::Workflows,
            Screen::Definitions => TopScreen::Definitions,
        }
    }

    pub fn screen_title(&self) -> &'static str {
        match &self.screen {
            Screen::Dashboard => "Dashboard",
            Screen::Queue => "Queue",
            Screen::JobDetail { .. } => "Job",
            Screen::Workflows => "Workflows",
            Screen::WorkflowDetail { .. } => "Workflow",
            Screen::Definitions => "Definitions",
        }
    }

    fn capture_view_state(&self) -> ViewState {
        ViewState {
            list_selection: self.list_selection,
            detail_scroll: self.detail_scroll,
            job_detail_pane: self.job_detail_pane,
        }
    }

    fn restore_view_state(&mut self, state: ViewState) {
        self.list_selection = state.list_selection;
        self.detail_scroll = state.detail_scroll;
        self.job_detail_pane = state.job_detail_pane;
        self.clamp_selection();
    }

    fn save_current_top_view_state(&mut self) {
        if self.screen_stack.is_empty() && self.is_top_level_screen() {
            self.top_view_states[self.top_screen_index()] = self.capture_view_state();
        }
    }

    fn is_top_level_screen(&self) -> bool {
        matches!(
            self.screen,
            Screen::Dashboard | Screen::Queue | Screen::Workflows | Screen::Definitions
        )
    }

    pub fn status_line(&self) -> String {
        let rows = self.list_len();
        let selected = if rows == 0 {
            "row 0/0".to_owned()
        } else {
            format!("row {}/{}", self.list_selection.min(rows - 1) + 1, rows)
        };
        let refresh = self.refresh_status_label();
        let query = self
            .last_fetch_duration
            .map(format_duration)
            .unwrap_or_else(|| "query --".to_owned());
        let filters = self.filter_status_label();
        let message = self
            .notice
            .as_ref()
            .or(self.last_error.as_ref())
            .map(|e| format!(" | {}", truncate_status(e, 52)))
            .unwrap_or_default();
        format!(
            "{} | {} | {} | {} | {}{}",
            self.screen_title(),
            selected,
            filters,
            refresh,
            query,
            message,
        )
    }

    pub fn key_hint_line(&self) -> &'static str {
        match self.screen {
            Screen::Dashboard => {
                "Enter queue filter | / search | t type | c clear | p pause | r refresh | ? help | q quit"
            }
            Screen::Queue => {
                "Enter/l open | f status | / search | t type | c clear | y copy id | g/G top/end | p pause"
            }
            Screen::JobDetail { .. } => {
                "h/Esc back | [/] panes | v wrap | R raw | y copy id | . refresh | p pause"
            }
            Screen::Workflows => {
                "Enter/l open | / search | t type | c clear | y copy id | g/G top/end | p pause"
            }
            Screen::WorkflowDetail { .. } => {
                if self.selected_workflow_step_job_id().is_some() {
                    "Enter/l open job | h/Esc back | / search | y copy id | g/G top/end | p pause"
                } else {
                    "h/Esc back | / search | y copy id | g/G top/end | p pause"
                }
            }
            Screen::Definitions => {
                "/ search | t type | c clear | g/G top/end | p pause | r refresh"
            }
        }
    }

    fn refresh_status_label(&self) -> String {
        let age = self
            .last_refresh
            .map(|t| format_duration_short(t.elapsed()))
            .unwrap_or_else(|| "never".to_owned());
        let paused = if self.refresh_paused { " paused" } else { "" };
        let fetch = if self.fetching { " fetching" } else { "" };
        format!("refresh {age}{paused}{fetch}")
    }

    fn filter_status_label(&self) -> String {
        let search = self
            .table_search
            .as_deref()
            .map(|q| format!(" search='{q}'"))
            .unwrap_or_default();
        match self.screen {
            Screen::Dashboard => format!("scope {}", self.scope.label()) + &search,
            Screen::Queue | Screen::JobDetail { .. } => format!(
                "scope {} status {} type {}{}",
                self.scope.label(),
                self.queue_filter.label(),
                self.job_type_filter.as_deref().unwrap_or("any"),
                search,
            ),
            Screen::Workflows | Screen::WorkflowDetail { .. } => format!(
                "scope {} workflow {}{}",
                self.scope.label(),
                self.workflow_type_filter.as_deref().unwrap_or("any"),
                search,
            ),
            Screen::Definitions => format!(
                "type {}{}",
                self.job_type_filter.as_deref().unwrap_or("any"),
                search,
            ),
        }
    }

    pub fn navigate_top(&mut self, top: TopScreen) {
        self.save_current_top_view_state();
        self.screen_stack.clear();
        self.screen = Self::screen_from_top(top);
        self.restore_view_state(self.top_view_states[self.top_screen_index()]);
        self.bump_fetch_generation();
    }

    fn screen_from_top(top: TopScreen) -> Screen {
        match top {
            TopScreen::Dashboard => Screen::Dashboard,
            TopScreen::Queue => Screen::Queue,
            TopScreen::Workflows => Screen::Workflows,
            TopScreen::Definitions => Screen::Definitions,
        }
    }

    pub(super) fn invalidate_cache(&mut self) {
        self.dashboard = None;
        self.jobs = None;
        self.job_detail = None;
        self.workflows = None;
        self.workflow_detail = None;
        self.definitions = None;
        self.bump_fetch_generation();
    }

    pub fn push_job_detail(&mut self, job_id: Uuid) {
        self.bump_fetch_generation();
        self.screen_stack.push(ScreenFrame {
            screen: self.screen.clone(),
            state: self.capture_view_state(),
        });
        self.screen = Screen::JobDetail { job_id };
        self.job_detail = None;
        self.list_selection = 0;
        self.detail_scroll = 0;
        self.job_detail_pane = JobDetailPane::Summary;
    }

    pub fn push_workflow_detail(&mut self, run_id: Uuid) {
        self.bump_fetch_generation();
        self.screen_stack.push(ScreenFrame {
            screen: self.screen.clone(),
            state: self.capture_view_state(),
        });
        self.screen = Screen::WorkflowDetail { run_id };
        self.workflow_detail = None;
        self.list_selection = 0;
    }

    pub fn pop_screen(&mut self) {
        if let Some(prev) = self.screen_stack.pop() {
            self.bump_fetch_generation();
            self.screen = prev.screen;
            self.restore_view_state(prev.state);
        }
    }

    pub fn list_len(&self) -> usize {
        match &self.screen {
            Screen::Dashboard => self.dashboard.as_ref().map_or(0, |d| {
                d.metrics
                    .iter()
                    .filter(|metric| self.dashboard_metric_matches_search(d, metric))
                    .count()
            }),
            Screen::Queue => self.jobs.as_ref().map_or(0, |d| {
                d.jobs.iter().filter(|j| self.job_matches_search(j)).count()
            }),
            Screen::JobDetail { .. } => match self.job_detail_pane {
                JobDetailPane::Events => self.job_detail.as_ref().map_or(0, |d| {
                    d.events
                        .iter()
                        .filter(|event| self.job_event_matches_search(event))
                        .count()
                }),
                JobDetailPane::Logs => self.job_detail.as_ref().map_or(0, |d| {
                    d.logs
                        .iter()
                        .filter(|l| {
                            self.matches_table_search(|| {
                                vec![l.id.to_string(), l.level.clone(), l.message.clone()]
                            })
                        })
                        .count()
                }),
                _ => 0,
            },
            Screen::Workflows => self.workflows.as_ref().map_or(0, |d| {
                d.runs
                    .iter()
                    .filter(|r| {
                        self.matches_table_search(|| {
                            vec![
                                r.id.to_string(),
                                r.workflow_type.as_str().to_owned(),
                                crate::format::workflow_run_status_label(r.status).to_owned(),
                            ]
                        })
                    })
                    .count()
            }),
            Screen::WorkflowDetail { .. } => self.workflow_detail.as_ref().map_or(0, |d| {
                d.steps
                    .iter()
                    .filter(|s| {
                        self.matches_table_search(|| {
                            vec![
                                s.step_key.as_str().to_owned(),
                                crate::format::workflow_step_status_label(s.status).to_owned(),
                                s.job_type
                                    .as_ref()
                                    .map(|t| t.as_str().to_owned())
                                    .unwrap_or_default(),
                                s.job_id.map(|id| id.to_string()).unwrap_or_default(),
                            ]
                        })
                    })
                    .count()
            }),
            Screen::Definitions => self.definitions.as_ref().map_or(0, |d| {
                d.definitions
                    .iter()
                    .filter(|def| {
                        self.matches_table_search(|| {
                            vec![
                                def.job_type.as_str().to_owned(),
                                def.version.to_string(),
                                if def.is_enabled {
                                    "enabled"
                                } else {
                                    "disabled"
                                }
                                .to_owned(),
                            ]
                        })
                    })
                    .count()
            }),
        }
    }

    pub(crate) fn apply_fetch(&mut self, outcome: FetchOutcome, duration: Duration) {
        self.fetching = false;
        self.last_fetch_duration = Some(duration);
        match outcome {
            FetchOutcome::Dashboard(Ok(data)) => {
                self.dashboard = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::Jobs(Ok(data)) => {
                self.jobs = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::JobDetail(Ok(data)) if matches!(&self.screen, Screen::JobDetail { job_id } if *job_id == data.job.id) =>
            {
                self.job_detail = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::Workflows(Ok(data)) => {
                self.workflows = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::WorkflowDetail(Ok(data))
                if matches!(
                    &self.screen,
                    Screen::WorkflowDetail { run_id } if *run_id == data.run.id
                ) =>
            {
                self.workflow_detail = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::Definitions(Ok(data)) => {
                self.definitions = Some(*data);
                self.last_error = None;
                self.last_refresh = Some(Instant::now());
            }
            FetchOutcome::Dashboard(Err(e))
            | FetchOutcome::Jobs(Err(e))
            | FetchOutcome::JobDetail(Err(e))
            | FetchOutcome::Workflows(Err(e))
            | FetchOutcome::WorkflowDetail(Err(e))
            | FetchOutcome::Definitions(Err(e)) => {
                self.last_error = Some(e);
            }
            _ => {}
        }
        self.clamp_selection();
    }

    pub(super) fn clamp_selection(&mut self) {
        let len = self.list_len();
        if len == 0 {
            self.list_selection = 0;
        } else if self.list_selection >= len {
            self.list_selection = len - 1;
        }
    }

    pub fn table_search_query(&self) -> Option<&str> {
        self.table_search.as_deref().filter(|q| !q.is_empty())
    }

    pub fn matches_table_search<I, S, F>(&self, fields: F) -> bool
    where
        F: FnOnce() -> I,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Some(query) = self.table_search_query() else {
            return true;
        };
        let query = query.to_ascii_lowercase();
        fields()
            .into_iter()
            .any(|field| field.as_ref().to_ascii_lowercase().contains(&query))
    }

    pub(crate) fn dashboard_metric_matches_search(
        &self,
        data: &DashboardData,
        metric: &runledger_postgres::jobs::JobMetricsRecord,
    ) -> bool {
        self.matches_table_search(|| data.row_for(metric).into_fields())
    }

    pub(crate) fn job_event_matches_search(
        &self,
        event: &runledger_postgres::jobs::JobEventRecord,
    ) -> bool {
        self.matches_table_search(|| {
            vec![
                event.id.to_string(),
                event.event_type.as_db_value().to_owned(),
                event
                    .stage
                    .map(|stage| stage.as_db_value())
                    .unwrap_or("")
                    .to_owned(),
                event.payload.to_string(),
            ]
        })
    }

    fn job_matches_search(&self, job: &runledger_postgres::jobs::JobQueueRecord) -> bool {
        self.matches_table_search(|| {
            vec![
                job.id.to_string(),
                job.job_type.as_str().to_owned(),
                crate::format::job_status_label(job.status).to_owned(),
                job.stage.as_db_value().to_owned(),
                job.worker_id.as_deref().unwrap_or("").to_owned(),
            ]
        })
    }
}

impl JobDetailPane {
    fn next(self) -> Self {
        match self {
            Self::Summary => Self::Events,
            Self::Events => Self::Logs,
            Self::Logs => Self::Payload,
            Self::Payload => Self::Summary,
        }
    }

    fn prev(self) -> Self {
        match self {
            Self::Summary => Self::Payload,
            Self::Events => Self::Summary,
            Self::Logs => Self::Events,
            Self::Payload => Self::Logs,
        }
    }
}

fn truncate_status(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    let mut end = max;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

fn format_duration(duration: Duration) -> String {
    format!("query {:.0}ms", duration.as_secs_f64() * 1000.0)
}

fn format_duration_short(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}h", seconds / 3600)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use chrono::Utc;
    use runledger_core::jobs::{
        JobEventType, JobStage, JobStatus, JobTypeName, StepKeyName, WorkflowRunStatus,
        WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
    };
    use runledger_postgres::jobs::{
        JobEventRecord, JobQueueRecord, WorkflowRunDbRecord, WorkflowStepDbRecord,
    };

    use crate::data::DashboardContinuationMetrics;

    fn test_config() -> Config {
        Config {
            database_url: "postgres://example/runledger".to_owned(),
            org: None,
            refresh_ms: 2000,
            limit: 100,
            skip_schema_check: false,
        }
    }

    fn job_record_with_payload(id: Uuid, payload: serde_json::Value) -> JobQueueRecord {
        let now = Utc::now();
        JobQueueRecord {
            id,
            job_type: JobTypeName::new("jobs.test").expect("valid job type"),
            organization_id: None,
            payload,
            status: JobStatus::Pending,
            priority: 0,
            run_number: 1,
            attempt: 0,
            max_attempts: 3,
            timeout_seconds: 300,
            next_run_at: now,
            lease_expires_at: None,
            last_heartbeat_at: None,
            worker_id: None,
            started_at: None,
            finished_at: None,
            stage: JobStage::Queued,
            progress_done: None,
            progress_total: None,
            progress_pct: None,
            checkpoint: None,
            output: None,
            idempotency_key: None,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn job_metrics(job_type: &str) -> runledger_postgres::jobs::JobMetricsRecord {
        runledger_postgres::jobs::JobMetricsRecord {
            job_type: JobTypeName::new(job_type).expect("valid job type"),
            pending_count: 0,
            leased_count: 0,
            stale_leases: 0,
            succeeded_24h: 0,
            retryable_24h: 0,
            terminal_24h: 0,
            panicked_24h: 0,
            timeout_24h: 0,
            dead_lettered_24h: 0,
            p50_duration_ms_24h: None,
            p95_duration_ms_24h: None,
        }
    }

    fn enqueued_event(id: i64, job_id: Uuid, payload: serde_json::Value) -> JobEventRecord {
        JobEventRecord {
            id,
            job_id,
            run_number: 1,
            attempt: None,
            event_type: JobEventType::Enqueued,
            stage: Some(JobStage::Queued),
            progress_done: None,
            progress_total: None,
            payload,
            occurred_at: Utc::now(),
        }
    }

    fn workflow_detail_with_step(job_id: Option<Uuid>) -> WorkflowDetailData {
        let now = Utc::now();
        let run_id = Uuid::new_v4();
        let is_job = job_id.is_some();
        WorkflowDetailData {
            run: WorkflowRunDbRecord {
                id: run_id,
                workflow_type: WorkflowTypeName::new("workflows.test")
                    .expect("valid workflow type"),
                organization_id: None,
                status: WorkflowRunStatus::Running,
                idempotency_key: None,
                result_step_key: None,
                metadata: serde_json::json!({}),
                started_at: now,
                finished_at: None,
                created_at: now,
                updated_at: now,
            },
            steps: vec![WorkflowStepDbRecord {
                id: Uuid::new_v4(),
                workflow_run_id: run_id,
                step_key: StepKeyName::new("step.test").expect("valid step key"),
                execution_kind: if is_job {
                    WorkflowStepExecutionKind::Job
                } else {
                    WorkflowStepExecutionKind::External
                },
                job_type: is_job.then(|| JobTypeName::new("jobs.test").expect("valid job type")),
                organization_id: None,
                payload: serde_json::json!({}),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                stage: is_job.then_some(JobStage::Queued),
                allow_handler_continuation: false,
                execution_resource_key: None,
                status: if is_job {
                    WorkflowStepStatus::Enqueued
                } else {
                    WorkflowStepStatus::WaitingForExternal
                },
                job_id,
                released_at: None,
                started_at: None,
                finished_at: None,
                dependency_count_total: 0,
                dependency_count_pending: 0,
                dependency_count_unsatisfied: 0,
                status_reason: None,
                last_error_code: None,
                last_error_message: None,
                output: None,
                created_at: now,
                updated_at: now,
            }],
            dependencies: Vec::new(),
            steps_total: 1,
            dependencies_total: 0,
        }
    }

    fn key(code: crossterm::event::KeyCode) -> crossterm::event::KeyEvent {
        crossterm::event::KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
    }

    #[test]
    fn active_input_representation_keeps_modal_modes_mutually_exclusive() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));

        assert_eq!(app.active_input(), &ActiveInput::None);

        app.handle_key(key(crossterm::event::KeyCode::Char('o')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Organization {
                text: String::new()
            }
        );

        app.handle_key(key(crossterm::event::KeyCode::Char(':')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Organization {
                text: ":".to_owned()
            },
            "an active organization input routes character keys to its current buffer"
        );

        app.handle_key(key(crossterm::event::KeyCode::Esc));
        app.handle_key(key(crossterm::event::KeyCode::Char(':')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Command {
                text: String::new()
            }
        );
    }

    #[test]
    fn only_organization_and_filter_inputs_block_fetches() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));

        assert!(app.allows_fetch());

        app.handle_key(key(crossterm::event::KeyCode::Char('o')));
        assert!(!app.allows_fetch());
        app.handle_key(key(crossterm::event::KeyCode::Esc));

        app.handle_key(key(crossterm::event::KeyCode::Char('t')));
        assert!(!app.allows_fetch());
        app.handle_key(key(crossterm::event::KeyCode::Esc));

        app.handle_key(key(crossterm::event::KeyCode::Char('/')));
        assert!(app.allows_fetch());
        app.handle_key(key(crossterm::event::KeyCode::Esc));

        app.handle_key(key(crossterm::event::KeyCode::Char(':')));
        assert!(app.allows_fetch());
    }

    #[test]
    fn ignored_keys_preserve_every_active_input_variant() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        let cases = [
            (
                ActiveInput::Organization {
                    text: "organization".to_owned(),
                },
                crossterm::event::KeyCode::Left,
            ),
            (
                ActiveInput::Filter {
                    target: FilterTarget::Workflow,
                    text: "workflow".to_owned(),
                },
                crossterm::event::KeyCode::Tab,
            ),
            (
                ActiveInput::Search {
                    text: "search".to_owned(),
                },
                crossterm::event::KeyCode::Down,
            ),
            (
                ActiveInput::Command {
                    text: "command".to_owned(),
                },
                crossterm::event::KeyCode::PageUp,
            ),
        ];

        for (active_input, ignored_key) in cases {
            app.active_input = active_input.clone();
            assert!(!app.handle_key(key(ignored_key)));
            assert_eq!(app.active_input(), &active_input);
        }
    }

    #[test]
    fn invalid_organization_input_closes_without_changing_scope_or_cache() {
        let fetch_generation = Arc::new(AtomicU64::new(0));
        let mut app = App::new(test_config(), fetch_generation.clone());
        let original_scope = Scope::for_org(Uuid::new_v4());
        app.scope = original_scope;
        app.jobs = Some(JobsData { jobs: Vec::new() });
        app.active_input = ActiveInput::Organization {
            text: "not-a-uuid".to_owned(),
        };

        assert!(!app.handle_key(key(crossterm::event::KeyCode::Enter)));
        assert_eq!(app.scope, original_scope);
        assert_eq!(app.last_error.as_deref(), Some("Invalid organization UUID"));
        assert!(app.jobs.is_some());
        assert_eq!(fetch_generation.load(Ordering::Acquire), 0);
        assert_eq!(app.active_input(), &ActiveInput::None);
    }

    #[test]
    fn command_input_preserves_refresh_and_non_refresh_return_semantics() {
        let fetch_generation = Arc::new(AtomicU64::new(0));
        let mut app = App::new(test_config(), fetch_generation.clone());
        app.scope = Scope::for_org(Uuid::new_v4());
        app.active_input = ActiveInput::Command {
            text: "scope global".to_owned(),
        };

        assert!(app.handle_key(key(crossterm::event::KeyCode::Enter)));
        assert_eq!(app.scope, Scope::global());
        assert_eq!(fetch_generation.load(Ordering::Acquire), 1);
        assert_eq!(app.active_input(), &ActiveInput::None);

        app.active_input = ActiveInput::Command {
            text: "refresh 5s".to_owned(),
        };
        assert!(!app.handle_key(key(crossterm::event::KeyCode::Enter)));
        assert_eq!(app.config.refresh_ms, 5_000);
        assert_eq!(fetch_generation.load(Ordering::Acquire), 1);
        assert_eq!(app.active_input(), &ActiveInput::None);
    }

    #[test]
    fn type_filter_input_selects_the_target_and_invalidates_its_cache() {
        let fetch_generation = Arc::new(AtomicU64::new(0));
        let mut app = App::new(test_config(), fetch_generation.clone());
        app.jobs = Some(JobsData { jobs: Vec::new() });
        app.definitions = Some(DefinitionsData {
            definitions: Vec::new(),
        });

        app.handle_key(key(crossterm::event::KeyCode::Char('t')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Filter {
                target: FilterTarget::Job,
                text: String::new()
            }
        );
        app.handle_key(key(crossterm::event::KeyCode::Char('j')));
        assert!(app.handle_key(key(crossterm::event::KeyCode::Enter)));
        assert_eq!(app.job_type_filter.as_deref(), Some("j"));
        assert!(app.jobs.is_none());
        assert!(app.definitions.is_none());
        assert_eq!(fetch_generation.load(Ordering::Acquire), 1);

        app.screen = Screen::Workflows;
        app.workflows = Some(WorkflowsData { runs: Vec::new() });
        app.handle_key(key(crossterm::event::KeyCode::Char('t')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Filter {
                target: FilterTarget::Workflow,
                text: String::new()
            }
        );
        app.handle_key(key(crossterm::event::KeyCode::Char('w')));
        assert!(app.handle_key(key(crossterm::event::KeyCode::Enter)));
        assert_eq!(app.workflow_type_filter.as_deref(), Some("w"));
        assert!(app.workflows.is_none());
        assert_eq!(fetch_generation.load(Ordering::Acquire), 2);
    }

    #[test]
    fn search_input_edits_without_scheduling_a_fetch_and_resets_selection() {
        let fetch_generation = Arc::new(AtomicU64::new(0));
        let mut app = App::new(test_config(), fetch_generation.clone());
        app.screen = Screen::Queue;
        app.jobs = Some(JobsData { jobs: Vec::new() });
        app.table_search = Some("saved".to_owned());
        app.list_selection = 9;

        app.handle_key(key(crossterm::event::KeyCode::Char('/')));
        assert_eq!(
            app.active_input(),
            &ActiveInput::Search {
                text: "saved".to_owned()
            }
        );
        app.handle_key(key(crossterm::event::KeyCode::Backspace));
        app.handle_key(key(crossterm::event::KeyCode::Char('d')));
        assert!(!app.handle_key(key(crossterm::event::KeyCode::Enter)));

        assert_eq!(app.table_search.as_deref(), Some("saved"));
        assert_eq!(app.list_selection, 0);
        assert_eq!(fetch_generation.load(Ordering::Acquire), 0);
        assert_eq!(app.active_input(), &ActiveInput::None);
    }

    #[test]
    fn table_search_does_not_build_fields_without_a_query() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        let build_count = Cell::new(0);

        assert!(app.matches_table_search(|| {
            build_count.set(build_count.get() + 1);
            ["needle"]
        }));
        assert_eq!(build_count.get(), 0);

        app.table_search = Some("needle".to_owned());
        assert!(app.matches_table_search(|| {
            build_count.set(build_count.get() + 1);
            ["needle"]
        }));
        assert_eq!(build_count.get(), 1);
    }

    #[test]
    fn workflow_detail_open_hint_tracks_selected_step_job() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        let external_detail = workflow_detail_with_step(None);
        app.screen = Screen::WorkflowDetail {
            run_id: external_detail.run.id,
        };
        app.workflow_detail = Some(external_detail);

        assert!(!app.key_hint_line().contains("open job"));

        let job_detail = workflow_detail_with_step(Some(Uuid::new_v4()));
        app.screen = Screen::WorkflowDetail {
            run_id: job_detail.run.id,
        };
        app.workflow_detail = Some(job_detail);

        assert!(app.key_hint_line().contains("Enter/l open job"));
    }

    #[test]
    fn payload_only_event_search_keeps_count_and_selection_aligned() {
        const SEARCH_SENTINEL: &str = "payload-only-replay-reason";

        let job_id = Uuid::new_v4();
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.screen = Screen::JobDetail { job_id };
        app.job_detail_pane = JobDetailPane::Events;
        app.table_search = Some(SEARCH_SENTINEL.to_owned());
        app.job_detail = Some(JobDetailData {
            job: job_record_with_payload(job_id, serde_json::json!({})),
            events: vec![
                enqueued_event(10, job_id, serde_json::json!({"job_type": "jobs.test"})),
                enqueued_event(
                    11,
                    job_id,
                    serde_json::json!({
                        "job_type": "jobs.test",
                        "reason": SEARCH_SENTINEL
                    }),
                ),
            ],
            logs: Vec::new(),
            workflow_run_id: None,
        });

        let matching_event_ids = app
            .job_detail
            .as_ref()
            .expect("job detail")
            .events
            .iter()
            .filter(|event| app.job_event_matches_search(event))
            .map(|event| event.id)
            .collect::<Vec<_>>();

        assert_eq!(app.list_len(), matching_event_ids.len());
        assert_eq!(matching_event_ids, vec![11]);
        assert_eq!(matching_event_ids[app.list_selection], 11);
    }

    #[test]
    fn every_rendered_dashboard_field_search_activates_the_matching_job_type() {
        const TARGET_JOB_TYPE: &str = "jobs.continuation_target";
        let mut target = job_metrics(TARGET_JOB_TYPE);
        target.pending_count = 10_101;
        target.leased_count = 20_202;
        target.stale_leases = 30_303;
        target.succeeded_24h = 70_707;
        target.dead_lettered_24h = 80_808;
        target.p50_duration_ms_24h = Some(90_909.4);
        target.p95_duration_ms_24h = Some(100_010.6);
        let mut other = job_metrics("jobs.other");
        other.p50_duration_ms_24h = Some(1.0);
        other.p95_duration_ms_24h = Some(2.0);
        let dashboard = DashboardData {
            metrics: vec![other, target],
            continuation_metrics: std::collections::BTreeMap::from([(
                TARGET_JOB_TYPE.to_owned(),
                DashboardContinuationMetrics {
                    continued_24h: 40_404,
                    active_continued_count: 50_505,
                    max_active_run_number: 60_606,
                },
            )]),
            failed_workflows: 0,
            external_waits: 0,
        };

        for query in [
            "continuation_target",
            "10101",
            "20202",
            "30303",
            "40404",
            "50505",
            "60606",
            "70707",
            "80808",
            "90909",
            "100011",
        ] {
            let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
            app.table_search = Some(query.to_owned());
            app.dashboard = Some(dashboard.clone());

            assert_eq!(app.list_len(), 1, "query {query}");

            let refresh = app.handle_key(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Enter,
                crossterm::event::KeyModifiers::NONE,
            ));

            assert!(refresh, "query {query}");
            assert_eq!(app.screen, Screen::Queue, "query {query}");
            assert_eq!(
                app.job_type_filter.as_deref(),
                Some(TARGET_JOB_TYPE),
                "query {query}"
            );
        }

        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.table_search = Some("90909.4".to_owned());
        app.dashboard = Some(dashboard);
        assert_eq!(app.list_len(), 0, "search uses the rendered rounded value");

        let mut duration_other = job_metrics("jobs.duration_other");
        duration_other.p50_duration_ms_24h = Some(1.0);
        duration_other.p95_duration_ms_24h = Some(2.0);
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.table_search = Some("—".to_owned());
        app.dashboard = Some(DashboardData {
            metrics: vec![duration_other, job_metrics("jobs.no_duration_target")],
            continuation_metrics: std::collections::BTreeMap::new(),
            failed_workflows: 0,
            external_waits: 0,
        });

        assert_eq!(
            app.list_len(),
            1,
            "rendered em dash matches missing duration"
        );
        let refresh = app.handle_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        assert!(refresh);
        assert_eq!(app.screen, Screen::Queue);
        assert_eq!(
            app.job_type_filter.as_deref(),
            Some("jobs.no_duration_target")
        );
    }

    #[test]
    fn payload_scroll_uses_last_visible_height_for_up_and_down() {
        let job_id = Uuid::new_v4();
        let payload = serde_json::json!({
            "lines": (0..30).collect::<Vec<_>>()
        });
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.screen = Screen::JobDetail { job_id };
        app.job_detail_pane = JobDetailPane::Payload;
        app.job_detail = Some(JobDetailData {
            job: job_record_with_payload(job_id, payload),
            events: Vec::new(),
            logs: Vec::new(),
            workflow_run_id: None,
        });

        app.update_payload_visible_rows(8);
        let max_scroll = app.payload_scroll_max();
        assert!(max_scroll > 0);

        for _ in 0..(max_scroll + 10) {
            app.move_selection(1);
        }
        assert_eq!(app.detail_scroll, max_scroll);

        app.move_selection(-1);
        assert_eq!(app.detail_scroll, max_scroll - 1);
    }

    #[test]
    fn top_navigation_preserves_selection_for_cached_screens() {
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.screen = Screen::Queue;
        app.jobs = Some(JobsData {
            jobs: vec![
                job_record_with_payload(Uuid::new_v4(), serde_json::json!({"i": 0})),
                job_record_with_payload(Uuid::new_v4(), serde_json::json!({"i": 1})),
                job_record_with_payload(Uuid::new_v4(), serde_json::json!({"i": 2})),
            ],
        });
        app.list_selection = 2;

        app.navigate_top(TopScreen::Dashboard);
        assert_eq!(app.screen, Screen::Dashboard);

        app.navigate_top(TopScreen::Queue);
        assert_eq!(app.screen, Screen::Queue);
        assert_eq!(app.list_selection, 2);
    }

    #[test]
    fn popping_detail_restores_parent_selection() {
        let job_ids = [Uuid::new_v4(), Uuid::new_v4()];
        let mut app = App::new(test_config(), Arc::new(AtomicU64::new(0)));
        app.screen = Screen::Queue;
        app.jobs = Some(JobsData {
            jobs: vec![
                job_record_with_payload(job_ids[0], serde_json::json!({"i": 0})),
                job_record_with_payload(job_ids[1], serde_json::json!({"i": 1})),
            ],
        });
        app.list_selection = 1;

        app.push_job_detail(job_ids[1]);
        assert_eq!(app.list_selection, 0);

        app.pop_screen();
        assert_eq!(app.screen, Screen::Queue);
        assert_eq!(app.list_selection, 1);
    }
}
