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
    pub show_org_input: bool,
    pub org_input: String,
    pub show_filter_input: bool,
    pub filter_input: String,
    pub filter_input_workflow: bool,
    pub show_search_input: bool,
    pub search_input: String,
    pub table_search: Option<String>,
    pub show_command_input: bool,
    pub command_input: String,
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
            show_org_input: false,
            org_input: String::new(),
            show_filter_input: false,
            filter_input: String::new(),
            filter_input_workflow: false,
            show_search_input: false,
            search_input: String::new(),
            table_search: None,
            show_command_input: false,
            command_input: String::new(),
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
                "Enter/l open job | h/Esc back | / search | y copy id | g/G top/end | p pause"
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
                    .filter(|m| {
                        self.matches_table_search(vec![
                            m.job_type.as_str().to_owned(),
                            m.pending_count.to_string(),
                            m.leased_count.to_string(),
                            m.stale_leases.to_string(),
                            m.dead_lettered_24h.to_string(),
                        ])
                    })
                    .count()
            }),
            Screen::Queue => self.jobs.as_ref().map_or(0, |d| {
                d.jobs.iter().filter(|j| self.job_matches_search(j)).count()
            }),
            Screen::JobDetail { .. } => match self.job_detail_pane {
                JobDetailPane::Events => self.job_detail.as_ref().map_or(0, |d| {
                    d.events
                        .iter()
                        .filter(|e| {
                            self.matches_table_search(vec![
                                e.id.to_string(),
                                e.event_type.as_db_value().to_owned(),
                                e.stage.map(|s| s.as_db_value()).unwrap_or("").to_owned(),
                            ])
                        })
                        .count()
                }),
                JobDetailPane::Logs => self.job_detail.as_ref().map_or(0, |d| {
                    d.logs
                        .iter()
                        .filter(|l| {
                            self.matches_table_search(vec![
                                l.id.to_string(),
                                l.level.clone(),
                                l.message.clone(),
                            ])
                        })
                        .count()
                }),
                _ => 0,
            },
            Screen::Workflows => self.workflows.as_ref().map_or(0, |d| {
                d.runs
                    .iter()
                    .filter(|r| {
                        self.matches_table_search(vec![
                            r.id.to_string(),
                            r.workflow_type.as_str().to_owned(),
                            crate::format::workflow_run_status_label(r.status).to_owned(),
                        ])
                    })
                    .count()
            }),
            Screen::WorkflowDetail { .. } => self.workflow_detail.as_ref().map_or(0, |d| {
                d.steps
                    .iter()
                    .filter(|s| {
                        self.matches_table_search(vec![
                            s.step_key.as_str().to_owned(),
                            crate::format::workflow_step_status_label(s.status).to_owned(),
                            s.job_type
                                .as_ref()
                                .map(|t| t.as_str().to_owned())
                                .unwrap_or_default(),
                            s.job_id.map(|id| id.to_string()).unwrap_or_default(),
                        ])
                    })
                    .count()
            }),
            Screen::Definitions => self.definitions.as_ref().map_or(0, |d| {
                d.definitions
                    .iter()
                    .filter(|def| {
                        self.matches_table_search(vec![
                            def.job_type.as_str().to_owned(),
                            def.version.to_string(),
                            if def.is_enabled {
                                "enabled"
                            } else {
                                "disabled"
                            }
                            .to_owned(),
                        ])
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

    pub fn matches_table_search<I, S>(&self, fields: I) -> bool
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Some(query) = self.table_search_query() else {
            return true;
        };
        let query = query.to_ascii_lowercase();
        fields
            .into_iter()
            .any(|field| field.as_ref().to_ascii_lowercase().contains(&query))
    }

    fn job_matches_search(&self, job: &runledger_postgres::jobs::JobQueueRecord) -> bool {
        self.matches_table_search(vec![
            job.id.to_string(),
            job.job_type.as_str().to_owned(),
            crate::format::job_status_label(job.status).to_owned(),
            job.stage.as_db_value().to_owned(),
            job.worker_id.as_deref().unwrap_or("").to_owned(),
        ])
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
    use super::*;
    use chrono::Utc;
    use runledger_core::jobs::{JobStage, JobStatus, JobTypeName};
    use runledger_postgres::jobs::JobQueueRecord;

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
