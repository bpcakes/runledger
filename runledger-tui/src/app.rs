use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use runledger_postgres::DbPool;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::config::Config;
use crate::data::{
    DashboardData, DefinitionsData, JobDetailData, JobsData, QueueStatusFilter, WorkflowDetailData,
    WorkflowsData, fetch_dashboard, fetch_definitions, fetch_job_detail, fetch_jobs,
    fetch_workflow_detail, fetch_workflows,
};
use crate::scope::Scope;
use crate::ui;
use crate::ui::render::{screen_from_top, top_screen_from_index};

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

enum FetchOutcome {
    Dashboard(Result<Box<DashboardData>, String>),
    Jobs(Result<Box<JobsData>, String>),
    JobDetail(Result<Box<JobDetailData>, String>),
    Workflows(Result<Box<WorkflowsData>, String>),
    WorkflowDetail(Result<Box<WorkflowDetailData>, String>),
    Definitions(Result<Box<DefinitionsData>, String>),
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
        self.screen = screen_from_top(top);
        self.restore_view_state(self.top_view_states[self.top_screen_index()]);
        self.bump_fetch_generation();
    }

    fn invalidate_cache(&mut self) {
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

    fn apply_fetch(&mut self, outcome: FetchOutcome, duration: Duration) {
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

    fn clamp_selection(&mut self) {
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

    pub fn update_payload_visible_rows(&mut self, visible_rows: usize) {
        self.payload_visible_rows = visible_rows.max(1);
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            self.detail_scroll = self.detail_scroll.min(self.payload_scroll_max());
        }
    }

    /// Returns true when a data refresh should be scheduled.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.show_org_input {
            return self.handle_org_input_key(key);
        }
        if self.show_filter_input {
            return self.handle_filter_input_key(key);
        }
        if self.show_search_input {
            return self.handle_search_input_key(key);
        }
        if self.show_command_input {
            return self.handle_command_input_key(key);
        }
        if self.show_help && key.code != KeyCode::Char('?') && key.code != KeyCode::Esc {
            return false;
        }

        let mut refresh = false;
        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
            }
            KeyCode::Char('?') => self.show_help = !self.show_help,
            KeyCode::Esc => {
                if self.show_help {
                    self.show_help = false;
                } else if !self.screen_stack.is_empty() {
                    self.pop_screen();
                    refresh = true;
                }
            }
            KeyCode::Char('r') => {
                self.bump_fetch_generation();
                refresh = true;
            }
            KeyCode::Char('.') => {
                self.bump_fetch_generation();
                refresh = true;
            }
            KeyCode::Char('p') => {
                self.refresh_paused = !self.refresh_paused;
                self.notice = Some(if self.refresh_paused {
                    "Auto-refresh paused".to_owned()
                } else {
                    "Auto-refresh resumed".to_owned()
                });
            }
            KeyCode::Char('o') => {
                self.show_org_input = true;
                self.org_input = self
                    .scope
                    .organization_id
                    .map(|id| id.to_string())
                    .unwrap_or_default();
            }
            KeyCode::Char('/') => {
                self.show_search_input = true;
                self.search_input = self.table_search.clone().unwrap_or_default();
            }
            KeyCode::Char('t') => {
                self.show_filter_input = true;
                self.filter_input_workflow = matches!(
                    self.screen,
                    Screen::Workflows | Screen::WorkflowDetail { .. }
                );
                self.filter_input = if self.filter_input_workflow {
                    self.workflow_type_filter.clone().unwrap_or_default()
                } else {
                    self.job_type_filter.clone().unwrap_or_default()
                };
            }
            KeyCode::Char('w') if matches!(self.screen, Screen::Workflows) => {
                self.show_filter_input = true;
                self.filter_input_workflow = true;
                self.filter_input = self.workflow_type_filter.clone().unwrap_or_default();
            }
            KeyCode::Char(':') => {
                self.show_command_input = true;
                self.command_input.clear();
            }
            KeyCode::Char('c') => {
                refresh = self.clear_context_filters();
            }
            KeyCode::Char('y') => self.copy_selected_identifier(),
            KeyCode::Char('f') if matches!(self.screen, Screen::Queue) => {
                self.bump_fetch_generation();
                self.queue_filter = self.queue_filter.next();
                self.jobs = None;
                refresh = true;
            }
            KeyCode::Char('1') => {
                self.navigate_top(TopScreen::Dashboard);
                refresh = true;
            }
            KeyCode::Char('2') => {
                self.navigate_top(TopScreen::Queue);
                refresh = true;
            }
            KeyCode::Char('3') => {
                self.navigate_top(TopScreen::Workflows);
                refresh = true;
            }
            KeyCode::Char('4') => {
                self.navigate_top(TopScreen::Definitions);
                refresh = true;
            }
            KeyCode::Tab => {
                let next =
                    top_screen_from_index((self.top_screen_index() + 1) % Self::TOP_SCREEN_COUNT);
                self.navigate_top(next);
                refresh = true;
            }
            KeyCode::BackTab => {
                let next = top_screen_from_index(
                    (self.top_screen_index() + Self::TOP_SCREEN_COUNT - 1) % Self::TOP_SCREEN_COUNT,
                );
                self.navigate_top(next);
                refresh = true;
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1),
            KeyCode::Char('g') | KeyCode::Home => self.move_to_start(),
            KeyCode::Char('G') | KeyCode::End => self.move_to_end(),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::Char('h') => {
                if !self.screen_stack.is_empty() {
                    self.pop_screen();
                    refresh = true;
                }
            }
            KeyCode::Char('v')
                if matches!(
                    (&self.screen, self.job_detail_pane),
                    (Screen::JobDetail { .. }, JobDetailPane::Payload)
                ) =>
            {
                self.payload_wrap = !self.payload_wrap;
                self.notice = Some(if self.payload_wrap {
                    "Payload wrap enabled".to_owned()
                } else {
                    "Payload wrap disabled".to_owned()
                });
            }
            KeyCode::Char('R')
                if matches!(
                    (&self.screen, self.job_detail_pane),
                    (Screen::JobDetail { .. }, JobDetailPane::Payload)
                ) =>
            {
                self.payload_raw = !self.payload_raw;
                self.detail_scroll = 0;
                self.notice = Some(if self.payload_raw {
                    "Payload raw mode".to_owned()
                } else {
                    "Payload pretty mode".to_owned()
                });
            }
            KeyCode::Char(']') | KeyCode::Right
                if matches!(self.screen, Screen::JobDetail { .. }) =>
            {
                self.job_detail_pane = self.job_detail_pane.next();
                self.detail_scroll = 0;
                self.list_selection = 0;
            }
            KeyCode::Char('[') | KeyCode::Left
                if matches!(self.screen, Screen::JobDetail { .. }) =>
            {
                self.job_detail_pane = self.job_detail_pane.prev();
                self.detail_scroll = 0;
                self.list_selection = 0;
            }
            KeyCode::Char('l') | KeyCode::Enter => {
                let before = self.screen.clone();
                self.activate_selection();
                if self.screen != before {
                    refresh = true;
                }
            }
            _ => {}
        }
        refresh
    }

    fn handle_org_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_org_input = false,
            KeyCode::Enter => {
                let trimmed = self.org_input.trim();
                self.scope = if trimmed.is_empty() {
                    Scope::global()
                } else {
                    match Uuid::parse_str(trimmed) {
                        Ok(id) => Scope::for_org(id),
                        Err(_) => {
                            self.last_error = Some("Invalid organization UUID".to_owned());
                            self.show_org_input = false;
                            return false;
                        }
                    }
                };
                self.show_org_input = false;
                self.invalidate_cache();
                refresh = true;
            }
            KeyCode::Backspace => {
                self.org_input.pop();
            }
            KeyCode::Char(c) => self.org_input.push(c),
            _ => {}
        }
        refresh
    }

    fn handle_filter_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_filter_input = false,
            KeyCode::Enter => {
                let trimmed = self.filter_input.trim();
                let value = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                if self.filter_input_workflow {
                    self.bump_fetch_generation();
                    self.workflow_type_filter = value;
                    self.workflows = None;
                } else {
                    self.bump_fetch_generation();
                    self.job_type_filter = value;
                    self.jobs = None;
                    self.definitions = None;
                }
                self.show_filter_input = false;
                refresh = true;
            }
            KeyCode::Backspace => {
                self.filter_input.pop();
            }
            KeyCode::Char(c) => self.filter_input.push(c),
            _ => {}
        }
        refresh
    }

    fn handle_search_input_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => self.show_search_input = false,
            KeyCode::Enter => {
                let trimmed = self.search_input.trim();
                self.table_search = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                self.show_search_input = false;
                self.list_selection = 0;
                self.clamp_selection();
            }
            KeyCode::Backspace => {
                self.search_input.pop();
            }
            KeyCode::Char(c) => self.search_input.push(c),
            _ => {}
        }
        false
    }

    fn handle_command_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_command_input = false,
            KeyCode::Enter => {
                let command = self.command_input.trim().to_owned();
                refresh = self.execute_command(&command);
                self.show_command_input = false;
            }
            KeyCode::Backspace => {
                self.command_input.pop();
            }
            KeyCode::Char(c) => self.command_input.push(c),
            _ => {}
        }
        refresh
    }

    fn move_selection(&mut self, delta: i32) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            let max_scroll = self.payload_scroll_max();
            if max_scroll == 0 {
                return;
            }
            let next = if delta.is_positive() {
                self.detail_scroll.saturating_add(1)
            } else {
                self.detail_scroll.saturating_sub(1)
            };
            self.detail_scroll = next.min(max_scroll);
            return;
        }

        let len = self.list_len();
        if len == 0 {
            return;
        }
        let step = delta.unsigned_abs() as usize;
        let next = if delta.is_positive() {
            self.list_selection.saturating_add(step)
        } else {
            self.list_selection.saturating_sub(step)
        };
        self.list_selection = next.min(len - 1);
    }

    fn move_to_start(&mut self) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            self.detail_scroll = 0;
        } else {
            self.list_selection = 0;
        }
    }

    fn move_to_end(&mut self) {
        if matches!(
            (&self.screen, self.job_detail_pane),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        ) {
            self.detail_scroll = self.payload_scroll_max();
            return;
        }
        let len = self.list_len();
        if len > 0 {
            self.list_selection = len - 1;
        }
    }

    fn payload_scroll_max(&self) -> usize {
        let Some(detail) = &self.job_detail else {
            return 0;
        };
        let lines = if self.payload_raw {
            crate::format::job_payload_raw_lines(&detail.job.payload)
        } else {
            crate::format::job_payload_lines(&detail.job.payload)
        };
        crate::format::job_payload_scroll_max(lines.len(), self.payload_visible_rows)
    }

    fn activate_selection(&mut self) {
        match &self.screen {
            Screen::Dashboard => {
                if let Some(job_type) = self.selected_dashboard_job_type() {
                    self.job_type_filter = Some(job_type);
                    self.queue_filter = QueueStatusFilter::All;
                    self.navigate_top(TopScreen::Queue);
                }
            }
            Screen::Queue => {
                if let Some(job_id) = self.selected_job_id() {
                    self.push_job_detail(job_id);
                }
            }
            Screen::Workflows => {
                if let Some(run_id) = self.selected_workflow_run_id() {
                    self.push_workflow_detail(run_id);
                }
            }
            Screen::WorkflowDetail { .. } => {
                if let Some(job_id) = self.selected_workflow_step_job_id() {
                    self.push_job_detail(job_id);
                }
            }
            _ => {}
        }
    }

    fn selected_dashboard_job_type(&self) -> Option<String> {
        self.dashboard
            .as_ref()?
            .metrics
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
            .nth(self.list_selection)
            .map(|m| m.job_type.as_str().to_owned())
    }

    fn selected_job_id(&self) -> Option<Uuid> {
        self.jobs
            .as_ref()?
            .jobs
            .iter()
            .filter(|j| self.job_matches_search(j))
            .nth(self.list_selection)
            .map(|j| j.id)
    }

    fn selected_workflow_run_id(&self) -> Option<Uuid> {
        self.workflows
            .as_ref()?
            .runs
            .iter()
            .filter(|r| {
                self.matches_table_search(vec![
                    r.id.to_string(),
                    r.workflow_type.as_str().to_owned(),
                    crate::format::workflow_run_status_label(r.status).to_owned(),
                ])
            })
            .nth(self.list_selection)
            .map(|r| r.id)
    }

    fn selected_workflow_step_job_id(&self) -> Option<Uuid> {
        self.workflow_detail
            .as_ref()?
            .steps
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
            .nth(self.list_selection)
            .and_then(|s| s.job_id)
    }

    fn selected_identifier(&self) -> Option<String> {
        match self.screen {
            Screen::Dashboard => self.selected_dashboard_job_type(),
            Screen::Queue => self.selected_job_id().map(|id| id.to_string()),
            Screen::JobDetail { job_id } => Some(job_id.to_string()),
            Screen::Workflows => self.selected_workflow_run_id().map(|id| id.to_string()),
            Screen::WorkflowDetail { run_id } => self
                .selected_workflow_step_job_id()
                .map(|id| id.to_string())
                .or_else(|| Some(run_id.to_string())),
            Screen::Definitions => self
                .definitions
                .as_ref()?
                .definitions
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
                .nth(self.list_selection)
                .map(|def| def.job_type.as_str().to_owned()),
        }
    }

    fn copy_selected_identifier(&mut self) {
        let Some(value) = self.selected_identifier() else {
            self.notice = Some("Nothing selected to copy".to_owned());
            return;
        };
        match copy_to_terminal_clipboard(&value) {
            Ok(()) => self.notice = Some(format!("Copied {value}")),
            Err(error) => self.notice = Some(format!("Copy failed: {error}")),
        }
    }

    fn clear_context_filters(&mut self) -> bool {
        let mut refresh = false;
        self.table_search = None;
        match self.screen {
            Screen::Queue | Screen::JobDetail { .. } => {
                if self.queue_filter != QueueStatusFilter::All || self.job_type_filter.is_some() {
                    self.queue_filter = QueueStatusFilter::All;
                    self.job_type_filter = None;
                    self.jobs = None;
                    self.definitions = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Workflows | Screen::WorkflowDetail { .. } => {
                if self.workflow_type_filter.is_some() {
                    self.workflow_type_filter = None;
                    self.workflows = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Definitions => {
                if self.job_type_filter.is_some() {
                    self.job_type_filter = None;
                    self.definitions = None;
                    self.bump_fetch_generation();
                    refresh = true;
                }
            }
            Screen::Dashboard => {}
        }
        self.list_selection = 0;
        self.notice = Some("Cleared filters".to_owned());
        refresh
    }

    fn execute_command(&mut self, command: &str) -> bool {
        let parts: Vec<&str> = command.split_whitespace().collect();
        match parts.as_slice() {
            [] => false,
            ["scope", "global"] => {
                self.scope = Scope::global();
                self.invalidate_cache();
                self.notice = Some("Scope set to global".to_owned());
                true
            }
            ["filter", "status", status] => {
                let Some(filter) = QueueStatusFilter::from_command(status) else {
                    self.notice = Some(format!("Unknown status filter: {status}"));
                    return false;
                };
                self.queue_filter = filter;
                self.jobs = None;
                self.bump_fetch_generation();
                self.navigate_top(TopScreen::Queue);
                true
            }
            ["refresh", value] => {
                if let Some(ms) = parse_refresh_ms(value) {
                    self.config.refresh_ms = ms;
                    self.notice = Some(format!(
                        "Refresh interval set to {}",
                        format_duration_short(self.config.refresh_interval())
                    ));
                } else {
                    self.notice = Some(format!("Invalid refresh interval: {value}"));
                }
                false
            }
            ["copy", "id"] => {
                self.copy_selected_identifier();
                false
            }
            _ => {
                self.notice = Some(format!("Unknown command: {command}"));
                false
            }
        }
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

fn parse_refresh_ms(value: &str) -> Option<u64> {
    if let Some(raw) = value.strip_suffix("ms") {
        return raw.parse().ok();
    }
    if let Some(raw) = value.strip_suffix('s') {
        return raw.parse::<u64>().ok().map(|seconds| seconds * 1000);
    }
    value.parse().ok()
}

fn copy_to_terminal_clipboard(value: &str) -> std::io::Result<()> {
    let encoded = base64_encode(value.as_bytes());
    let mut stdout = std::io::stdout();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

struct FetchRequest {
    screen: Screen,
    scope: Scope,
    queue_filter: QueueStatusFilter,
    job_type_filter: Option<String>,
    workflow_type_filter: Option<String>,
    limit: i64,
}

async fn execute_fetch(pool: &DbPool, req: FetchRequest) -> FetchOutcome {
    match req.screen {
        Screen::Dashboard => {
            FetchOutcome::Dashboard(fetch_dashboard(pool, req.scope).await.map(Box::new))
        }
        Screen::Queue => FetchOutcome::Jobs(
            fetch_jobs(
                pool,
                req.scope,
                req.queue_filter,
                req.job_type_filter,
                req.limit,
            )
            .await
            .map(Box::new),
        ),
        Screen::JobDetail { job_id } => FetchOutcome::JobDetail(
            fetch_job_detail(pool, req.scope, job_id, req.limit)
                .await
                .map(Box::new),
        ),
        Screen::Workflows => FetchOutcome::Workflows(
            fetch_workflows(pool, req.scope, req.workflow_type_filter, req.limit)
                .await
                .map(Box::new),
        ),
        Screen::WorkflowDetail { run_id } => FetchOutcome::WorkflowDetail(
            fetch_workflow_detail(pool, req.scope, run_id)
                .await
                .map(Box::new),
        ),
        Screen::Definitions => FetchOutcome::Definitions(
            fetch_definitions(pool, req.job_type_filter, req.limit)
                .await
                .map(Box::new),
        ),
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        let _ = crossterm::execute!(std::io::stdout(), crossterm::cursor::Show);
    }
}

pub async fn run(pool: DbPool, config: Config) -> std::io::Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let _terminal_guard = TerminalGuard;
    crossterm::execute!(std::io::stdout(), crossterm::terminal::EnterAlternateScreen)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;

    let fetch_generation = Arc::new(AtomicU64::new(0));
    let app = Arc::new(Mutex::new(App::new(
        config.clone(),
        fetch_generation.clone(),
    )));
    let in_flight = Arc::new(AtomicBool::new(false));
    let (fetch_tx, mut fetch_rx) = mpsc::unbounded_channel::<(u64, FetchRequest)>();

    let pool_bg = pool.clone();
    let in_flight_bg = in_flight.clone();
    let app_fetch = app.clone();
    let fetch_generation_bg = fetch_generation.clone();
    let fetch_worker = tokio::spawn(async move {
        while let Some((generation, req)) = fetch_rx.recv().await {
            let started_at = Instant::now();
            let outcome = execute_fetch(&pool_bg, req).await;
            let fetch_duration = started_at.elapsed();
            if fetch_generation_bg.load(Ordering::Acquire) != generation {
                in_flight_bg.store(false, Ordering::Release);
                let mut guard = app_fetch.lock().await;
                guard.fetching = false;
                continue;
            }
            let mut guard = app_fetch.lock().await;
            guard.apply_fetch(outcome, fetch_duration);
            in_flight_bg.store(false, Ordering::Release);
        }
    });

    let mut next_refresh = Instant::now();

    let mut need_fetch = true;

    loop {
        if fetch_worker.is_finished() {
            return Err(std::io::Error::other(
                "data fetch worker stopped unexpectedly",
            ));
        }

        let block_fetch = {
            let guard = app.lock().await;
            !guard.show_org_input && !guard.show_filter_input
        };
        if need_fetch && !in_flight.load(Ordering::Acquire) && block_fetch {
            let req = {
                let guard = app.lock().await;
                FetchRequest {
                    screen: guard.screen.clone(),
                    scope: guard.scope,
                    queue_filter: guard.queue_filter,
                    job_type_filter: guard.job_type_filter.clone(),
                    workflow_type_filter: guard.workflow_type_filter.clone(),
                    limit: guard.config.limit,
                }
            };
            in_flight.store(true, Ordering::Release);
            let generation = fetch_generation.fetch_add(1, Ordering::AcqRel) + 1;
            {
                let mut guard = app.lock().await;
                guard.fetching = true;
            }
            let _ = fetch_tx.send((generation, req));
            need_fetch = false;
        }

        {
            let mut guard = app.lock().await;
            terminal.draw(|f| ui::draw(f, &mut guard))?;
            if guard.should_quit {
                break;
            }
        }

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                let mut guard = app.lock().await;
                if guard.handle_key(key) {
                    need_fetch = true;
                }
            }
        }

        if Instant::now() >= next_refresh {
            let refresh_every = {
                let guard = app.lock().await;
                if guard.refresh_paused {
                    None
                } else {
                    Some(guard.config.refresh_interval())
                }
            };
            if let Some(refresh_every) = refresh_every {
                need_fetch = true;
                next_refresh = Instant::now() + refresh_every;
            } else {
                next_refresh = Instant::now() + Duration::from_millis(250);
            }
        }
    }

    Ok(())
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
