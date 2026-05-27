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

pub struct App {
    pub config: Config,
    pub scope: Scope,
    pub screen: Screen,
    pub screen_stack: Vec<Screen>,
    pub list_selection: usize,
    pub detail_scroll: usize,
    pub payload_visible_rows: usize,
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
    pub dashboard: Option<DashboardData>,
    pub jobs: Option<JobsData>,
    pub job_detail: Option<JobDetailData>,
    pub workflows: Option<WorkflowsData>,
    pub workflow_detail: Option<WorkflowDetailData>,
    pub definitions: Option<DefinitionsData>,
    pub last_error: Option<String>,
    pub last_refresh: Option<Instant>,
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
            list_selection: 0,
            detail_scroll: 0,
            payload_visible_rows: 1,
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
            dashboard: None,
            jobs: None,
            job_detail: None,
            workflows: None,
            workflow_detail: None,
            definitions: None,
            last_error: None,
            last_refresh: None,
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

    pub fn status_line(&self) -> String {
        let scope = self.scope.label();
        let refresh = self
            .last_refresh
            .map(|t| format!("{:.1}s ago", t.elapsed().as_secs_f32()))
            .unwrap_or_else(|| "never".to_owned());
        let fetch = if self.fetching { " | fetching…" } else { "" };
        let err = self
            .last_error
            .as_ref()
            .map(|e| format!(" | err: {}", truncate_status(e, 40)))
            .unwrap_or_default();
        format!(
            "scope={scope} | screen={} | refresh={refresh}{fetch}{err} | ? help",
            self.screen_title()
        )
    }

    pub fn navigate_top(&mut self, top: TopScreen) {
        self.screen_stack.clear();
        self.screen = screen_from_top(top);
        self.list_selection = 0;
        self.detail_scroll = 0;
        self.invalidate_cache();
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
        self.screen_stack.push(self.screen.clone());
        self.screen = Screen::JobDetail { job_id };
        self.job_detail = None;
        self.list_selection = 0;
        self.detail_scroll = 0;
        self.job_detail_pane = JobDetailPane::Summary;
    }

    pub fn push_workflow_detail(&mut self, run_id: Uuid) {
        self.bump_fetch_generation();
        self.screen_stack.push(self.screen.clone());
        self.screen = Screen::WorkflowDetail { run_id };
        self.workflow_detail = None;
        self.list_selection = 0;
    }

    pub fn pop_screen(&mut self) {
        if let Some(prev) = self.screen_stack.pop() {
            self.bump_fetch_generation();
            self.screen = prev;
            self.list_selection = 0;
            self.detail_scroll = 0;
        }
    }

    pub fn list_len(&self) -> usize {
        match &self.screen {
            Screen::Dashboard => self.dashboard.as_ref().map_or(0, |d| d.metrics.len()),
            Screen::Queue => self.jobs.as_ref().map_or(0, |d| d.jobs.len()),
            Screen::JobDetail { .. } => match self.job_detail_pane {
                JobDetailPane::Events => self.job_detail.as_ref().map_or(0, |d| d.events.len()),
                JobDetailPane::Logs => self.job_detail.as_ref().map_or(0, |d| d.logs.len()),
                _ => 0,
            },
            Screen::Workflows => self.workflows.as_ref().map_or(0, |d| d.runs.len()),
            Screen::WorkflowDetail { .. } => {
                self.workflow_detail.as_ref().map_or(0, |d| d.steps.len())
            }
            Screen::Definitions => self.definitions.as_ref().map_or(0, |d| d.definitions.len()),
        }
    }

    fn apply_fetch(&mut self, outcome: FetchOutcome) {
        self.fetching = false;
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
        let len = self.list_len();
        if len > 0 && self.list_selection >= len {
            self.list_selection = len - 1;
        }
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
                self.invalidate_cache();
                refresh = true;
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
                self.show_filter_input = true;
                self.filter_input_workflow = false;
                self.filter_input = self.job_type_filter.clone().unwrap_or_default();
            }
            KeyCode::Char('w') if matches!(self.screen, Screen::Workflows) => {
                self.show_filter_input = true;
                self.filter_input_workflow = true;
                self.filter_input = self.workflow_type_filter.clone().unwrap_or_default();
            }
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
            KeyCode::Enter => {
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
        let next = if delta.is_positive() {
            self.list_selection.saturating_add(1)
        } else {
            self.list_selection.saturating_sub(1)
        };
        self.list_selection = next.min(len - 1);
    }

    fn payload_scroll_max(&self) -> usize {
        let Some(detail) = &self.job_detail else {
            return 0;
        };
        let lines = crate::format::job_payload_lines(&detail.job.payload);
        crate::format::job_payload_scroll_max(lines.len(), self.payload_visible_rows)
    }

    fn activate_selection(&mut self) {
        match &self.screen {
            Screen::Queue => {
                if let Some(job) = self
                    .jobs
                    .as_ref()
                    .and_then(|d| d.jobs.get(self.list_selection))
                {
                    self.push_job_detail(job.id);
                }
            }
            Screen::Workflows => {
                if let Some(run) = self
                    .workflows
                    .as_ref()
                    .and_then(|d| d.runs.get(self.list_selection))
                {
                    self.push_workflow_detail(run.id);
                }
            }
            _ => {}
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
            let outcome = execute_fetch(&pool_bg, req).await;
            if fetch_generation_bg.load(Ordering::Acquire) != generation {
                in_flight_bg.store(false, Ordering::Release);
                let mut guard = app_fetch.lock().await;
                guard.fetching = false;
                continue;
            }
            let mut guard = app_fetch.lock().await;
            guard.apply_fetch(outcome);
            in_flight_bg.store(false, Ordering::Release);
        }
    });

    let refresh_every = config.refresh_interval();
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
            need_fetch = true;
            next_refresh = Instant::now() + refresh_every;
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
}
