use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use runledger_postgres::DbPool;
use tokio::task::JoinSet;

use crate::app::App;
use crate::app::fetch::{FetchOutcome, FetchResult, FetchStatus, execute_fetch};
use crate::config::Config;
use crate::ui;

const PAUSED_REFRESH_POLL: Duration = Duration::from_millis(250);

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        let _ = crossterm::execute!(std::io::stdout(), crossterm::cursor::Show);
    }
}

#[derive(Debug)]
struct FetchLifecycle {
    requested_generation: u64,
    in_flight_generation: Option<u64>,
    applied_generation: Option<u64>,
    next_refresh: Instant,
    last_refresh: Option<Instant>,
    last_fetch_duration: Option<Duration>,
}

impl FetchLifecycle {
    fn new(now: Instant) -> Self {
        Self {
            requested_generation: 1,
            in_flight_generation: None,
            applied_generation: None,
            next_refresh: now,
            last_refresh: None,
            last_fetch_duration: None,
        }
    }

    fn request_refresh(&mut self) {
        self.requested_generation = self.requested_generation.wrapping_add(1);
    }

    fn schedule_auto_refresh(
        &mut self,
        now: Instant,
        refresh_paused: bool,
        refresh_interval: Duration,
    ) {
        if now < self.next_refresh {
            return;
        }
        if refresh_paused {
            self.next_refresh = now + PAUSED_REFRESH_POLL;
            return;
        }

        let pending_generation = match self.in_flight_generation {
            Some(in_flight) => self.requested_generation != in_flight,
            None => self.applied_generation != Some(self.requested_generation),
        };
        if pending_generation {
            self.next_refresh = now + refresh_interval;
        } else if self.in_flight_generation.is_none() {
            self.request_refresh();
            self.next_refresh = now + refresh_interval;
        }
    }

    fn start_fetch(&mut self) -> Option<u64> {
        if self.in_flight_generation.is_some()
            || self.applied_generation == Some(self.requested_generation)
        {
            return None;
        }
        self.in_flight_generation = Some(self.requested_generation);
        Some(self.requested_generation)
    }

    fn complete_fetch(&mut self, result: FetchResult) -> Option<FetchOutcome> {
        if self.in_flight_generation != Some(result.generation) {
            return None;
        }
        self.in_flight_generation = None;
        if self.requested_generation != result.generation {
            return None;
        }

        self.applied_generation = Some(result.generation);
        self.last_fetch_duration = Some(result.duration);
        Some(result.outcome)
    }

    fn record_refresh(&mut self, now: Instant) {
        self.last_refresh = Some(now);
    }

    fn status(&self) -> FetchStatus {
        FetchStatus {
            last_refresh: self.last_refresh,
            last_fetch_duration: self.last_fetch_duration,
            fetching: self.in_flight_generation.is_some(),
        }
    }
}

pub async fn run(pool: DbPool, config: Config) -> std::io::Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let _terminal_guard = TerminalGuard;
    crossterm::execute!(std::io::stdout(), crossterm::terminal::EnterAlternateScreen)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
    let mut app = App::new(config);
    let mut fetch_lifecycle = FetchLifecycle::new(Instant::now());
    let mut fetch_workers = JoinSet::new();

    loop {
        apply_finished_fetches(&mut fetch_workers, &mut fetch_lifecycle, &mut app)?;
        fetch_lifecycle.schedule_auto_refresh(
            Instant::now(),
            app.refresh_paused,
            app.config.refresh_interval(),
        );

        if app.allows_fetch()
            && let Some(generation) = fetch_lifecycle.start_fetch()
        {
            let request = app.fetch_request();
            let fetch_pool = pool.clone();
            fetch_workers
                .spawn(async move { execute_fetch(&fetch_pool, generation, request).await });
        }

        terminal.draw(|frame| ui::draw(frame, &mut app, fetch_lifecycle.status()))?;
        if app.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && app.handle_key(key)
        {
            fetch_lifecycle.request_refresh();
        }
    }

    fetch_workers.shutdown().await;
    Ok(())
}

fn apply_finished_fetches(
    fetch_workers: &mut JoinSet<FetchResult>,
    fetch_lifecycle: &mut FetchLifecycle,
    app: &mut App,
) -> std::io::Result<()> {
    while let Some(joined) = fetch_workers.try_join_next() {
        let result = joined.map_err(|error| {
            std::io::Error::other(format!("data fetch worker stopped unexpectedly: {error}"))
        })?;
        if let Some(outcome) = fetch_lifecycle.complete_fetch(result)
            && app.apply_fetch(outcome)
        {
            fetch_lifecycle.record_refresh(Instant::now());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::oneshot;

    use super::*;
    use crate::data::JobsData;

    fn test_app() -> App {
        App::new(Config {
            database_url: "postgres://example/runledger".to_owned(),
            org: None,
            refresh_ms: 2_000,
            limit: 100,
            skip_schema_check: false,
        })
    }

    fn error_result(generation: u64, duration: Duration) -> FetchResult {
        FetchResult {
            generation,
            outcome: FetchOutcome::Dashboard(Err("fetch failed".to_owned())),
            duration,
        }
    }

    fn success_result(generation: u64, duration: Duration) -> FetchResult {
        FetchResult {
            generation,
            outcome: FetchOutcome::Jobs(Ok(Box::new(JobsData { jobs: Vec::new() }))),
            duration,
        }
    }

    #[test]
    fn out_of_order_completions_cannot_replace_the_current_generation() {
        let now = Instant::now();
        let mut lifecycle = FetchLifecycle::new(now);
        let first = lifecycle.start_fetch().expect("initial fetch");
        lifecycle.request_refresh();

        assert!(
            lifecycle
                .complete_fetch(error_result(first + 1, Duration::from_millis(2)))
                .is_none()
        );
        assert_eq!(lifecycle.in_flight_generation, Some(first));
        assert!(
            lifecycle
                .complete_fetch(error_result(first, Duration::from_millis(3)))
                .is_none()
        );

        let current = lifecycle.start_fetch().expect("replacement fetch");
        assert_eq!(current, first + 1);
        assert!(
            lifecycle
                .complete_fetch(error_result(first, Duration::from_millis(4)))
                .is_none()
        );
        assert_eq!(lifecycle.in_flight_generation, Some(current));
        assert!(
            lifecycle
                .complete_fetch(error_result(current, Duration::from_millis(5)))
                .is_some()
        );
        assert_eq!(lifecycle.applied_generation, Some(current));
        assert_eq!(
            lifecycle.last_fetch_duration,
            Some(Duration::from_millis(5))
        );
    }

    #[test]
    fn rapid_filter_changes_coalesce_behind_the_in_flight_fetch() {
        let mut lifecycle = FetchLifecycle::new(Instant::now());
        let mut app = test_app();
        app.current_frame.screen = crate::app::Screen::Queue;
        let first = lifecycle.start_fetch().expect("initial fetch");

        for _ in 0..3 {
            let key = crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('f'),
                crossterm::event::KeyModifiers::NONE,
            );
            assert!(app.handle_key(key));
            lifecycle.request_refresh();
        }
        assert_eq!(lifecycle.requested_generation, first + 3);
        assert!(lifecycle.start_fetch().is_none());
        assert!(
            lifecycle
                .complete_fetch(error_result(first, Duration::from_millis(1)))
                .is_none()
        );

        assert_eq!(lifecycle.start_fetch(), Some(first + 3));
    }

    #[test]
    fn current_errors_are_applied_without_advancing_success_timing() {
        let mut lifecycle = FetchLifecycle::new(Instant::now());
        let mut app = test_app();
        let generation = lifecycle.start_fetch().expect("initial fetch");
        let duration = Duration::from_millis(17);

        let outcome = lifecycle
            .complete_fetch(error_result(generation, duration))
            .expect("current error outcome");
        assert!(!app.apply_fetch(outcome));
        assert_eq!(lifecycle.applied_generation, Some(generation));
        assert_eq!(lifecycle.last_fetch_duration, Some(duration));
        assert_eq!(lifecycle.last_refresh, None);
        assert!(!lifecycle.status().fetching);
        assert_eq!(app.last_error.as_deref(), Some("fetch failed"));
    }

    #[test]
    fn current_success_advances_application_and_refresh_timing_together() {
        let now = Instant::now();
        let mut lifecycle = FetchLifecycle::new(now);
        let mut app = test_app();
        let generation = lifecycle.start_fetch().expect("initial fetch");

        let outcome = lifecycle
            .complete_fetch(success_result(generation, Duration::from_millis(9)))
            .expect("current success outcome");
        assert!(app.apply_fetch(outcome));
        lifecycle.record_refresh(now);

        assert_eq!(lifecycle.applied_generation, Some(generation));
        assert_eq!(lifecycle.last_refresh, Some(now));
        assert!(app.jobs.is_some());
        assert_eq!(app.last_error, None);
    }

    #[test]
    fn refresh_timing_waits_for_an_in_flight_generation_without_invalidating_it() {
        let now = Instant::now();
        let interval = Duration::from_secs(2);
        let mut lifecycle = FetchLifecycle::new(now);

        lifecycle.schedule_auto_refresh(now, false, interval);
        let first = lifecycle.start_fetch().expect("initial fetch");
        let due = now + interval;
        lifecycle.schedule_auto_refresh(due, false, interval);
        assert_eq!(lifecycle.requested_generation, first);
        assert_eq!(lifecycle.next_refresh, due);

        assert!(
            lifecycle
                .complete_fetch(error_result(first, Duration::from_millis(1)))
                .is_some()
        );
        lifecycle.schedule_auto_refresh(due, false, interval);
        assert_eq!(lifecycle.requested_generation, first + 1);
        assert_eq!(lifecycle.next_refresh, due + interval);

        lifecycle.schedule_auto_refresh(due + interval, true, interval);
        assert_eq!(lifecycle.next_refresh, due + interval + PAUSED_REFRESH_POLL);
        assert_eq!(lifecycle.requested_generation, first + 1);
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_and_drains_an_in_flight_fetch() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let mut workers = JoinSet::new();
        workers.spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            let _ = started_tx.send(());
            pending::<FetchResult>().await
        });
        started_rx.await.expect("fetch task started");

        workers.shutdown().await;

        assert!(workers.is_empty());
        assert!(dropped.load(Ordering::Acquire));
    }
}
