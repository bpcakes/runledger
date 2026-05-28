use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use runledger_postgres::DbPool;
use tokio::sync::{Mutex, mpsc};

use crate::app::App;
use crate::app::fetch::{FetchRequest, execute_fetch};
use crate::config::Config;
use crate::ui;

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

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            let mut guard = app.lock().await;
            if guard.handle_key(key) {
                need_fetch = true;
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
