use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, Tabs};

use crate::app::{App, Screen, TopScreen};
use crate::scope::Scope;

pub const TAB_LABELS: [&str; 4] = ["Dashboard", "Queue", "Workflows", "Definitions"];

#[must_use]
pub fn status_style_warn() -> Style {
    Style::new().fg(ratatui::style::Color::Yellow)
}

pub fn draw_top_chrome_underlay(f: &mut Frame, app: &App) {
    draw_tabs_and_status(f, app);
}

pub fn draw_top_chrome(f: &mut Frame, app: &mut App) {
    let content_area = draw_tabs_and_status(f, app);
    draw_screen_content(f, content_area, app);
}

fn draw_tabs_and_status(f: &mut Frame, app: &App) -> Rect {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(area);

    let tabs = Tabs::new(TAB_LABELS)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" runledger-tui "),
        )
        .select(app.top_screen_index())
        .highlight_style(Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED));
    f.render_widget(tabs, chunks[0]);

    f.render_widget(
        Paragraph::new(app.status_line()).alignment(Alignment::Left),
        chunks[2],
    );

    chunks[1]
}

fn draw_screen_content(f: &mut Frame, area: Rect, app: &mut App) {
    match &app.screen {
        Screen::Dashboard => {
            if let Some(data) = &app.dashboard {
                super::dashboard::draw(f, area, app, data);
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
        Screen::Queue => {
            if let Some(data) = &app.jobs {
                super::jobs::draw(f, area, app, data);
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
        Screen::JobDetail { job_id } => {
            if let Some(data) = app.job_detail.clone() {
                if data.job.id == *job_id {
                    super::job_detail::draw(f, area, app, &data);
                } else {
                    draw_loading(f, area, &app.last_error);
                }
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
        Screen::Workflows => {
            if let Some(data) = &app.workflows {
                super::workflows::draw_runs(f, area, app, data);
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
        Screen::WorkflowDetail { run_id } => {
            if let Some(data) = &app.workflow_detail {
                if data.run.id == *run_id {
                    super::workflows::draw_detail(f, area, app, data);
                } else {
                    draw_loading(f, area, &app.last_error);
                }
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
        Screen::Definitions => {
            if let Some(data) = &app.definitions {
                super::definitions::draw(f, area, app, data);
            } else {
                draw_loading(f, area, &app.last_error);
            }
        }
    }
}

pub fn draw_loading(f: &mut Frame, area: Rect, last_error: &Option<String>) {
    let msg = match last_error {
        Some(e) => format!("Error: {e}\n\n(press r to retry)"),
        None => "Loading…".to_owned(),
    };
    let block =
        Paragraph::new(msg).block(Block::default().borders(Borders::ALL).title(" Loading "));
    f.render_widget(block, area);
}

pub fn draw_table(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: Vec<Row<'_>>,
    selected: usize,
) {
    draw_table_with_selection(f, area, title, headers, rows, Some(selected));
}

pub fn draw_table_unselected(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: Vec<Row<'_>>,
) {
    draw_table_with_selection(f, area, title, headers, rows, None);
}

fn draw_table_with_selection(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: Vec<Row<'_>>,
    selected: Option<usize>,
) {
    let col_count = headers.len();
    let header = Row::new(headers).style(Style::new().add_modifier(Modifier::BOLD));
    let widths: Vec<Constraint> = (0..col_count).map(|_| Constraint::Min(8)).collect();
    let row_count = rows.len();
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▶ ");
    let mut state = ratatui::widgets::TableState::default();
    if let Some(selected) = selected
        && row_count > 0
    {
        state.select(Some(selected.min(row_count - 1)));
    }
    f.render_stateful_widget(table, area, &mut state);
}

pub fn scope_banner(scope: Scope) -> Line<'static> {
    let label = scope.label();
    Line::from(vec![
        Span::raw("Scope: "),
        Span::styled(
            label,
            if scope.organization_id.is_none() {
                Style::new().fg(ratatui::style::Color::Cyan)
            } else {
                Style::new().fg(ratatui::style::Color::Magenta)
            },
        ),
    ])
}

pub fn top_screen_from_index(index: usize) -> TopScreen {
    match index {
        0 => TopScreen::Dashboard,
        1 => TopScreen::Queue,
        2 => TopScreen::Workflows,
        _ => TopScreen::Definitions,
    }
}

pub fn screen_from_top(top: TopScreen) -> Screen {
    match top {
        TopScreen::Dashboard => Screen::Dashboard,
        TopScreen::Queue => Screen::Queue,
        TopScreen::Workflows => Screen::Workflows,
        TopScreen::Definitions => Screen::Definitions,
    }
}
