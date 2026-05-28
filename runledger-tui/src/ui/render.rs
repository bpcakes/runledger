use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs};
use runledger_core::jobs::{JobStatus, WorkflowRunStatus, WorkflowStepStatus};

use crate::app::{App, Screen, TopScreen};
use crate::scope::Scope;

pub const TAB_LABELS: [&str; 4] = ["Dashboard", "Queue", "Workflows", "Definitions"];

#[derive(Debug, Clone, Copy)]
pub enum CellAlign {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
pub struct TableColumn {
    pub header: &'static str,
    pub width: Constraint,
    pub align: CellAlign,
    pub priority: u8,
}

impl TableColumn {
    #[must_use]
    pub const fn left(header: &'static str, width: Constraint) -> Self {
        Self {
            header,
            width,
            align: CellAlign::Left,
            priority: 0,
        }
    }

    #[must_use]
    pub const fn right(header: &'static str, width: Constraint) -> Self {
        Self {
            header,
            width,
            align: CellAlign::Right,
            priority: 0,
        }
    }

    #[must_use]
    pub const fn optional(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

pub struct TableRow {
    pub cells: Vec<Cell<'static>>,
    pub style: Style,
}

impl TableRow {
    #[must_use]
    pub fn new(cells: Vec<Cell<'static>>) -> Self {
        Self {
            cells,
            style: Style::default(),
        }
    }

    #[must_use]
    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

#[must_use]
pub fn table_cell(value: impl Into<String>, align: CellAlign) -> Cell<'static> {
    let alignment = match align {
        CellAlign::Left => Alignment::Left,
        CellAlign::Right => Alignment::Right,
    };
    Cell::new(Line::from(value.into()).alignment(alignment))
}

#[must_use]
pub fn status_style_warn() -> Style {
    Style::new().fg(ratatui::style::Color::Yellow)
}

#[must_use]
pub fn job_status_style(status: JobStatus) -> Style {
    match status {
        JobStatus::Pending => Style::new().fg(Color::Gray),
        JobStatus::Leased => Style::new().fg(Color::Cyan),
        JobStatus::Succeeded => Style::new().fg(Color::Green),
        JobStatus::DeadLettered => Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        JobStatus::Canceled => Style::new().fg(Color::DarkGray),
    }
}

#[must_use]
pub fn workflow_run_status_style(status: WorkflowRunStatus) -> Style {
    match status {
        WorkflowRunStatus::Running => Style::new().fg(Color::Cyan),
        WorkflowRunStatus::WaitingForExternal => Style::new().fg(Color::Magenta),
        WorkflowRunStatus::Succeeded => Style::new().fg(Color::Green),
        WorkflowRunStatus::CompletedWithErrors => {
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
        }
        WorkflowRunStatus::Canceled => Style::new().fg(Color::DarkGray),
    }
}

#[must_use]
pub fn workflow_step_status_style(status: WorkflowStepStatus) -> Style {
    match status {
        WorkflowStepStatus::Blocked => Style::new().fg(Color::Yellow),
        WorkflowStepStatus::WaitingForExternal => Style::new().fg(Color::Magenta),
        WorkflowStepStatus::Enqueued => Style::new().fg(Color::Gray),
        WorkflowStepStatus::Running => Style::new().fg(Color::Cyan),
        WorkflowStepStatus::Succeeded => Style::new().fg(Color::Green),
        WorkflowStepStatus::Failed => Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        WorkflowStepStatus::Canceled => Style::new().fg(Color::DarkGray),
    }
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
            Constraint::Length(1),
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
        Paragraph::new(app.status_line())
            .alignment(Alignment::Left)
            .style(if app.last_error.is_some() {
                Style::new().fg(Color::Red)
            } else {
                Style::new().fg(Color::Gray)
            }),
        chunks[2],
    );
    f.render_widget(
        Paragraph::new(app.key_hint_line()).style(Style::new().fg(Color::DarkGray)),
        chunks[3],
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
    match last_error {
        Some(error) => {
            let block = Paragraph::new(format!("{error}\n\nPress r to retry."))
                .block(Block::default().borders(Borders::ALL).title(" Error "))
                .style(Style::new().fg(Color::Red));
            f.render_widget(block, area);
        }
        None => {
            let block = Paragraph::new("Loading…")
                .alignment(Alignment::Center)
                .block(Block::default().borders(Borders::ALL).title(" Loading "));
            f.render_widget(block, area);
        }
    }
}

pub fn draw_table(
    f: &mut Frame,
    area: Rect,
    title: &'static str,
    columns: &[TableColumn],
    rows: Vec<TableRow>,
    selected: usize,
    empty_message: &str,
) {
    draw_table_with_selection(f, area, title, columns, rows, Some(selected), empty_message);
}

pub fn draw_table_unselected(
    f: &mut Frame,
    area: Rect,
    title: &'static str,
    columns: &[TableColumn],
    rows: Vec<TableRow>,
    empty_message: &str,
) {
    draw_table_with_selection(f, area, title, columns, rows, None, empty_message);
}

fn draw_table_with_selection(
    f: &mut Frame,
    area: Rect,
    title: &'static str,
    columns: &[TableColumn],
    rows: Vec<TableRow>,
    selected: Option<usize>,
    empty_message: &str,
) {
    let visible_columns = visible_column_indexes(area.width, columns);
    let headers = visible_columns
        .iter()
        .map(|index| table_cell(columns[*index].header, columns[*index].align));
    let header = Row::new(headers).style(Style::new().add_modifier(Modifier::BOLD));
    let widths: Vec<Constraint> = visible_columns
        .iter()
        .map(|index| columns[*index].width)
        .collect();
    let row_count = rows.len();
    if row_count == 0 {
        let block = Paragraph::new(empty_message.to_owned())
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL).title(title))
            .style(Style::new().fg(Color::DarkGray));
        f.render_widget(block, area);
        return;
    }
    let selected_label = selected
        .map(|index| format!(" {}/{} ", index.min(row_count - 1) + 1, row_count))
        .unwrap_or_else(|| format!(" {row_count} "));
    let compact_note = if visible_columns.len() < columns.len() {
        " compact: Enter for details"
    } else {
        ""
    };
    let rows = rows
        .into_iter()
        .map(|row| {
            let cells = row
                .cells
                .into_iter()
                .enumerate()
                .filter_map(|(index, cell)| visible_columns.contains(&index).then_some(cell));
            Row::new(cells).style(row.style)
        })
        .collect::<Vec<_>>();
    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(if selected.is_some() {
                    Borders::ALL
                } else {
                    Borders::TOP
                })
                .title(title)
                .title_bottom(
                    Line::from(format!("{selected_label}{compact_note}"))
                        .alignment(Alignment::Right),
                ),
        )
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

fn visible_column_indexes(width: u16, columns: &[TableColumn]) -> Vec<usize> {
    let max_priority = if width < 64 {
        0
    } else if width < 96 {
        1
    } else {
        u8::MAX
    };
    let visible = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (column.priority <= max_priority).then_some(index))
        .collect::<Vec<_>>();
    if visible.is_empty() { vec![0] } else { visible }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrow_tables_keep_only_required_columns() {
        let columns = [
            TableColumn::left("ID", Constraint::Length(10)),
            TableColumn::left("Type", Constraint::Min(20)),
            TableColumn::right("Updated", Constraint::Length(10)).optional(1),
            TableColumn::right("Worker", Constraint::Length(10)).optional(2),
        ];

        assert_eq!(visible_column_indexes(50, &columns), vec![0, 1]);
        assert_eq!(visible_column_indexes(80, &columns), vec![0, 1, 2]);
        assert_eq!(visible_column_indexes(120, &columns), vec![0, 1, 2, 3]);
    }

    #[test]
    fn narrow_tables_always_leave_a_column() {
        let columns = [TableColumn::left("Only", Constraint::Min(10)).optional(2)];
        assert_eq!(visible_column_indexes(20, &columns), vec![0]);
    }
}
