use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::data::DefinitionsData;
use crate::format::truncate_str;
use crate::ui::render::{CellAlign, TableColumn, TableRow, draw_table, table_cell};

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &DefinitionsData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    let filter_line = format!(
        "job_type: {}",
        app.job_type_filter.as_deref().unwrap_or("(any)")
    );
    f.render_widget(Paragraph::new(filter_line), chunks[0]);

    let columns = [
        TableColumn::left("Job type", Constraint::Min(28)),
        TableColumn::right("Ver", Constraint::Length(6)).optional(1),
        TableColumn::left("Enabled", Constraint::Length(9)),
        TableColumn::right("Max att", Constraint::Length(8)).optional(1),
        TableColumn::right("Timeout", Constraint::Length(9)).optional(2),
        TableColumn::right("Priority", Constraint::Length(9)).optional(2),
    ];
    let rows: Vec<TableRow> = data
        .definitions
        .iter()
        .filter(|d| {
            app.matches_table_search(|| {
                vec![
                    d.job_type.as_str().to_owned(),
                    d.version.to_string(),
                    if d.is_enabled { "enabled" } else { "disabled" }.to_owned(),
                ]
            })
        })
        .map(|d| {
            TableRow::new(vec![
                table_cell(truncate_str(d.job_type.as_str(), 48), CellAlign::Left),
                table_cell(d.version.to_string(), CellAlign::Right),
                table_cell(if d.is_enabled { "yes" } else { "no" }, CellAlign::Left),
                table_cell(d.max_attempts.to_string(), CellAlign::Right),
                table_cell(d.default_timeout_seconds.to_string(), CellAlign::Right),
                table_cell(d.default_priority.to_string(), CellAlign::Right),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[1],
        " Job definitions ",
        &columns,
        rows,
        app.list_selection,
        "No job definitions match the current filter.",
    );
}
