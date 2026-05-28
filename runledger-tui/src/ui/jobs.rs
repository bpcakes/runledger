use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::data::JobsData;
use crate::format::{format_relative_timestamp, job_status_label, short_uuid, truncate_str};
use crate::ui::render::{
    CellAlign, TableColumn, TableRow, draw_table, job_status_style, scope_banner, table_cell,
};

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &JobsData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(area);

    f.render_widget(Paragraph::new(scope_banner(app.scope)), chunks[0]);

    let filter_line = format!(
        "Filter: {} | job_type: {}",
        app.queue_filter.label(),
        app.job_type_filter.as_deref().unwrap_or("(any)")
    );
    f.render_widget(Paragraph::new(filter_line), chunks[1]);

    let columns = [
        TableColumn::left("ID", Constraint::Length(15)),
        TableColumn::left("Type", Constraint::Min(24)),
        TableColumn::left("Status", Constraint::Length(8)),
        TableColumn::left("Stage", Constraint::Length(12)).optional(1),
        TableColumn::right("Att", Constraint::Length(7)).optional(2),
        TableColumn::left("Worker", Constraint::Length(14)).optional(2),
        TableColumn::right("Updated", Constraint::Length(10)).optional(1),
    ];
    let rows: Vec<TableRow> = data
        .jobs
        .iter()
        .filter(|j| {
            app.matches_table_search(vec![
                j.id.to_string(),
                j.job_type.as_str().to_owned(),
                job_status_label(j.status).to_owned(),
                j.stage.as_db_value().to_owned(),
                j.worker_id.as_deref().unwrap_or("").to_owned(),
            ])
        })
        .map(|j| {
            TableRow::new(vec![
                table_cell(short_uuid(j.id), CellAlign::Left),
                table_cell(truncate_str(j.job_type.as_str(), 48), CellAlign::Left),
                table_cell(job_status_label(j.status), CellAlign::Left)
                    .style(job_status_style(j.status)),
                table_cell(j.stage.as_db_value(), CellAlign::Left),
                table_cell(
                    format!("{}/{}", j.attempt, j.max_attempts),
                    CellAlign::Right,
                ),
                table_cell(
                    j.worker_id
                        .as_deref()
                        .map(|w| truncate_str(w, 12))
                        .unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Left,
                ),
                table_cell(format_relative_timestamp(j.updated_at), CellAlign::Right),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[2],
        " Job queue ",
        &columns,
        rows,
        app.list_selection,
        "No jobs match the current scope and filters.",
    );
}
