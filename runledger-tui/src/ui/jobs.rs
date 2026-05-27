use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Paragraph, Row};

use crate::app::App;
use crate::data::JobsData;
use crate::format::{format_timestamp, job_status_label, short_uuid, truncate_str};
use crate::ui::render::{draw_table, scope_banner};

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

    let headers = vec!["ID", "Type", "Status", "Stage", "Att", "Worker", "Updated"];
    let rows: Vec<Row> = data
        .jobs
        .iter()
        .map(|j| {
            Row::new(vec![
                short_uuid(j.id),
                truncate_str(j.job_type.as_str(), 24),
                job_status_label(j.status).to_owned(),
                j.stage.as_db_value().to_owned(),
                format!("{}/{}", j.attempt, j.max_attempts),
                j.worker_id
                    .as_deref()
                    .map(|w| truncate_str(w, 12))
                    .unwrap_or_else(|| "—".to_owned()),
                format_timestamp(j.updated_at),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[2],
        " Job queue ",
        headers,
        rows,
        app.list_selection,
    );
}
