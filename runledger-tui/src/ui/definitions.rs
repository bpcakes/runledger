use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Paragraph, Row};

use crate::app::App;
use crate::data::DefinitionsData;
use crate::format::truncate_str;
use crate::ui::render::draw_table;

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

    let headers = vec![
        "Job type", "Ver", "Enabled", "Max att", "Timeout", "Priority",
    ];
    let rows: Vec<Row> = data
        .definitions
        .iter()
        .map(|d| {
            Row::new(vec![
                truncate_str(d.job_type.as_str(), 32),
                d.version.to_string(),
                if d.is_enabled { "yes" } else { "no" }.to_owned(),
                d.max_attempts.to_string(),
                d.default_timeout_seconds.to_string(),
                d.default_priority.to_string(),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[1],
        " Job definitions ",
        headers,
        rows,
        app.list_selection,
    );
}
