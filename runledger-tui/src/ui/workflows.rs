use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Paragraph, Row};

use crate::app::App;
use crate::data::{WorkflowDetailData, WorkflowsData};
use crate::format::{
    format_optional_timestamp, format_timestamp, short_uuid, truncate_str,
    workflow_run_status_label, workflow_step_status_label,
};
use crate::ui::render::{draw_table, draw_table_unselected, scope_banner};

pub fn draw_runs(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &WorkflowsData) {
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
        "workflow_type: {}",
        app.workflow_type_filter.as_deref().unwrap_or("(any)")
    );
    f.render_widget(Paragraph::new(filter_line), chunks[1]);

    let headers = vec!["ID", "Type", "Status", "Started", "Finished"];
    let rows: Vec<Row> = data
        .runs
        .iter()
        .map(|r| {
            Row::new(vec![
                short_uuid(r.id),
                truncate_str(r.workflow_type.as_str(), 28),
                workflow_run_status_label(r.status).to_owned(),
                format_timestamp(r.started_at),
                format_optional_timestamp(r.finished_at),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[2],
        " Workflow runs ",
        headers,
        rows,
        app.list_selection,
    );
}

pub fn draw_detail(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    app: &App,
    data: &WorkflowDetailData,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Min(6),
            Constraint::Length(8),
        ])
        .split(area);

    f.render_widget(Paragraph::new(scope_banner(app.scope)), chunks[0]);

    let run = &data.run;
    let header = format!(
        "Run {} | {} | {}",
        short_uuid(run.id),
        run.workflow_type.as_str(),
        workflow_run_status_label(run.status),
    );
    f.render_widget(Paragraph::new(header), chunks[1]);

    let step_headers = vec!["Step", "Status", "Job", "Deps pend", "Job ID"];
    let step_rows: Vec<Row> = data
        .steps
        .iter()
        .map(|s| {
            Row::new(vec![
                s.step_key.as_str().to_owned(),
                workflow_step_status_label(s.status).to_owned(),
                s.job_type
                    .as_ref()
                    .map(|t| truncate_str(t.as_str(), 20))
                    .unwrap_or_else(|| "—".to_owned()),
                format!(
                    "{}/{}",
                    s.dependency_count_pending, s.dependency_count_total
                ),
                s.job_id.map(short_uuid).unwrap_or_else(|| "—".to_owned()),
            ])
        })
        .collect();
    draw_table(
        f,
        chunks[2],
        " Steps ",
        step_headers,
        step_rows,
        app.list_selection,
    );

    let dep_headers = vec!["Prereq step", "Dependent step", "Mode"];
    let step_key_by_id: std::collections::HashMap<_, _> = data
        .steps
        .iter()
        .map(|s| (s.id, s.step_key.as_str()))
        .collect();
    let dep_rows: Vec<Row> = data
        .dependencies
        .iter()
        .map(|d| {
            Row::new(vec![
                step_key_by_id
                    .get(&d.prerequisite_step_id)
                    .copied()
                    .unwrap_or("?")
                    .to_owned(),
                step_key_by_id
                    .get(&d.dependent_step_id)
                    .copied()
                    .unwrap_or("?")
                    .to_owned(),
                format!("{:?}", d.release_mode),
            ])
        })
        .collect();
    draw_table_unselected(f, chunks[3], " Dependencies ", dep_headers, dep_rows);
}
