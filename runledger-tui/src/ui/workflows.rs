use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::data::{WorkflowDetailData, WorkflowsData};
use crate::format::{
    format_optional_timestamp, format_relative_timestamp, short_uuid, truncate_str,
    workflow_run_status_label, workflow_step_status_label,
};
use crate::ui::render::{
    CellAlign, TableColumn, TableEnterAction, TableRow, TableSelection, draw_table,
    draw_table_unselected, scope_banner, table_cell, workflow_run_status_style,
    workflow_step_status_style,
};

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

    let columns = [
        TableColumn::left("ID", Constraint::Length(15)),
        TableColumn::left("Type", Constraint::Min(28)),
        TableColumn::left("Status", Constraint::Length(8)),
        TableColumn::right("Started", Constraint::Length(10)).optional(1),
        TableColumn::right("Finished", Constraint::Length(19)).optional(2),
    ];
    let rows: Vec<TableRow> = app
        .visible_workflow_runs(&data.runs)
        .map(|r| {
            TableRow::new(vec![
                table_cell(short_uuid(r.id), CellAlign::Left),
                table_cell(truncate_str(r.workflow_type.as_str(), 48), CellAlign::Left),
                table_cell(workflow_run_status_label(r.status), CellAlign::Left)
                    .style(workflow_run_status_style(r.status)),
                table_cell(format_relative_timestamp(r.started_at), CellAlign::Right),
                table_cell(format_optional_timestamp(r.finished_at), CellAlign::Right),
            ])
        })
        .collect();

    draw_table(
        f,
        chunks[2],
        " Workflow runs ",
        &columns,
        rows,
        TableSelection::new(app.list_selection, TableEnterAction::OpenDetails),
        "No workflow runs match the current scope and filters.",
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
    let mut header = format!(
        "Run {} | {} | {}",
        short_uuid(run.id),
        run.workflow_type.as_str(),
        workflow_run_status_label(run.status),
    );
    if data.steps_truncated() || data.dependencies_truncated() {
        header.push_str(&format!(
            "\nShowing {} of {} steps and {} of {} dependencies.",
            data.steps.len(),
            data.steps_total,
            data.dependencies.len(),
            data.dependencies_total
        ));
    }
    f.render_widget(Paragraph::new(header), chunks[1]);

    let step_columns = [
        TableColumn::left("Step", Constraint::Min(18)),
        TableColumn::left("Status", Constraint::Length(8)),
        TableColumn::left("Job", Constraint::Min(18)).optional(1),
        TableColumn::right("Deps", Constraint::Length(8)).optional(1),
        TableColumn::left("Job ID", Constraint::Length(15)).optional(2),
    ];
    let filtered_steps: Vec<_> = app.visible_workflow_steps(&data.steps).collect();
    let selected_step_id = filtered_steps
        .get(
            app.list_selection
                .min(filtered_steps.len().saturating_sub(1)),
        )
        .map(|step| step.id);
    let step_enter_action = if app.selected_workflow_step_job_id().is_some() {
        TableEnterAction::OpenDetails
    } else {
        TableEnterAction::None
    };
    let step_rows: Vec<TableRow> = filtered_steps
        .iter()
        .copied()
        .map(|s| {
            TableRow::new(vec![
                table_cell(s.step_key.as_str(), CellAlign::Left),
                table_cell(workflow_step_status_label(s.status), CellAlign::Left)
                    .style(workflow_step_status_style(s.status)),
                table_cell(
                    s.job_type
                        .as_ref()
                        .map(|t| truncate_str(t.as_str(), 32))
                        .unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Left,
                ),
                table_cell(
                    format!(
                        "{}/{}",
                        s.dependency_count_pending, s.dependency_count_total
                    ),
                    CellAlign::Right,
                ),
                table_cell(
                    s.job_id.map(short_uuid).unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Left,
                ),
            ])
        })
        .collect();
    draw_table(
        f,
        chunks[2],
        " Steps ",
        &step_columns,
        step_rows,
        TableSelection::new(app.list_selection, step_enter_action),
        "No workflow steps match the current search.",
    );

    let dep_columns = [
        TableColumn::left("Direction", Constraint::Length(10)),
        TableColumn::left("Prereq step", Constraint::Percentage(35)),
        TableColumn::left("Dependent step", Constraint::Percentage(35)),
        TableColumn::left("Mode", Constraint::Percentage(20)).optional(1),
    ];
    let step_key_by_id: std::collections::HashMap<_, _> = data
        .steps
        .iter()
        .map(|s| (s.id, s.step_key.as_str()))
        .collect();
    let missing_step_label = if data.steps_truncated() {
        "(outside page)"
    } else {
        "?"
    };
    let dep_rows: Vec<TableRow> = data
        .dependencies
        .iter()
        .filter(|d| {
            selected_step_id.is_none_or(|selected| {
                d.prerequisite_step_id == selected || d.dependent_step_id == selected
            })
        })
        .map(|d| {
            let direction = selected_step_id.map_or("all", |selected| {
                if d.prerequisite_step_id == selected {
                    "downstream"
                } else {
                    "upstream"
                }
            });
            TableRow::new(vec![
                table_cell(direction, CellAlign::Left),
                table_cell(
                    step_key_by_id
                        .get(&d.prerequisite_step_id)
                        .copied()
                        .unwrap_or(missing_step_label)
                        .to_owned(),
                    CellAlign::Left,
                ),
                table_cell(
                    step_key_by_id
                        .get(&d.dependent_step_id)
                        .copied()
                        .unwrap_or(missing_step_label)
                        .to_owned(),
                    CellAlign::Left,
                ),
                table_cell(format!("{:?}", d.release_mode), CellAlign::Left),
            ])
        })
        .collect();
    let dependency_empty = if data.dependencies_truncated() {
        "No direct dependencies in the fetched page; more dependency rows are available."
    } else {
        "The selected step has no direct dependencies."
    };
    draw_table_unselected(
        f,
        chunks[3],
        " Dependencies for selected step ",
        &dep_columns,
        dep_rows,
        dependency_empty,
    );
}
