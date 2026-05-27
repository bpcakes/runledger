use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Row, Tabs};

use crate::app::{App, JobDetailPane};
use crate::data::JobDetailData;
use crate::format::{
    format_optional_timestamp, format_timestamp, job_payload_lines, job_status_label, short_uuid,
    truncate_str,
};
use crate::ui::render::{draw_table, scope_banner};

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, app: &mut App, data: &JobDetailData) {
    let job = &data.job;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    f.render_widget(Paragraph::new(scope_banner(app.scope)), chunks[0]);

    let header = format!(
        "Job {} | {} | {} | run {} att {}/{}",
        short_uuid(job.id),
        job.job_type.as_str(),
        job_status_label(job.status),
        job.run_number,
        job.attempt,
        job.max_attempts,
    );
    f.render_widget(
        Paragraph::new(header).style(Style::new().add_modifier(Modifier::BOLD)),
        chunks[1],
    );

    let pane_labels = ["Summary", "Events", "Logs", "Payload"];
    let tabs = Tabs::new(pane_labels)
        .select(job_detail_pane_index(app.job_detail_pane))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_widget(tabs, chunks[2]);

    match app.job_detail_pane {
        JobDetailPane::Summary => draw_summary(f, chunks[3], data),
        JobDetailPane::Events => draw_events(f, chunks[3], app, data),
        JobDetailPane::Logs => draw_logs(f, chunks[3], app, data),
        JobDetailPane::Payload => draw_payload(f, chunks[3], app, data),
    }
}

fn job_detail_pane_index(pane: JobDetailPane) -> usize {
    match pane {
        JobDetailPane::Summary => 0,
        JobDetailPane::Events => 1,
        JobDetailPane::Logs => 2,
        JobDetailPane::Payload => 3,
    }
}

fn draw_summary(f: &mut Frame, area: ratatui::layout::Rect, data: &JobDetailData) {
    let job = &data.job;
    let mut lines = vec![
        Line::from(format!("Full ID: {}", job.id)),
        Line::from(format!(
            "Organization: {}",
            job.organization_id
                .map(|o| o.to_string())
                .unwrap_or_else(|| "(none)".to_owned())
        )),
        Line::from(format!("Stage: {}", job.stage.as_db_value())),
        Line::from(format!(
            "Progress: {} / {}",
            job.progress_done
                .map(|v| v.to_string())
                .unwrap_or_else(|| "—".to_owned()),
            job.progress_total
                .map(|v| v.to_string())
                .unwrap_or_else(|| "—".to_owned()),
        )),
        Line::from(format!(
            "Lease expires: {}",
            format_optional_timestamp(job.lease_expires_at)
        )),
        Line::from(format!(
            "Last heartbeat: {}",
            format_optional_timestamp(job.last_heartbeat_at)
        )),
        Line::from(format!(
            "Worker: {}",
            job.worker_id.as_deref().unwrap_or("—")
        )),
        Line::from(format!(
            "Started: {}",
            format_optional_timestamp(job.started_at)
        )),
        Line::from(format!(
            "Finished: {}",
            format_optional_timestamp(job.finished_at)
        )),
        Line::from(format!("Created: {}", format_timestamp(job.created_at))),
        Line::from(format!("Updated: {}", format_timestamp(job.updated_at))),
    ];
    if let Some(code) = &job.last_error_code {
        lines.push(Line::from(vec![
            Span::styled("Error: ", Style::new().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "{} — {}",
                code,
                job.last_error_message.as_deref().unwrap_or("")
            )),
        ]));
    }
    if let Some(wf) = data.workflow_run_id {
        lines.push(Line::from(format!("Workflow run: {}", short_uuid(wf))));
    }
    if let Some(key) = &job.idempotency_key {
        lines.push(Line::from(format!("Idempotency: {key}")));
    }
    let block = Paragraph::new(lines)
        .block(ratatui::widgets::Block::default().borders(ratatui::widgets::Borders::ALL));
    f.render_widget(block, area);
}

fn draw_events(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &JobDetailData) {
    let headers = vec!["ID", "Type", "Stage", "When"];
    let rows: Vec<Row> = data
        .events
        .iter()
        .map(|e| {
            Row::new(vec![
                e.id.to_string(),
                e.event_type.as_db_value().to_owned(),
                e.stage
                    .map(|s| s.as_db_value().to_owned())
                    .unwrap_or_else(|| "—".to_owned()),
                format_timestamp(e.occurred_at),
            ])
        })
        .collect();
    draw_table(f, area, " Events ", headers, rows, app.list_selection);
}

fn draw_logs(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &JobDetailData) {
    let headers = vec!["ID", "Level", "Message", "When"];
    let rows: Vec<Row> = data
        .logs
        .iter()
        .map(|l| {
            Row::new(vec![
                l.id.to_string(),
                l.level.clone(),
                truncate_str(&l.message, 48),
                format_timestamp(l.occurred_at),
            ])
        })
        .collect();
    draw_table(f, area, " Logs ", headers, rows, app.list_selection);
}

fn draw_payload(f: &mut Frame, area: ratatui::layout::Rect, app: &mut App, data: &JobDetailData) {
    let lines = job_payload_lines(&data.job.payload);
    let visible_rows = usize::from(area.height.saturating_sub(2));
    app.update_payload_visible_rows(visible_rows);
    let scroll = app.detail_scroll;
    let visible: Vec<Line> = lines
        .iter()
        .skip(scroll)
        .map(|l| Line::from(l.as_str()))
        .collect();
    let block = Paragraph::new(visible).block(
        ratatui::widgets::Block::default()
            .borders(ratatui::widgets::Borders::ALL)
            .title(" Payload JSON "),
    );
    f.render_widget(block, area);
}
