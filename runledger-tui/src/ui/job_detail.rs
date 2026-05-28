use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};

use crate::app::{App, JobDetailPane};
use crate::data::JobDetailData;
use crate::format::{
    format_optional_timestamp, format_timestamp, job_payload_lines, job_payload_raw_lines,
    job_status_label, short_uuid, truncate_str,
};
use crate::ui::render::{
    CellAlign, TableColumn, TableRow, draw_table, job_status_style, scope_banner, table_cell,
};

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
        Paragraph::new(header).style(job_status_style(job.status).add_modifier(Modifier::BOLD)),
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
    let progress = format!(
        "{} / {}",
        job.progress_done
            .map(|v| v.to_string())
            .unwrap_or_else(|| "—".to_owned()),
        job.progress_total
            .map(|v| v.to_string())
            .unwrap_or_else(|| "—".to_owned()),
    );
    let mut facts = vec![
        ("Full ID", job.id.to_string()),
        (
            "Organization",
            job.organization_id
                .map(|o| o.to_string())
                .unwrap_or_else(|| "(none)".to_owned()),
        ),
        ("Status", job_status_label(job.status).to_owned()),
        ("Stage", job.stage.as_db_value().to_owned()),
        ("Progress", progress),
        (
            "Run / attempt",
            format!("{} / {}", job.run_number, job.attempt),
        ),
        ("Worker", job.worker_id.as_deref().unwrap_or("—").to_owned()),
        (
            "Lease expires",
            format_optional_timestamp(job.lease_expires_at),
        ),
        (
            "Last heartbeat",
            format_optional_timestamp(job.last_heartbeat_at),
        ),
        ("Started", format_optional_timestamp(job.started_at)),
        ("Finished", format_optional_timestamp(job.finished_at)),
        ("Created", format_timestamp(job.created_at)),
        ("Updated", format_timestamp(job.updated_at)),
    ];
    if let Some(wf) = data.workflow_run_id {
        facts.push(("Workflow run", wf.to_string()));
    }
    if let Some(key) = &job.idempotency_key {
        facts.push(("Idempotency", key.clone()));
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(4)])
        .split(area);
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[0]);
    let midpoint = facts.len().div_ceil(2);
    draw_fact_column(f, columns[0], &facts[..midpoint], job.status);
    draw_fact_column(f, columns[1], &facts[midpoint..], job.status);

    let error_text = if let Some(code) = &job.last_error_code {
        format!(
            "{} — {}",
            code,
            job.last_error_message.as_deref().unwrap_or("")
        )
    } else {
        "No recorded error".to_owned()
    };
    f.render_widget(
        Paragraph::new(error_text)
            .style(if job.last_error_code.is_some() {
                Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::new().fg(Color::DarkGray)
            })
            .block(Block::default().borders(Borders::ALL).title(" Error ")),
        chunks[1],
    );
}

fn draw_events(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &JobDetailData) {
    let columns = [
        TableColumn::right("ID", Constraint::Length(8)),
        TableColumn::left("Type", Constraint::Min(18)),
        TableColumn::left("Stage", Constraint::Length(14)).optional(1),
        TableColumn::right("When", Constraint::Length(19)).optional(1),
    ];
    let rows: Vec<TableRow> = data
        .events
        .iter()
        .filter(|e| {
            app.matches_table_search(vec![
                e.id.to_string(),
                e.event_type.as_db_value().to_owned(),
                e.stage.map(|s| s.as_db_value()).unwrap_or("").to_owned(),
            ])
        })
        .map(|e| {
            TableRow::new(vec![
                table_cell(e.id.to_string(), CellAlign::Right),
                table_cell(e.event_type.as_db_value(), CellAlign::Left),
                table_cell(
                    e.stage
                        .map(|s| s.as_db_value().to_owned())
                        .unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Left,
                ),
                table_cell(format_timestamp(e.occurred_at), CellAlign::Right),
            ])
        })
        .collect();
    draw_table(
        f,
        area,
        " Events ",
        &columns,
        rows,
        app.list_selection,
        "No job events match the current search.",
    );
}

fn draw_logs(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &JobDetailData) {
    let columns = [
        TableColumn::right("ID", Constraint::Length(8)).optional(1),
        TableColumn::left("Level", Constraint::Length(8)),
        TableColumn::left("Message", Constraint::Min(32)),
        TableColumn::right("When", Constraint::Length(19)).optional(1),
    ];
    let rows: Vec<TableRow> = data
        .logs
        .iter()
        .filter(|l| {
            app.matches_table_search(vec![l.id.to_string(), l.level.clone(), l.message.clone()])
        })
        .map(|l| {
            TableRow::new(vec![
                table_cell(l.id.to_string(), CellAlign::Right),
                table_cell(l.level.clone(), CellAlign::Left),
                table_cell(truncate_str(&l.message, 96), CellAlign::Left),
                table_cell(format_timestamp(l.occurred_at), CellAlign::Right),
            ])
        })
        .collect();
    draw_table(
        f,
        area,
        " Logs ",
        &columns,
        rows,
        app.list_selection,
        "No log lines match the current search.",
    );
}

fn draw_payload(f: &mut Frame, area: ratatui::layout::Rect, app: &mut App, data: &JobDetailData) {
    let lines = if app.payload_raw {
        job_payload_raw_lines(&data.job.payload)
    } else {
        job_payload_lines(&data.job.payload)
    };
    let visible_rows = usize::from(area.height.saturating_sub(2));
    app.update_payload_visible_rows(visible_rows);
    let searched: Vec<_> = lines
        .iter()
        .filter(|line| {
            app.table_search_query()
                .is_none_or(|q| line.to_ascii_lowercase().contains(&q.to_ascii_lowercase()))
        })
        .collect();
    app.detail_scroll = app.detail_scroll.min(crate::format::job_payload_scroll_max(
        searched.len(),
        app.payload_visible_rows,
    ));
    let scroll = app.detail_scroll;
    let visible: Vec<Line> = searched
        .into_iter()
        .skip(scroll)
        .map(|l| Line::from(l.as_str()))
        .collect();
    let title = if app.payload_raw {
        " Payload JSON raw "
    } else {
        " Payload JSON pretty "
    };
    let mut block =
        Paragraph::new(visible).block(Block::default().borders(Borders::ALL).title(title));
    if app.payload_wrap {
        block = block.wrap(Wrap { trim: false });
    }
    f.render_widget(block, area);
}

fn draw_fact_column(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    facts: &[(&str, String)],
    status: runledger_core::jobs::JobStatus,
) {
    let lines: Vec<Line> = facts
        .iter()
        .map(|(label, value)| {
            let value_style = if *label == "Status" {
                job_status_style(status)
            } else {
                Style::default()
            };
            Line::from(vec![
                Span::styled(format!("{label:<15}"), Style::new().fg(Color::DarkGray)),
                Span::styled(value.clone(), value_style),
            ])
        })
        .collect();
    f.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Left)
            .block(Block::default().borders(Borders::TOP)),
        area,
    );
}
