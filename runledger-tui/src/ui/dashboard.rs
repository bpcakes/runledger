use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Paragraph};

use crate::app::App;
use crate::data::DashboardData;
use crate::ui::render::{
    CellAlign, TableColumn, TableRow, draw_table, scope_banner, status_style_warn, table_cell,
};

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &DashboardData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    f.render_widget(Paragraph::new(scope_banner(app.scope)), chunks[0]);
    draw_kpis(f, chunks[1], data);

    let columns = [
        TableColumn::left("Job type", Constraint::Min(24)),
        TableColumn::right("Pend", Constraint::Length(8)),
        TableColumn::right("Lease", Constraint::Length(8)),
        TableColumn::right("Stale", Constraint::Length(8)),
        TableColumn::right("OK 24h", Constraint::Length(9)).optional(2),
        TableColumn::right("DLQ 24h", Constraint::Length(9)).optional(1),
        TableColumn::right("P50 ms", Constraint::Length(9)).optional(2),
        TableColumn::right("P95 ms", Constraint::Length(9)).optional(1),
    ];
    let rows: Vec<TableRow> = data
        .metrics
        .iter()
        .filter(|m| {
            app.matches_table_search(vec![
                m.job_type.as_str().to_owned(),
                m.pending_count.to_string(),
                m.leased_count.to_string(),
                m.stale_leases.to_string(),
                m.dead_lettered_24h.to_string(),
            ])
        })
        .enumerate()
        .map(|(i, m)| {
            let warn = m.stale_leases > 0;
            let style = if warn {
                status_style_warn()
            } else {
                Style::default()
            };
            TableRow::new(vec![
                table_cell(m.job_type.as_str(), CellAlign::Left),
                table_cell(m.pending_count.to_string(), CellAlign::Right),
                table_cell(m.leased_count.to_string(), CellAlign::Right),
                table_cell(m.stale_leases.to_string(), CellAlign::Right),
                table_cell(m.succeeded_24h.to_string(), CellAlign::Right),
                table_cell(m.dead_lettered_24h.to_string(), CellAlign::Right),
                table_cell(
                    m.p50_duration_ms_24h
                        .map(|v| format!("{v:.0}"))
                        .unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Right,
                ),
                table_cell(
                    m.p95_duration_ms_24h
                        .map(|v| format!("{v:.0}"))
                        .unwrap_or_else(|| "—".to_owned()),
                    CellAlign::Right,
                ),
            ])
            .style(if i == app.list_selection && warn {
                style.add_modifier(Modifier::REVERSED)
            } else if warn {
                style
            } else {
                Style::default()
            })
        })
        .collect();

    draw_table(
        f,
        chunks[2],
        " Metrics ",
        &columns,
        rows,
        app.list_selection,
        "No job metrics in this scope.",
    );
}

fn draw_kpis(f: &mut Frame, area: ratatui::layout::Rect, data: &DashboardData) {
    let pending: i64 = data.metrics.iter().map(|m| m.pending_count).sum();
    let leased: i64 = data.metrics.iter().map(|m| m.leased_count).sum();
    let stale: i64 = data.metrics.iter().map(|m| m.stale_leases).sum();
    let dlq_24h: i64 = data.metrics.iter().map(|m| m.dead_lettered_24h).sum();
    let cells = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(17),
            Constraint::Percentage(17),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(17),
            Constraint::Percentage(17),
        ])
        .split(area);
    let kpis = [
        ("Pending", pending.to_string(), Color::Gray),
        ("Leased", leased.to_string(), Color::Cyan),
        ("Stale", stale.to_string(), Color::Yellow),
        ("DLQ 24h", dlq_24h.to_string(), Color::Red),
        ("WF failed", data.failed_workflows.to_string(), Color::Red),
        (
            "WF external",
            data.external_waits.to_string(),
            Color::Magenta,
        ),
    ];
    for (area, (label, value, color)) in cells.iter().zip(kpis) {
        f.render_widget(
            Paragraph::new(format!("{label}\n{value}"))
                .alignment(Alignment::Center)
                .style(Style::new().fg(color).add_modifier(Modifier::BOLD))
                .block(Block::default()),
            *area,
        );
    }
}
