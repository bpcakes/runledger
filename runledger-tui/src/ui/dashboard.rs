use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Paragraph, Row};

use crate::app::App;
use crate::data::DashboardData;
use crate::ui::render::{draw_table, scope_banner, status_style_warn};

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, app: &App, data: &DashboardData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    f.render_widget(Paragraph::new(scope_banner(app.scope)), chunks[0]);

    let headers = vec![
        "Job type", "Pend", "Lease", "Stale", "OK 24h", "DLQ 24h", "P50 ms", "P95 ms",
    ];
    let rows: Vec<Row> = data
        .metrics
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let warn = m.stale_leases > 0;
            let style = if warn {
                status_style_warn()
            } else {
                Style::default()
            };
            Row::new(vec![
                m.job_type.as_str().to_owned(),
                m.pending_count.to_string(),
                m.leased_count.to_string(),
                m.stale_leases.to_string(),
                m.succeeded_24h.to_string(),
                m.dead_lettered_24h.to_string(),
                m.p50_duration_ms_24h
                    .map(|v| format!("{v:.0}"))
                    .unwrap_or_else(|| "—".to_owned()),
                m.p95_duration_ms_24h
                    .map(|v| format!("{v:.0}"))
                    .unwrap_or_else(|| "—".to_owned()),
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
        chunks[1],
        " Metrics (job_metrics_rollup) ",
        headers,
        rows,
        app.list_selection,
    );
}
