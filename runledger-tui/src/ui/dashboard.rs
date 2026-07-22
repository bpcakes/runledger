use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Paragraph};

use crate::app::App;
use crate::data::DashboardData;
use crate::ui::render::{
    CellAlign, TableColumn, TableRow, draw_table, scope_banner, status_style_warn, table_cell,
};

const DASHBOARD_COLUMNS: [TableColumn; 11] = [
    TableColumn::left("Job type", Constraint::Min(24)),
    TableColumn::right("Pend", Constraint::Length(8)),
    TableColumn::right("Lease", Constraint::Length(8)),
    TableColumn::right("Stale", Constraint::Length(8)),
    TableColumn::right("Cont 24h", Constraint::Length(9)).optional(3),
    TableColumn::right("Cont now", Constraint::Length(9)).optional(2),
    TableColumn::right("Max run", Constraint::Length(8)).optional(1),
    TableColumn::right("OK 24h", Constraint::Length(9)).optional(2),
    TableColumn::right("DLQ 24h", Constraint::Length(9)).optional(1),
    TableColumn::right("P50 ms", Constraint::Length(9)).optional(2),
    TableColumn::right("P95 ms", Constraint::Length(9)).optional(1),
];

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

    let rows: Vec<TableRow> = data
        .metrics
        .iter()
        .filter(|metric| app.dashboard_metric_matches_search(data, metric))
        .enumerate()
        .map(|(i, metric)| {
            let row = data.row_for(metric);
            let warn = row.has_stale_leases();
            let style = if warn {
                status_style_warn()
            } else {
                Style::default()
            };
            let cells = row
                .into_fields()
                .into_iter()
                .enumerate()
                .map(|(index, field)| {
                    let align = if index == 0 {
                        CellAlign::Left
                    } else {
                        CellAlign::Right
                    };
                    table_cell(field, align)
                })
                .collect();
            TableRow::new(cells).style(if i == app.list_selection && warn {
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
        &DASHBOARD_COLUMNS,
        rows,
        app.list_selection,
        "No job metrics in this scope.",
    );
}

fn draw_kpis(f: &mut Frame, area: ratatui::layout::Rect, data: &DashboardData) {
    let pending: i64 = data.metrics.iter().map(|m| m.pending_count).sum();
    let leased: i64 = data.metrics.iter().map(|m| m.leased_count).sum();
    let stale: i64 = data.metrics.iter().map(|m| m.stale_leases).sum();
    let active_continued: i64 = data
        .continuation_metrics
        .values()
        .map(|m| m.active_continued_count)
        .sum();
    let dlq_24h: i64 = data.metrics.iter().map(|m| m.dead_lettered_24h).sum();
    let cells = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
            Constraint::Ratio(1, 7),
        ])
        .split(area);
    let kpis = [
        ("Pending", pending.to_string(), Color::Gray),
        ("Leased", leased.to_string(), Color::Cyan),
        ("Stale", stale.to_string(), Color::Yellow),
        ("Cont active", active_continued.to_string(), Color::Magenta),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::render::{minimum_table_width, visible_column_indexes};

    #[test]
    fn dashboard_columns_change_at_exact_priority_tier_breakpoints() {
        let required = vec![0, 1, 2, 3];
        let through_priority_one = vec![0, 1, 2, 3, 6, 8, 10];
        let through_priority_two = vec![0, 1, 2, 3, 5, 6, 7, 8, 9, 10];
        let all = (0..DASHBOARD_COLUMNS.len()).collect::<Vec<_>>();

        let required_width = minimum_table_width(&DASHBOARD_COLUMNS, 0, true);
        let priority_one_width = minimum_table_width(&DASHBOARD_COLUMNS, 1, true);
        let priority_two_width = minimum_table_width(&DASHBOARD_COLUMNS, 2, true);
        let priority_three_width = minimum_table_width(&DASHBOARD_COLUMNS, 3, true);

        assert_eq!(required_width, 55);
        assert_eq!(priority_one_width, 84);
        assert_eq!(priority_two_width, 114);
        assert_eq!(priority_three_width, 124);

        assert_eq!(
            visible_column_indexes(required_width - 1, &DASHBOARD_COLUMNS, true),
            required
        );
        assert_eq!(
            visible_column_indexes(priority_one_width - 1, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            visible_column_indexes(priority_one_width, &DASHBOARD_COLUMNS, true),
            through_priority_one
        );
        assert_eq!(
            visible_column_indexes(95, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3, 6, 8, 10]
        );
        assert_eq!(
            visible_column_indexes(96, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3, 6, 8, 10]
        );
        assert_eq!(
            visible_column_indexes(priority_two_width - 1, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3, 6, 8, 10]
        );
        assert_eq!(
            visible_column_indexes(priority_two_width, &DASHBOARD_COLUMNS, true),
            through_priority_two
        );
        assert_eq!(
            visible_column_indexes(120, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3, 5, 6, 7, 8, 9, 10]
        );
        assert_eq!(
            visible_column_indexes(priority_three_width - 1, &DASHBOARD_COLUMNS, true),
            vec![0, 1, 2, 3, 5, 6, 7, 8, 9, 10]
        );
        assert_eq!(
            visible_column_indexes(priority_three_width, &DASHBOARD_COLUMNS, true),
            all
        );
    }
}
