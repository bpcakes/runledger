use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

pub fn draw(f: &mut Frame) {
    let area = centered_rect(70, 80, f.area());
    f.render_widget(Clear, area);
    let lines = vec![
        Line::from(Span::styled(
            "runledger-tui (read-only)",
            Style::new().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Navigation"),
        Line::from("  1-4 / Tab     Switch top-level screen"),
        Line::from("  Shift+Tab     Previous top-level screen"),
        Line::from("  j / k         Move selection"),
        Line::from("  Enter         Open job or workflow detail"),
        Line::from("  Esc           Back from detail"),
        Line::from("  [ / ]         Job detail sub-panes"),
        Line::from("  f             Cycle queue status filter"),
        Line::from("  /             Edit job_type filter (queue/definitions)"),
        Line::from("  w             Edit workflow_type filter"),
        Line::from("  o             Set organization scope (UUID or empty=global)"),
        Line::from("  r             Force refresh"),
        Line::from("  ?             Toggle this help"),
        Line::from("  q             Quit"),
        Line::from(""),
        Line::from(
            "Default scope is global (all organizations). Use --org at startup or o at runtime.",
        ),
    ];
    let block = Paragraph::new(lines).block(
        Block::default()
            .title(" Help ")
            .borders(Borders::ALL)
            .style(Style::new().add_modifier(Modifier::REVERSED)),
    );
    f.render_widget(block, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
