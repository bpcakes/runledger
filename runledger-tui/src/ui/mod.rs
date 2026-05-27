mod dashboard;
mod definitions;
mod help;
mod job_detail;
mod jobs;
pub mod render;
mod workflows;

use ratatui::Frame;
use ratatui::layout::Constraint;

use crate::app::App;

pub fn draw(f: &mut Frame, app: &mut App) {
    if app.show_help {
        render::draw_top_chrome_underlay(f, app);
        help::draw(f);
        return;
    }

    if app.show_org_input {
        render::draw_top_chrome_underlay(f, app);
        draw_org_input(f, app);
        return;
    }

    if app.show_filter_input {
        render::draw_top_chrome_underlay(f, app);
        draw_filter_input(f, app);
        return;
    }

    render::draw_top_chrome(f, app);
}

fn draw_org_input(f: &mut Frame, app: &App) {
    let area = centered_popup(f.area(), 60, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let hint = "Organization UUID (empty = global). Enter confirm, Esc cancel.";
    let text = format!("{hint}\n\n> {}", app.org_input);
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Organization scope ")
            .borders(ratatui::widgets::Borders::ALL),
    );
    f.render_widget(block, area);
}

fn draw_filter_input(f: &mut Frame, app: &App) {
    let area = centered_popup(f.area(), 60, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let label = if app.filter_input_workflow {
        "workflow_type substring (empty = any)"
    } else {
        "job_type substring (empty = any)"
    };
    let text = format!("{label}\n\n> {}", app.filter_input);
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Filter ")
            .borders(ratatui::widgets::Borders::ALL),
    );
    f.render_widget(block, area);
}

fn centered_popup(
    area: ratatui::layout::Rect,
    percent_x: u16,
    percent_y: u16,
) -> ratatui::layout::Rect {
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
