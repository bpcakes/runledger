mod dashboard;
mod definitions;
mod help;
mod job_detail;
mod jobs;
pub mod render;
mod workflows;

use ratatui::Frame;
use ratatui::layout::Constraint;

use crate::app::fetch::FetchStatus;
use crate::app::{ActiveInput, App, FilterTarget};

pub fn draw(f: &mut Frame, app: &mut App, fetch_status: FetchStatus) {
    if app.show_help {
        render::draw_top_chrome_underlay(f, app, fetch_status);
        help::draw(f);
        return;
    }

    if !matches!(app.active_input(), ActiveInput::None) {
        render::draw_top_chrome_underlay(f, app, fetch_status);
        match app.active_input() {
            ActiveInput::None => {}
            ActiveInput::Organization { text } => draw_org_input(f, text),
            ActiveInput::Filter { target, text } => draw_filter_input(f, *target, text),
            ActiveInput::Search { text } => draw_search_input(f, text),
            ActiveInput::Command { text } => draw_command_input(f, text),
        }
        return;
    }

    render::draw_top_chrome(f, app, fetch_status);
}

fn draw_org_input(f: &mut Frame, input: &str) {
    let area = centered_popup(f.area(), 60, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let hint = "Organization UUID (empty = global). Enter confirm, Esc cancel.";
    let text = format!("{hint}\n\n> {input}");
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Organization scope ")
            .borders(ratatui::widgets::Borders::ALL),
    );
    f.render_widget(block, area);
}

fn draw_filter_input(f: &mut Frame, target: FilterTarget, input: &str) {
    let area = centered_popup(f.area(), 60, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let label = match target {
        FilterTarget::Job => "job_type substring (empty = any)",
        FilterTarget::Workflow => "workflow_type substring (empty = any)",
    };
    let text = format!("{label}\n\n> {input}");
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Filter ")
            .borders(ratatui::widgets::Borders::ALL),
    );
    f.render_widget(block, area);
}

fn draw_search_input(f: &mut Frame, input: &str) {
    let area = centered_popup(f.area(), 60, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let text = format!("Search current table (empty = clear)\n\n/ {input}");
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Search ")
            .borders(ratatui::widgets::Borders::ALL),
    );
    f.render_widget(block, area);
}

fn draw_command_input(f: &mut Frame, input: &str) {
    let area = centered_popup(f.area(), 70, 20);
    f.render_widget(ratatui::widgets::Clear, area);
    let text =
        format!("Commands: filter status dlq | scope global | refresh 5s | copy id\n\n: {input}");
    let block = ratatui::widgets::Paragraph::new(text).block(
        ratatui::widgets::Block::default()
            .title(" Command ")
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
