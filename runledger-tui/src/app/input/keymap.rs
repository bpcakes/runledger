use crossterm::event::{KeyCode, KeyEvent};

use super::super::{ActiveInput, App, FilterTarget, JobDetailPane, Screen, TopScreen};

impl App {
    /// Returns true when a data refresh should be scheduled.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !matches!(self.active_input(), ActiveInput::None) {
            return self.handle_active_input_key(key);
        }
        if self.show_help && key.code != KeyCode::Char('?') && key.code != KeyCode::Esc {
            return false;
        }

        let mut refresh = false;
        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
            }
            KeyCode::Char('?') => self.show_help = !self.show_help,
            KeyCode::Esc => {
                if self.show_help {
                    self.show_help = false;
                } else if !self.screen_stack.is_empty() {
                    self.pop_screen();
                    refresh = true;
                }
            }
            KeyCode::Char('r') | KeyCode::Char('.') => {
                refresh = true;
            }
            KeyCode::Char('p') => {
                self.refresh_paused = !self.refresh_paused;
                self.notice = Some(if self.refresh_paused {
                    "Auto-refresh paused".to_owned()
                } else {
                    "Auto-refresh resumed".to_owned()
                });
            }
            KeyCode::Char('o') => {
                self.active_input = ActiveInput::Organization {
                    text: self
                        .scope
                        .organization_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                };
            }
            KeyCode::Char('/') => {
                self.active_input = ActiveInput::Search {
                    text: self.table_search.clone().unwrap_or_default(),
                };
            }
            KeyCode::Char('t') => {
                let target = if matches!(
                    self.screen,
                    Screen::Workflows | Screen::WorkflowDetail { .. }
                ) {
                    FilterTarget::Workflow
                } else {
                    FilterTarget::Job
                };
                let text = if target == FilterTarget::Workflow {
                    self.workflow_type_filter.clone().unwrap_or_default()
                } else {
                    self.job_type_filter.clone().unwrap_or_default()
                };
                self.active_input = ActiveInput::Filter { target, text };
            }
            KeyCode::Char('w') if matches!(self.screen, Screen::Workflows) => {
                self.active_input = ActiveInput::Filter {
                    target: FilterTarget::Workflow,
                    text: self.workflow_type_filter.clone().unwrap_or_default(),
                };
            }
            KeyCode::Char(':') => {
                self.active_input = ActiveInput::Command {
                    text: String::new(),
                };
            }
            KeyCode::Char('c') => {
                refresh = self.clear_context_filters();
            }
            KeyCode::Char('y') => self.copy_selected_identifier(),
            KeyCode::Char('f') if matches!(self.screen, Screen::Queue) => {
                refresh = self.transition_queue_status_filter(self.queue_filter.next());
            }
            KeyCode::Char('1') => {
                self.navigate_top(TopScreen::Dashboard);
                refresh = true;
            }
            KeyCode::Char('2') => {
                self.navigate_top(TopScreen::Queue);
                refresh = true;
            }
            KeyCode::Char('3') => {
                self.navigate_top(TopScreen::Workflows);
                refresh = true;
            }
            KeyCode::Char('4') => {
                self.navigate_top(TopScreen::Definitions);
                refresh = true;
            }
            KeyCode::Tab => {
                let next =
                    top_screen_from_index((self.top_screen_index() + 1) % Self::TOP_SCREEN_COUNT);
                self.navigate_top(next);
                refresh = true;
            }
            KeyCode::BackTab => {
                let next = top_screen_from_index(
                    (self.top_screen_index() + Self::TOP_SCREEN_COUNT - 1) % Self::TOP_SCREEN_COUNT,
                );
                self.navigate_top(next);
                refresh = true;
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1),
            KeyCode::Char('g') | KeyCode::Home => self.move_to_start(),
            KeyCode::Char('G') | KeyCode::End => self.move_to_end(),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::Char('h') => {
                if !self.screen_stack.is_empty() {
                    self.pop_screen();
                    refresh = true;
                }
            }
            KeyCode::Char('v')
                if matches!(
                    (&self.screen, self.job_detail_pane),
                    (Screen::JobDetail { .. }, JobDetailPane::Payload)
                ) =>
            {
                self.payload_wrap = !self.payload_wrap;
                self.notice = Some(if self.payload_wrap {
                    "Payload wrap enabled".to_owned()
                } else {
                    "Payload wrap disabled".to_owned()
                });
            }
            KeyCode::Char('R')
                if matches!(
                    (&self.screen, self.job_detail_pane),
                    (Screen::JobDetail { .. }, JobDetailPane::Payload)
                ) =>
            {
                self.payload_raw = !self.payload_raw;
                self.job_detail_viewport.scroll = 0;
                self.notice = Some(if self.payload_raw {
                    "Payload raw mode".to_owned()
                } else {
                    "Payload pretty mode".to_owned()
                });
            }
            KeyCode::Char(']') | KeyCode::Right
                if matches!(self.screen, Screen::JobDetail { .. }) =>
            {
                self.job_detail_pane = self.job_detail_pane.next();
                self.job_detail_viewport.scroll = 0;
                self.list_selection = 0;
            }
            KeyCode::Char('[') | KeyCode::Left
                if matches!(self.screen, Screen::JobDetail { .. }) =>
            {
                self.job_detail_pane = self.job_detail_pane.prev();
                self.job_detail_viewport.scroll = 0;
                self.list_selection = 0;
            }
            KeyCode::Char('l') | KeyCode::Enter => {
                let before = self.screen.clone();
                self.activate_selection();
                if self.screen != before {
                    refresh = true;
                }
            }
            _ => {}
        }
        refresh
    }
}

fn top_screen_from_index(index: usize) -> TopScreen {
    match index {
        0 => TopScreen::Dashboard,
        1 => TopScreen::Queue,
        2 => TopScreen::Workflows,
        _ => TopScreen::Definitions,
    }
}
