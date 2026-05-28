use crossterm::event::{KeyCode, KeyEvent};
use uuid::Uuid;

use crate::scope::Scope;

use super::super::App;

impl App {
    pub(in crate::app::input) fn handle_org_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_org_input = false,
            KeyCode::Enter => {
                let trimmed = self.org_input.trim();
                self.scope = if trimmed.is_empty() {
                    Scope::global()
                } else {
                    match Uuid::parse_str(trimmed) {
                        Ok(id) => Scope::for_org(id),
                        Err(_) => {
                            self.last_error = Some("Invalid organization UUID".to_owned());
                            self.show_org_input = false;
                            return false;
                        }
                    }
                };
                self.show_org_input = false;
                self.invalidate_cache();
                refresh = true;
            }
            KeyCode::Backspace => {
                self.org_input.pop();
            }
            KeyCode::Char(c) => self.org_input.push(c),
            _ => {}
        }
        refresh
    }

    pub(in crate::app::input) fn handle_filter_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_filter_input = false,
            KeyCode::Enter => {
                let trimmed = self.filter_input.trim();
                let value = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                if self.filter_input_workflow {
                    self.bump_fetch_generation();
                    self.workflow_type_filter = value;
                    self.workflows = None;
                } else {
                    self.bump_fetch_generation();
                    self.job_type_filter = value;
                    self.jobs = None;
                    self.definitions = None;
                }
                self.show_filter_input = false;
                refresh = true;
            }
            KeyCode::Backspace => {
                self.filter_input.pop();
            }
            KeyCode::Char(c) => self.filter_input.push(c),
            _ => {}
        }
        refresh
    }

    pub(in crate::app::input) fn handle_search_input_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => self.show_search_input = false,
            KeyCode::Enter => {
                let trimmed = self.search_input.trim();
                self.table_search = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                self.show_search_input = false;
                self.list_selection = 0;
                self.clamp_selection();
            }
            KeyCode::Backspace => {
                self.search_input.pop();
            }
            KeyCode::Char(c) => self.search_input.push(c),
            _ => {}
        }
        false
    }

    pub(in crate::app::input) fn handle_command_input_key(&mut self, key: KeyEvent) -> bool {
        let mut refresh = false;
        match key.code {
            KeyCode::Esc => self.show_command_input = false,
            KeyCode::Enter => {
                let command = self.command_input.trim().to_owned();
                refresh = self.execute_command(&command);
                self.show_command_input = false;
            }
            KeyCode::Backspace => {
                self.command_input.pop();
            }
            KeyCode::Char(c) => self.command_input.push(c),
            _ => {}
        }
        refresh
    }
}
