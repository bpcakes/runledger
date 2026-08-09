use crossterm::event::{KeyCode, KeyEvent};
use uuid::Uuid;

use crate::scope::Scope;

use super::super::{ActiveInput, App, FilterTarget};

impl App {
    pub(in crate::app::input) fn handle_active_input_key(&mut self, key: KeyEvent) -> bool {
        match std::mem::replace(&mut self.active_input, ActiveInput::None) {
            ActiveInput::None => false,
            ActiveInput::Organization { text } => self.handle_organization_input_key(key, text),
            ActiveInput::Filter { target, text } => self.handle_filter_input_key(key, target, text),
            ActiveInput::Search { text } => self.handle_search_input_key(key, text),
            ActiveInput::Command { text } => self.handle_command_input_key(key, text),
        }
    }

    fn handle_organization_input_key(&mut self, key: KeyEvent, text: String) -> bool {
        match key.code {
            KeyCode::Esc => false,
            KeyCode::Enter => {
                let trimmed = text.trim();
                self.scope = if trimmed.is_empty() {
                    Scope::global()
                } else {
                    match Uuid::parse_str(trimmed) {
                        Ok(id) => Scope::for_org(id),
                        Err(_) => {
                            self.last_error = Some("Invalid organization UUID".to_owned());
                            return false;
                        }
                    }
                };
                self.invalidate_cache();
                true
            }
            KeyCode::Backspace => {
                let mut text = text;
                text.pop();
                self.active_input = ActiveInput::Organization { text };
                false
            }
            KeyCode::Char(c) => {
                let mut text = text;
                text.push(c);
                self.active_input = ActiveInput::Organization { text };
                false
            }
            _ => {
                self.active_input = ActiveInput::Organization { text };
                false
            }
        }
    }

    fn handle_filter_input_key(
        &mut self,
        key: KeyEvent,
        target: FilterTarget,
        text: String,
    ) -> bool {
        match key.code {
            KeyCode::Esc => false,
            KeyCode::Enter => {
                let trimmed = text.trim();
                let value = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                match target {
                    FilterTarget::Workflow => {
                        self.bump_fetch_generation();
                        self.workflow_type_filter = value;
                        self.workflows = None;
                    }
                    FilterTarget::Job => {
                        self.bump_fetch_generation();
                        self.job_type_filter = value;
                        self.jobs = None;
                        self.definitions = None;
                    }
                }
                true
            }
            KeyCode::Backspace => {
                let mut text = text;
                text.pop();
                self.active_input = ActiveInput::Filter { target, text };
                false
            }
            KeyCode::Char(c) => {
                let mut text = text;
                text.push(c);
                self.active_input = ActiveInput::Filter { target, text };
                false
            }
            _ => {
                self.active_input = ActiveInput::Filter { target, text };
                false
            }
        }
    }

    fn handle_search_input_key(&mut self, key: KeyEvent, text: String) -> bool {
        match key.code {
            KeyCode::Esc => false,
            KeyCode::Enter => {
                let trimmed = text.trim();
                self.table_search = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                };
                self.list_selection = 0;
                self.clamp_selection();
                false
            }
            KeyCode::Backspace => {
                let mut text = text;
                text.pop();
                self.active_input = ActiveInput::Search { text };
                false
            }
            KeyCode::Char(c) => {
                let mut text = text;
                text.push(c);
                self.active_input = ActiveInput::Search { text };
                false
            }
            _ => {
                self.active_input = ActiveInput::Search { text };
                false
            }
        }
    }

    fn handle_command_input_key(&mut self, key: KeyEvent, text: String) -> bool {
        match key.code {
            KeyCode::Esc => false,
            KeyCode::Enter => self.execute_command(text.trim()),
            KeyCode::Backspace => {
                let mut text = text;
                text.pop();
                self.active_input = ActiveInput::Command { text };
                false
            }
            KeyCode::Char(c) => {
                let mut text = text;
                text.push(c);
                self.active_input = ActiveInput::Command { text };
                false
            }
            _ => {
                self.active_input = ActiveInput::Command { text };
                false
            }
        }
    }
}
