use crossterm::event::{KeyCode, KeyEvent};

use super::super::{ActiveInput, App, FilterTarget, JobDetailPane, Screen, TopScreen};

impl App {
    /// Returns true when a data refresh should be scheduled.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !matches!(self.active_input(), ActiveInput::None) {
            return self.handle_active_input_key(key);
        }
        if self.help_blocks_key(&key) {
            return false;
        }
        self.handle_idle_key(key)
    }

    fn help_blocks_key(&self, key: &KeyEvent) -> bool {
        self.show_help && key.code != KeyCode::Char('?') && key.code != KeyCode::Esc
    }

    /// Dispatch order is part of the keymap contract: global bindings take
    /// precedence over navigation bindings, which take precedence over
    /// screen-specific bindings. Guarded bindings intentionally fall through
    /// to the next stage when their screen or pane is inactive.
    fn handle_idle_key(&mut self, key: KeyEvent) -> bool {
        if let Some(refresh) = self.handle_global_key(&key) {
            return refresh;
        }
        if let Some(refresh) = self.handle_navigation_key(&key) {
            return refresh;
        }
        self.handle_screen_key(&key)
    }

    fn handle_global_key(&mut self, key: &KeyEvent) -> Option<bool> {
        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                Some(false)
            }
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
                Some(false)
            }
            KeyCode::Esc => Some(self.handle_escape()),
            KeyCode::Char('r') | KeyCode::Char('.') => Some(true),
            KeyCode::Char('p') => {
                self.toggle_refresh_paused();
                Some(false)
            }
            KeyCode::Char('o') => {
                self.begin_organization_input();
                Some(false)
            }
            KeyCode::Char('/') => {
                self.begin_search_input();
                Some(false)
            }
            KeyCode::Char('t') => {
                self.begin_type_filter_input();
                Some(false)
            }
            KeyCode::Char('w') if matches!(self.current_frame.screen, Screen::Workflows) => {
                self.begin_workflow_filter_input();
                Some(false)
            }
            KeyCode::Char(':') => {
                self.active_input = ActiveInput::Command {
                    text: String::new(),
                };
                Some(false)
            }
            KeyCode::Char('c') => Some(self.clear_context_filters()),
            KeyCode::Char('y') => {
                self.copy_selected_identifier();
                Some(false)
            }
            _ => None,
        }
    }

    fn handle_escape(&mut self) -> bool {
        if self.show_help {
            self.show_help = false;
            false
        } else if !self.screen_stack.is_empty() {
            self.pop_screen();
            true
        } else {
            false
        }
    }

    fn toggle_refresh_paused(&mut self) {
        self.refresh_paused = !self.refresh_paused;
        self.notice = Some(if self.refresh_paused {
            "Auto-refresh paused".to_owned()
        } else {
            "Auto-refresh resumed".to_owned()
        });
    }

    fn begin_organization_input(&mut self) {
        self.active_input = ActiveInput::Organization {
            text: self
                .scope
                .organization_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
        };
    }

    fn begin_search_input(&mut self) {
        self.active_input = ActiveInput::Search {
            text: self.table_search.clone().unwrap_or_default(),
        };
    }

    fn begin_type_filter_input(&mut self) {
        let target = if matches!(
            self.current_frame.screen,
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

    fn begin_workflow_filter_input(&mut self) {
        self.active_input = ActiveInput::Filter {
            target: FilterTarget::Workflow,
            text: self.workflow_type_filter.clone().unwrap_or_default(),
        };
    }

    fn handle_navigation_key(&mut self, key: &KeyEvent) -> Option<bool> {
        match key.code {
            KeyCode::Char('f') if matches!(self.current_frame.screen, Screen::Queue) => {
                Some(self.transition_queue_status_filter(self.queue_filter.next()))
            }
            KeyCode::Char('1') => {
                self.navigate_top(TopScreen::Dashboard);
                Some(true)
            }
            KeyCode::Char('2') => {
                self.navigate_top(TopScreen::Queue);
                Some(true)
            }
            KeyCode::Char('3') => {
                self.navigate_top(TopScreen::Workflows);
                Some(true)
            }
            KeyCode::Char('4') => {
                self.navigate_top(TopScreen::Definitions);
                Some(true)
            }
            KeyCode::Tab => {
                let next =
                    top_screen_from_index((self.top_screen_index() + 1) % Self::TOP_SCREEN_COUNT);
                self.navigate_top(next);
                Some(true)
            }
            KeyCode::BackTab => {
                let next = top_screen_from_index(
                    (self.top_screen_index() + Self::TOP_SCREEN_COUNT - 1) % Self::TOP_SCREEN_COUNT,
                );
                self.navigate_top(next);
                Some(true)
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection(1);
                Some(false)
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection(-1);
                Some(false)
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.move_to_start();
                Some(false)
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.move_to_end();
                Some(false)
            }
            KeyCode::PageDown => {
                self.move_selection(10);
                Some(false)
            }
            KeyCode::PageUp => {
                self.move_selection(-10);
                Some(false)
            }
            KeyCode::Char('h') => Some(self.pop_screen_if_stacked()),
            _ => None,
        }
    }

    fn pop_screen_if_stacked(&mut self) -> bool {
        if self.screen_stack.is_empty() {
            return false;
        }
        self.pop_screen();
        true
    }

    fn handle_screen_key(&mut self, key: &KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('v') if self.job_detail_payload_pane_active() => {
                self.toggle_payload_wrap();
                false
            }
            KeyCode::Char('R') if self.job_detail_payload_pane_active() => {
                self.toggle_payload_raw();
                false
            }
            KeyCode::Char(']') | KeyCode::Right
                if matches!(self.current_frame.screen, Screen::JobDetail { .. }) =>
            {
                self.cycle_job_detail_pane(true);
                false
            }
            KeyCode::Char('[') | KeyCode::Left
                if matches!(self.current_frame.screen, Screen::JobDetail { .. }) =>
            {
                self.cycle_job_detail_pane(false);
                false
            }
            KeyCode::Char('l') | KeyCode::Enter => self.activate_selection_and_refresh(),
            _ => false,
        }
    }

    fn job_detail_payload_pane_active(&self) -> bool {
        matches!(
            (
                &self.current_frame.screen,
                self.current_frame.state.job_detail_pane
            ),
            (Screen::JobDetail { .. }, JobDetailPane::Payload)
        )
    }

    fn toggle_payload_wrap(&mut self) {
        self.payload_wrap = !self.payload_wrap;
        self.notice = Some(if self.payload_wrap {
            "Payload wrap enabled".to_owned()
        } else {
            "Payload wrap disabled".to_owned()
        });
    }

    fn toggle_payload_raw(&mut self) {
        self.payload_raw = !self.payload_raw;
        self.current_frame.state.job_detail_viewport.scroll = 0;
        self.notice = Some(if self.payload_raw {
            "Payload raw mode".to_owned()
        } else {
            "Payload pretty mode".to_owned()
        });
    }

    fn cycle_job_detail_pane(&mut self, next: bool) {
        self.current_frame.state.job_detail_pane = if next {
            self.current_frame.state.job_detail_pane.next()
        } else {
            self.current_frame.state.job_detail_pane.prev()
        };
        self.current_frame.state.job_detail_viewport.scroll = 0;
        self.current_frame.state.list_selection = 0;
    }

    fn activate_selection_and_refresh(&mut self) -> bool {
        let before = self.current_frame.screen.clone();
        self.activate_selection();
        self.current_frame.screen != before
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::config::Config;
    use crate::data::QueueStatusFilter;

    fn test_app() -> App {
        App::new(Config {
            database_url: "postgres://example/runledger".to_owned(),
            org: None,
            refresh_ms: 2_000,
            limit: 100,
            skip_schema_check: false,
        })
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
    }

    enum ExpectedAction {
        WorkflowFilter,
        QueueFilter,
        PayloadWrap,
        PayloadRaw,
        Pane(JobDetailPane),
    }

    struct GuardedKeyCase {
        name: &'static str,
        code: KeyCode,
        screen: Screen,
        pane: JobDetailPane,
        refresh: bool,
        action: ExpectedAction,
    }

    #[test]
    fn guarded_idle_keys_apply_in_their_matching_context() {
        let job_id = Uuid::nil();
        let cases = [
            GuardedKeyCase {
                name: "workflow filter",
                code: KeyCode::Char('w'),
                screen: Screen::Workflows,
                pane: JobDetailPane::Summary,
                refresh: false,
                action: ExpectedAction::WorkflowFilter,
            },
            GuardedKeyCase {
                name: "queue filter",
                code: KeyCode::Char('f'),
                screen: Screen::Queue,
                pane: JobDetailPane::Summary,
                refresh: true,
                action: ExpectedAction::QueueFilter,
            },
            GuardedKeyCase {
                name: "payload wrap",
                code: KeyCode::Char('v'),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Payload,
                refresh: false,
                action: ExpectedAction::PayloadWrap,
            },
            GuardedKeyCase {
                name: "payload raw",
                code: KeyCode::Char('R'),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Payload,
                refresh: false,
                action: ExpectedAction::PayloadRaw,
            },
            GuardedKeyCase {
                name: "next pane bracket",
                code: KeyCode::Char(']'),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Summary,
                refresh: false,
                action: ExpectedAction::Pane(JobDetailPane::Events),
            },
            GuardedKeyCase {
                name: "next pane arrow",
                code: KeyCode::Right,
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Summary,
                refresh: false,
                action: ExpectedAction::Pane(JobDetailPane::Events),
            },
            GuardedKeyCase {
                name: "previous pane bracket",
                code: KeyCode::Char('['),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Summary,
                refresh: false,
                action: ExpectedAction::Pane(JobDetailPane::Payload),
            },
            GuardedKeyCase {
                name: "previous pane arrow",
                code: KeyCode::Left,
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Summary,
                refresh: false,
                action: ExpectedAction::Pane(JobDetailPane::Payload),
            },
        ];

        for case in cases {
            let mut app = test_app();
            app.current_frame.screen = case.screen;
            app.current_frame.state.job_detail_pane = case.pane;
            app.current_frame.state.job_detail_viewport.scroll = 5;

            assert_eq!(
                app.handle_key(key(case.code)),
                case.refresh,
                "{} refresh result",
                case.name
            );

            match case.action {
                ExpectedAction::WorkflowFilter => assert_eq!(
                    app.active_input(),
                    &ActiveInput::Filter {
                        target: FilterTarget::Workflow,
                        text: String::new(),
                    },
                    "{}",
                    case.name
                ),
                ExpectedAction::QueueFilter => {
                    assert_eq!(
                        app.queue_filter,
                        QueueStatusFilter::Pending,
                        "{}",
                        case.name
                    );
                }
                ExpectedAction::PayloadWrap => assert!(app.payload_wrap, "{}", case.name),
                ExpectedAction::PayloadRaw => {
                    assert!(app.payload_raw, "{}", case.name);
                    assert_eq!(
                        app.current_frame.state.job_detail_viewport.scroll, 0,
                        "{}",
                        case.name
                    );
                }
                ExpectedAction::Pane(pane) => {
                    assert_eq!(
                        app.current_frame.state.job_detail_pane, pane,
                        "{}",
                        case.name
                    );
                    assert_eq!(
                        app.current_frame.state.job_detail_viewport.scroll, 0,
                        "{}",
                        case.name
                    );
                }
            }
        }
    }

    struct InactiveGuardCase {
        name: &'static str,
        code: KeyCode,
        screen: Screen,
        pane: JobDetailPane,
    }

    #[test]
    fn guarded_idle_keys_fall_through_without_effect_outside_their_context() {
        let job_id = Uuid::nil();
        let cases = [
            InactiveGuardCase {
                name: "workflow filter outside workflows",
                code: KeyCode::Char('w'),
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "queue filter outside queue",
                code: KeyCode::Char('f'),
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "payload wrap outside payload pane",
                code: KeyCode::Char('v'),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "payload raw outside payload pane",
                code: KeyCode::Char('R'),
                screen: Screen::JobDetail { job_id },
                pane: JobDetailPane::Events,
            },
            InactiveGuardCase {
                name: "next pane bracket outside job detail",
                code: KeyCode::Char(']'),
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "next pane arrow outside job detail",
                code: KeyCode::Right,
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "previous pane bracket outside job detail",
                code: KeyCode::Char('['),
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "previous pane arrow outside job detail",
                code: KeyCode::Left,
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
            InactiveGuardCase {
                name: "back navigation with an empty stack",
                code: KeyCode::Char('h'),
                screen: Screen::Dashboard,
                pane: JobDetailPane::Summary,
            },
        ];

        for case in cases {
            let mut app = test_app();
            app.current_frame.screen = case.screen;
            app.current_frame.state.job_detail_pane = case.pane;
            app.current_frame.state.job_detail_viewport.scroll = 5;
            let frame_before = app.current_frame.clone();

            assert!(!app.handle_key(key(case.code)), "{}", case.name);
            assert_eq!(app.current_frame, frame_before, "{}", case.name);
            assert_eq!(app.active_input(), &ActiveInput::None, "{}", case.name);
            assert_eq!(app.queue_filter, QueueStatusFilter::All, "{}", case.name);
            assert!(!app.payload_wrap, "{}", case.name);
            assert!(!app.payload_raw, "{}", case.name);
        }
    }
}
