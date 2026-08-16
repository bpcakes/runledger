use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStatus, WorkflowRunStatus, WorkflowStepStatus};
use serde_json::Value;
use uuid::Uuid;

/// Maximum payload lines rendered/scrolled in job detail.
pub const JOB_PAYLOAD_MAX_LINES: usize = 200;

#[must_use]
pub fn short_uuid(id: Uuid) -> String {
    let s = id.to_string();
    if s.len() <= 13 {
        return s;
    }
    format!("{}…{}", &s[..8], &s[s.len() - 4..])
}

#[must_use]
pub fn job_status_label(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "PEND",
        JobStatus::Leased => "LEASE",
        JobStatus::Succeeded => "OK",
        JobStatus::DeadLettered => "DLQ",
        JobStatus::Canceled => "CANC",
    }
}

#[must_use]
pub fn workflow_run_status_label(status: WorkflowRunStatus) -> &'static str {
    match status {
        WorkflowRunStatus::Running => "RUN",
        WorkflowRunStatus::WaitingForExternal => "EXT",
        WorkflowRunStatus::Succeeded => "OK",
        WorkflowRunStatus::CompletedWithErrors => "ERR",
        WorkflowRunStatus::Canceled => "CANC",
    }
}

#[must_use]
pub fn workflow_step_status_label(status: WorkflowStepStatus) -> &'static str {
    match status {
        WorkflowStepStatus::Blocked => "BLK",
        WorkflowStepStatus::WaitingForExternal => "EXT",
        WorkflowStepStatus::Enqueued => "ENQ",
        WorkflowStepStatus::Running => "RUN",
        WorkflowStepStatus::Succeeded => "OK",
        WorkflowStepStatus::Failed => "FAIL",
        WorkflowStepStatus::Canceled => "CANC",
    }
}

#[must_use]
pub fn format_timestamp(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

#[must_use]
pub fn format_optional_timestamp(ts: Option<DateTime<Utc>>) -> String {
    ts.map(format_timestamp).unwrap_or_else(|| "—".to_owned())
}

#[must_use]
pub fn format_relative_timestamp(ts: DateTime<Utc>) -> String {
    let delta = Utc::now().signed_duration_since(ts);
    let seconds = delta.num_seconds().max(0);
    if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

#[must_use]
pub fn truncate_str(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut end = max_chars;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

/// Pretty-printed payload lines capped for terminal display.
#[must_use]
pub fn job_payload_lines(payload: &Value) -> Vec<String> {
    let pretty = serde_json::to_string_pretty(payload).unwrap_or_else(|_| "{}".to_owned());
    let raw: Vec<String> = pretty.lines().map(ToOwned::to_owned).collect();
    truncate_lines(&raw, JOB_PAYLOAD_MAX_LINES).0
}

#[must_use]
pub fn job_payload_raw_lines(payload: &Value) -> Vec<String> {
    let raw = serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_owned());
    truncate_lines(&[raw], JOB_PAYLOAD_MAX_LINES).0
}

#[must_use]
pub fn job_payload_scroll_max(line_count: usize, visible_rows: usize) -> usize {
    line_count.saturating_sub(visible_rows.max(1))
}

#[must_use]
pub fn truncate_lines(lines: &[String], max_lines: usize) -> (Vec<String>, bool) {
    if lines.len() <= max_lines {
        return (lines.to_vec(), false);
    }
    let mut out = lines[..max_lines].to_vec();
    out.push(format!("… ({} more lines)", lines.len() - max_lines));
    (out, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_uuid_truncates_middle() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
            .expect("fixed UUID fixture should parse");
        let s = short_uuid(id);
        assert!(s.contains('…'));
        assert!(s.starts_with("550e8400"));
    }

    #[test]
    fn job_status_labels_are_stable() {
        assert_eq!(job_status_label(JobStatus::Pending), "PEND");
        assert_eq!(job_status_label(JobStatus::DeadLettered), "DLQ");
    }

    #[test]
    fn truncate_str_respects_char_boundaries() {
        let s = "hello";
        assert_eq!(truncate_str(s, 10), "hello");
        assert_eq!(truncate_str("hello world", 5), "hello…");
    }

    #[test]
    fn job_payload_scroll_max_caps_to_line_count() {
        assert_eq!(job_payload_scroll_max(0, 10), 0);
        assert_eq!(job_payload_scroll_max(5, 10), 0);
        assert_eq!(job_payload_scroll_max(10, 4), 6);
        assert_eq!(job_payload_scroll_max(5, 0), 4);
    }

    #[test]
    fn job_payload_lines_respects_max() {
        let payload = serde_json::json!({"a": 0});
        let lines = job_payload_lines(&payload);
        assert!(!lines.is_empty());
        assert!(lines.len() <= JOB_PAYLOAD_MAX_LINES);
    }

    #[test]
    fn job_payload_raw_lines_uses_compact_json() {
        let payload = serde_json::json!({"a": 0, "b": [1, 2]});
        let lines = job_payload_raw_lines(&payload);
        assert_eq!(lines.len(), 1);
        assert!(!lines[0].contains('\n'));
    }

    #[test]
    fn truncate_lines_adds_footer() {
        let lines: Vec<String> = (0..5).map(|i| i.to_string()).collect();
        let (out, truncated) = truncate_lines(&lines, 3);
        assert!(truncated);
        assert_eq!(out.len(), 4);
        assert!(out[3].contains("more lines"));
    }
}
