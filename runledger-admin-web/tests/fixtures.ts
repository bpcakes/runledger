import type {
  Capabilities,
  DefinitionsResponse,
  Job,
  JobSummary,
  JobEventsResponse,
  JobLogsResponse,
  JobResponse,
  JobsResponse,
  MetricsResponse,
  WorkflowDependenciesResponse,
  WorkflowResponse,
  WorkflowStepsResponse,
  WorkflowSummary,
  WorkflowsResponse,
} from "../src/client.js";

export const jobId = "0198bb4e-5566-7000-8000-000000000001";
export const workflowId = "0198bb4e-5566-7000-8000-000000000002";
const timestamp = "2026-08-21T07:00:00Z";

export const capabilities: Capabilities = {
  actions: [],
  api_version: "v1",
  resources: [
    "metrics",
    "jobs",
    "job_events",
    "job_logs",
    "workflows",
    "definitions",
  ],
  scope: {
    kind: "organization",
    organization_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  },
  visibility: "metadata_only",
};

export const job: Job = {
  attempt: 0,
  created_at: timestamp,
  finished_at: null,
  id: jobId,
  job_type: "jobs.customer.import",
  last_error_code: null,
  last_heartbeat_at: null,
  lease_expires_at: null,
  max_attempts: 3,
  next_run_at: timestamp,
  organization_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  priority: 100,
  progress_done: null,
  progress_pct: null,
  progress_total: null,
  redacted_fields: ["payload", "idempotency_key"],
  run_number: 1,
  stage: "queued",
  started_at: null,
  status: "PENDING",
  timeout_seconds: 60,
  updated_at: timestamp,
};

export const jobSummary: JobSummary = {
  attempt: job.attempt,
  created_at: job.created_at,
  finished_at: job.finished_at,
  id: job.id,
  job_type: job.job_type,
  last_error_code: job.last_error_code,
  last_heartbeat_at: job.last_heartbeat_at,
  lease_expires_at: job.lease_expires_at,
  max_attempts: job.max_attempts,
  next_run_at: job.next_run_at,
  organization_id: job.organization_id,
  priority: job.priority,
  progress_done: job.progress_done,
  progress_pct: job.progress_pct,
  progress_total: job.progress_total,
  run_number: job.run_number,
  stage: job.stage,
  started_at: job.started_at,
  status: job.status,
  timeout_seconds: job.timeout_seconds,
  updated_at: job.updated_at,
};

export const metrics: MetricsResponse = {
  items: [
    {
      active_continued_count: "0",
      continued_24h: "0",
      dead_lettered_24h: "2",
      job_type: job.job_type,
      leased_count: "0",
      max_active_run_number: 0,
      p50_duration_ms_24h: null,
      p95_duration_ms_24h: null,
      panicked_24h: "0",
      pending_count: "1",
      retryable_24h: "0",
      stale_leases: "0",
      succeeded_24h: "12",
      terminal_24h: "3",
      timeout_24h: "0",
    },
  ],
  page: { has_more: false, limit: 50, max_offset: 10_000, offset: 0 },
};

export const jobs: JobsResponse = {
  items: [jobSummary],
  page: { has_more: false, limit: 50, max_offset: 10_000, offset: 0 },
};
export const jobResponse: JobResponse = { job };
export const events: JobEventsResponse = {
  items: [
    {
      attempt: null,
      event_type: "ENQUEUED",
      id: "1",
      job_id: jobId,
      occurred_at: timestamp,
      progress_done: null,
      progress_total: null,
      redacted_fields: ["payload"],
      run_number: 1,
      stage: "queued",
    },
  ],
  page: {
    cursor: null,
    has_more: false,
    limit: 50,
    next_cursor: "1",
    order: "newest_first",
  },
};
export const logs: JobLogsResponse = {
  items: [
    {
      attempt: null,
      id: "1",
      job_id: jobId,
      level: "info",
      occurred_at: timestamp,
      redacted_fields: ["message", "payload"],
      run_number: 1,
    },
  ],
  page: {
    cursor: null,
    has_more: false,
    limit: 50,
    next_cursor: "1",
    order: "newest_first",
  },
};

const workflowSummary: WorkflowSummary = {
  created_at: timestamp,
  finished_at: null,
  id: workflowId,
  organization_id: job.organization_id,
  result_step_key: null,
  started_at: timestamp,
  status: "RUNNING",
  updated_at: timestamp,
  workflow_type: "workflow.customer.import",
};

const workflow = {
  ...workflowSummary,
  redacted_fields: ["metadata", "idempotency_key"],
} as const;

export const workflows: WorkflowsResponse = {
  items: [workflowSummary],
  page: { has_more: false, limit: 50, max_offset: 10_000, offset: 0 },
};
export const workflowResponse: WorkflowResponse = {
  workflow,
};
export const workflowDependenciesResponse: WorkflowDependenciesResponse = {
  items: [],
  page: {
    has_more: false,
    limit: 50,
    max_offset: 10_000,
    offset: 0,
  },
};
export const workflowStepsResponse: WorkflowStepsResponse = {
  items: [
    {
      allow_handler_continuation: false,
      created_at: timestamp,
      has_hidden_prerequisites: true,
      visible_dependency_count_pending: 0,
      visible_dependency_count_total: 0,
      visible_dependency_count_unsatisfied: 0,
      execution_kind: "JOB",
      finished_at: null,
      id: "0198bb4e-5566-7000-8000-000000000003",
      job_id: jobId,
      job_type: job.job_type,
      last_error_code: null,
      max_attempts: 3,
      organization_id: job.organization_id,
      priority: 100,
      redacted_fields: ["payload"],
      released_at: timestamp,
      stage: "queued",
      started_at: null,
      status: "ENQUEUED",
      step_key: "import",
      timeout_seconds: 60,
      updated_at: timestamp,
      workflow_run_id: workflowId,
    },
  ],
  page: {
    has_more: false,
    limit: 50,
    max_offset: 10_000,
    offset: 0,
  },
};

export const definitions: DefinitionsResponse = {
  items: [
    {
      created_at: timestamp,
      default_priority: 100,
      default_timeout_seconds: 60,
      is_enabled: true,
      job_type: job.job_type,
      max_attempts: 3,
      updated_at: timestamp,
      version: 1,
    },
  ],
  page: { has_more: false, limit: 50, max_offset: 10_000, offset: 0 },
};
