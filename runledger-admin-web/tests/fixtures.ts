import type {
  Capabilities,
  DefinitionsResponse,
  Job,
  JobEventsResponse,
  JobLogsResponse,
  JobResponse,
  JobsResponse,
  MetricsResponse,
  WorkflowResponse,
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

export const metrics: MetricsResponse = {
  items: [
    {
      active_continued_count: 0,
      continued_24h: 0,
      dead_lettered_24h: 2,
      job_type: job.job_type,
      leased_count: 0,
      max_active_run_number: 0,
      p50_duration_ms_24h: null,
      p95_duration_ms_24h: null,
      panicked_24h: 0,
      pending_count: 1,
      retryable_24h: 0,
      stale_leases: 0,
      succeeded_24h: 12,
      terminal_24h: 3,
      timeout_24h: 0,
    },
  ],
};

export const jobs: JobsResponse = {
  items: [job],
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

const workflow = {
  created_at: timestamp,
  finished_at: null,
  id: workflowId,
  organization_id: job.organization_id,
  redacted_fields: ["metadata", "idempotency_key"],
  result_step_key: null,
  started_at: timestamp,
  status: "RUNNING",
  updated_at: timestamp,
  workflow_type: "workflow.customer.import",
} as const;

export const workflows: WorkflowsResponse = {
  items: [workflow],
  page: { has_more: false, limit: 50, max_offset: 10_000, offset: 0 },
};
export const workflowResponse: WorkflowResponse = {
  dependencies: [],
  dependencies_page: {
    has_more: false,
    limit: 50,
    max_offset: 10_000,
    offset: 0,
  },
  steps: [
    {
      allow_handler_continuation: false,
      created_at: timestamp,
      dependency_count_pending: 0,
      dependency_count_total: 0,
      dependency_count_unsatisfied: 0,
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
  steps_page: {
    has_more: false,
    limit: 50,
    max_offset: 10_000,
    offset: 0,
  },
  workflow,
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
