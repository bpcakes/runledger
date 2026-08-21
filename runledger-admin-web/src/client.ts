export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { readonly [key: string]: JsonValue };

export type DataVisibility = "metadata_only" | "full";
export type AdminScope =
  | { readonly kind: "all" }
  | { readonly kind: "organization"; readonly organization_id: string };

export interface Capabilities {
  readonly api_version: "v1";
  readonly scope: AdminScope;
  readonly visibility: DataVisibility;
  readonly actions: readonly string[];
  readonly resources: readonly string[];
}

export interface Page {
  readonly limit: number;
  readonly offset: number;
  readonly has_more: boolean;
}

export type HistoryOrder = "newest_first" | "oldest_first";

export interface HistoryPage {
  readonly limit: number;
  readonly cursor: string | null;
  readonly next_cursor: string | null;
  readonly order: HistoryOrder;
  readonly has_more: boolean;
}

export interface JobMetrics {
  readonly job_type: string;
  readonly pending_count: number;
  readonly leased_count: number;
  readonly stale_leases: number;
  readonly succeeded_24h: number;
  readonly retryable_24h: number;
  readonly terminal_24h: number;
  readonly panicked_24h: number;
  readonly timeout_24h: number;
  readonly dead_lettered_24h: number;
  readonly p50_duration_ms_24h: number | null;
  readonly p95_duration_ms_24h: number | null;
  readonly continued_24h: number;
  readonly active_continued_count: number;
  readonly max_active_run_number: number;
}

export interface MetricsResponse {
  readonly items: readonly JobMetrics[];
}

export interface Job {
  readonly id: string;
  readonly job_type: string;
  readonly organization_id: string | null;
  readonly status: string;
  readonly priority: number;
  readonly run_number: number;
  readonly attempt: number;
  readonly max_attempts: number;
  readonly timeout_seconds: number;
  readonly next_run_at: string;
  readonly lease_expires_at: string | null;
  readonly last_heartbeat_at: string | null;
  readonly started_at: string | null;
  readonly finished_at: string | null;
  readonly stage: string;
  readonly progress_done: number | null;
  readonly progress_total: number | null;
  readonly progress_pct: number | null;
  readonly last_error_code: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly payload?: JsonValue;
  readonly checkpoint?: JsonValue;
  readonly output?: JsonValue;
  readonly idempotency_key?: string;
  readonly worker_id?: string;
  readonly status_reason?: string;
  readonly last_error_message?: string;
  readonly redacted_fields: readonly string[];
}

export interface JobsResponse {
  readonly items: readonly Job[];
  readonly page: Page;
}

export interface JobResponse {
  readonly job: Job;
}

export interface JobEvent {
  readonly id: string;
  readonly job_id: string;
  readonly run_number: number;
  readonly attempt: number | null;
  readonly event_type: string;
  readonly stage: string | null;
  readonly progress_done: number | null;
  readonly progress_total: number | null;
  readonly occurred_at: string;
  readonly payload?: JsonValue;
  readonly redacted_fields: readonly string[];
}

export interface JobEventsResponse {
  readonly items: readonly JobEvent[];
  readonly page: HistoryPage;
}

export interface JobLog {
  readonly id: string;
  readonly job_id: string;
  readonly run_number: number;
  readonly attempt: number | null;
  readonly level: string;
  readonly occurred_at: string;
  readonly message?: string;
  readonly payload?: JsonValue;
  readonly redacted_fields: readonly string[];
}

export interface JobLogsResponse {
  readonly items: readonly JobLog[];
  readonly page: HistoryPage;
}

export interface Workflow {
  readonly id: string;
  readonly workflow_type: string;
  readonly organization_id: string | null;
  readonly status: string;
  readonly result_step_key: string | null;
  readonly started_at: string;
  readonly finished_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly idempotency_key?: string;
  readonly metadata?: JsonValue;
  readonly redacted_fields: readonly string[];
}

export interface WorkflowsResponse {
  readonly items: readonly Workflow[];
  readonly page: Page;
}

export interface WorkflowStep {
  readonly id: string;
  readonly workflow_run_id: string;
  readonly step_key: string;
  readonly execution_kind: string;
  readonly job_type: string | null;
  readonly organization_id: string | null;
  readonly priority: number | null;
  readonly max_attempts: number | null;
  readonly timeout_seconds: number | null;
  readonly stage: string | null;
  readonly allow_handler_continuation: boolean;
  readonly status: string;
  readonly job_id: string | null;
  readonly released_at: string | null;
  readonly started_at: string | null;
  readonly finished_at: string | null;
  readonly dependency_count_total: number;
  readonly dependency_count_pending: number;
  readonly dependency_count_unsatisfied: number;
  readonly last_error_code: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly payload?: JsonValue;
  readonly execution_resource_key?: string;
  readonly status_reason?: string;
  readonly last_error_message?: string;
  readonly output?: JsonValue;
  readonly redacted_fields: readonly string[];
}

export interface WorkflowDependency {
  readonly workflow_run_id: string;
  readonly prerequisite_step_id: string;
  readonly dependent_step_id: string;
  readonly release_mode: string;
  readonly created_at: string;
}

export interface WorkflowResponse {
  readonly workflow: Workflow;
  readonly steps: readonly WorkflowStep[];
  readonly steps_page: Page;
  readonly dependencies: readonly WorkflowDependency[];
  readonly dependencies_page: Page;
}

export interface JobDefinition {
  readonly job_type: string;
  readonly version: number;
  readonly max_attempts: number;
  readonly default_timeout_seconds: number;
  readonly default_priority: number;
  readonly is_enabled: boolean;
  readonly created_at: string;
  readonly updated_at: string;
}

export interface DefinitionsResponse {
  readonly items: readonly JobDefinition[];
  readonly page: Page;
}

export interface RequestOptions {
  readonly signal?: AbortSignal;
}
export interface MetricsParams extends RequestOptions {
  readonly jobType?: string;
}
export interface JobsParams extends RequestOptions {
  readonly status?: string;
  readonly jobType?: string;
  readonly limit?: number;
  readonly offset?: number;
}
export interface HistoryParams extends RequestOptions {
  readonly limit?: number;
  readonly cursor?: string;
  readonly order?: HistoryOrder;
}
export interface WorkflowsParams extends RequestOptions {
  readonly status?: string;
  readonly workflowType?: string;
  readonly limit?: number;
  readonly offset?: number;
}
export interface WorkflowParams extends RequestOptions {
  readonly stepLimit?: number;
  readonly stepOffset?: number;
  readonly dependencyLimit?: number;
  readonly dependencyOffset?: number;
}
export interface DefinitionsParams extends RequestOptions {
  readonly jobType?: string;
  readonly limit?: number;
  readonly offset?: number;
}

export interface RunledgerAdminClient {
  capabilities(options?: RequestOptions): Promise<Capabilities>;
  metrics(params?: MetricsParams): Promise<MetricsResponse>;
  jobs(params?: JobsParams): Promise<JobsResponse>;
  job(jobId: string, options?: RequestOptions): Promise<JobResponse>;
  jobEvents(jobId: string, params?: HistoryParams): Promise<JobEventsResponse>;
  jobLogs(jobId: string, params?: HistoryParams): Promise<JobLogsResponse>;
  workflows(params?: WorkflowsParams): Promise<WorkflowsResponse>;
  workflow(workflowId: string, params?: WorkflowParams): Promise<WorkflowResponse>;
  definitions(params?: DefinitionsParams): Promise<DefinitionsResponse>;
}

export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
export interface RunledgerAdminClientOptions {
  readonly baseUrl?: string;
  readonly fetch?: FetchLike;
  readonly credentials?: RequestCredentials;
  readonly headers?:
    | Readonly<Record<string, string>>
    | (() => Readonly<Record<string, string>> | Promise<Readonly<Record<string, string>>>);
}

export class RunledgerAdminHttpError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "RunledgerAdminHttpError";
    this.status = status;
    this.code = code;
  }
}

export class RunledgerAdminContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RunledgerAdminContractError";
  }
}

type Decoder<T> = (value: unknown) => value is T;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isString = (value: unknown): value is string => typeof value === "string";
const isNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);
const isBoolean = (value: unknown): value is boolean => typeof value === "boolean";
const isJsonValue = (value: unknown): value is JsonValue => {
  if (value === null || isBoolean(value) || isNumber(value) || isString(value)) return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  return isRecord(value) && Object.values(value).every(isJsonValue);
};
const isNullable = <T>(value: unknown, decoder: Decoder<T>): value is T | null =>
  value === null || decoder(value);
const isOptional = <T>(value: unknown, decoder: Decoder<T>): value is T | undefined =>
  value === undefined || decoder(value);
const isArrayOf = <T>(value: unknown, decoder: Decoder<T>): value is T[] =>
  Array.isArray(value) && value.every(decoder);
const isStringArray = (value: unknown): value is string[] => isArrayOf(value, isString);

const isPage = (value: unknown): value is Page =>
  isRecord(value) &&
  isNumber(value.limit) &&
  isNumber(value.offset) &&
  isBoolean(value.has_more);
const isHistoryOrder = (value: unknown): value is HistoryOrder =>
  value === "newest_first" || value === "oldest_first";
const isHistoryPage = (value: unknown): value is HistoryPage =>
  isRecord(value) &&
  isNumber(value.limit) &&
  isNullable(value.cursor, isString) &&
  isNullable(value.next_cursor, isString) &&
  isHistoryOrder(value.order) &&
  isBoolean(value.has_more);
const isScope = (value: unknown): value is AdminScope =>
  isRecord(value) &&
  (value.kind === "all" || (value.kind === "organization" && isString(value.organization_id)));
const isCapabilities = (value: unknown): value is Capabilities =>
  isRecord(value) &&
  value.api_version === "v1" &&
  isScope(value.scope) &&
  (value.visibility === "metadata_only" || value.visibility === "full") &&
  isStringArray(value.actions) &&
  isStringArray(value.resources);

const isMetrics = (value: unknown): value is JobMetrics =>
  isRecord(value) &&
  isString(value.job_type) &&
  [
    value.pending_count,
    value.leased_count,
    value.stale_leases,
    value.succeeded_24h,
    value.retryable_24h,
    value.terminal_24h,
    value.panicked_24h,
    value.timeout_24h,
    value.dead_lettered_24h,
    value.continued_24h,
    value.active_continued_count,
    value.max_active_run_number,
  ].every(isNumber) &&
  isNullable(value.p50_duration_ms_24h, isNumber) &&
  isNullable(value.p95_duration_ms_24h, isNumber);

const isJob = (value: unknown): value is Job =>
  isRecord(value) &&
  isString(value.id) &&
  isString(value.job_type) &&
  isNullable(value.organization_id, isString) &&
  isString(value.status) &&
  isString(value.stage) &&
  [
    value.priority,
    value.run_number,
    value.attempt,
    value.max_attempts,
    value.timeout_seconds,
  ].every(isNumber) &&
  isString(value.next_run_at) &&
  isNullable(value.lease_expires_at, isString) &&
  isNullable(value.last_heartbeat_at, isString) &&
  isNullable(value.started_at, isString) &&
  isNullable(value.finished_at, isString) &&
  isNullable(value.progress_done, isNumber) &&
  isNullable(value.progress_total, isNumber) &&
  isNullable(value.progress_pct, isNumber) &&
  isNullable(value.last_error_code, isString) &&
  isString(value.created_at) &&
  isString(value.updated_at) &&
  isOptional(value.payload, isJsonValue) &&
  isOptional(value.checkpoint, isJsonValue) &&
  isOptional(value.output, isJsonValue) &&
  isOptional(value.idempotency_key, isString) &&
  isOptional(value.worker_id, isString) &&
  isOptional(value.status_reason, isString) &&
  isOptional(value.last_error_message, isString) &&
  isStringArray(value.redacted_fields);

const isJobEvent = (value: unknown): value is JobEvent =>
  isRecord(value) &&
  isString(value.id) &&
  isString(value.job_id) &&
  isNumber(value.run_number) &&
  isNullable(value.attempt, isNumber) &&
  isString(value.event_type) &&
  isNullable(value.stage, isString) &&
  isNullable(value.progress_done, isNumber) &&
  isNullable(value.progress_total, isNumber) &&
  isString(value.occurred_at) &&
  isOptional(value.payload, isJsonValue) &&
  isStringArray(value.redacted_fields);

const isJobLog = (value: unknown): value is JobLog =>
  isRecord(value) &&
  isString(value.id) &&
  isString(value.job_id) &&
  isNumber(value.run_number) &&
  isNullable(value.attempt, isNumber) &&
  isString(value.level) &&
  isString(value.occurred_at) &&
  isOptional(value.message, isString) &&
  isOptional(value.payload, isJsonValue) &&
  isStringArray(value.redacted_fields);

const isWorkflow = (value: unknown): value is Workflow =>
  isRecord(value) &&
  isString(value.id) &&
  isString(value.workflow_type) &&
  isNullable(value.organization_id, isString) &&
  isString(value.status) &&
  isNullable(value.result_step_key, isString) &&
  isString(value.started_at) &&
  isNullable(value.finished_at, isString) &&
  isString(value.created_at) &&
  isString(value.updated_at) &&
  isOptional(value.idempotency_key, isString) &&
  isOptional(value.metadata, isJsonValue) &&
  isStringArray(value.redacted_fields);

const isWorkflowStep = (value: unknown): value is WorkflowStep =>
  isRecord(value) &&
  isString(value.id) &&
  isString(value.workflow_run_id) &&
  isString(value.step_key) &&
  isString(value.execution_kind) &&
  isNullable(value.job_type, isString) &&
  isNullable(value.organization_id, isString) &&
  isNullable(value.priority, isNumber) &&
  isNullable(value.max_attempts, isNumber) &&
  isNullable(value.timeout_seconds, isNumber) &&
  isNullable(value.stage, isString) &&
  isBoolean(value.allow_handler_continuation) &&
  isString(value.status) &&
  isNullable(value.job_id, isString) &&
  isNullable(value.released_at, isString) &&
  isNullable(value.started_at, isString) &&
  isNullable(value.finished_at, isString) &&
  isNumber(value.dependency_count_total) &&
  isNumber(value.dependency_count_pending) &&
  isNumber(value.dependency_count_unsatisfied) &&
  isNullable(value.last_error_code, isString) &&
  isString(value.created_at) &&
  isString(value.updated_at) &&
  isOptional(value.payload, isJsonValue) &&
  isOptional(value.execution_resource_key, isString) &&
  isOptional(value.status_reason, isString) &&
  isOptional(value.last_error_message, isString) &&
  isOptional(value.output, isJsonValue) &&
  isStringArray(value.redacted_fields);

const isWorkflowDependency = (value: unknown): value is WorkflowDependency =>
  isRecord(value) &&
  isString(value.workflow_run_id) &&
  isString(value.prerequisite_step_id) &&
  isString(value.dependent_step_id) &&
  isString(value.release_mode) &&
  isString(value.created_at);

const isDefinition = (value: unknown): value is JobDefinition =>
  isRecord(value) &&
  isString(value.job_type) &&
  isNumber(value.version) &&
  isNumber(value.max_attempts) &&
  isNumber(value.default_timeout_seconds) &&
  isNumber(value.default_priority) &&
  isBoolean(value.is_enabled) &&
  isString(value.created_at) &&
  isString(value.updated_at);

const listDecoder = <T>(itemDecoder: Decoder<T>): Decoder<{ readonly items: readonly T[] }> =>
  (value: unknown): value is { readonly items: readonly T[] } =>
    isRecord(value) && isArrayOf(value.items, itemDecoder);
const pagedDecoder = <T>(
  itemDecoder: Decoder<T>,
): Decoder<{ readonly items: readonly T[]; readonly page: Page }> =>
  (value: unknown): value is { readonly items: readonly T[]; readonly page: Page } =>
    isRecord(value) && isArrayOf(value.items, itemDecoder) && isPage(value.page);
const historyDecoder = <T>(
  itemDecoder: Decoder<T>,
): Decoder<{ readonly items: readonly T[]; readonly page: HistoryPage }> =>
  (value: unknown): value is { readonly items: readonly T[]; readonly page: HistoryPage } =>
    isRecord(value) && isArrayOf(value.items, itemDecoder) && isHistoryPage(value.page);
const isJobResponse = (value: unknown): value is JobResponse =>
  isRecord(value) && isJob(value.job);
const isWorkflowResponse = (value: unknown): value is WorkflowResponse =>
  isRecord(value) &&
  isWorkflow(value.workflow) &&
  isArrayOf(value.steps, isWorkflowStep) &&
  isPage(value.steps_page) &&
  isArrayOf(value.dependencies, isWorkflowDependency) &&
  isPage(value.dependencies_page);

function decode<T>(value: unknown, decoder: Decoder<T>, resource: string): T {
  if (!decoder(value)) {
    throw new RunledgerAdminContractError(`Invalid Runledger ${resource} response.`);
  }
  return value;
}

function queryString(values: Readonly<Record<string, string | number | undefined>>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) search.set(key, String(value));
  }
  const encoded = search.toString();
  return encoded === "" ? "" : `?${encoded}`;
}

export function createRunledgerAdminClient(
  options: RunledgerAdminClientOptions = {},
): RunledgerAdminClient {
  const baseUrl = (options.baseUrl ?? "/api/admin/runledger/v1").replace(/\/$/, "");
  const fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);

  async function request<T>(
    path: string,
    decoder: Decoder<T>,
    resource: string,
    signal?: AbortSignal,
  ): Promise<T> {
    const configuredHeaders =
      typeof options.headers === "function" ? await options.headers() : options.headers;
    const response = await fetcher(`${baseUrl}${path}`, {
      credentials: options.credentials ?? "same-origin",
      headers: { Accept: "application/json", ...configuredHeaders },
      method: "GET",
      ...(signal === undefined ? {} : { signal }),
    });
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      if (!response.ok) {
        throw new RunledgerAdminHttpError(
          response.status,
          "admin.http_error",
          `Runledger request failed with HTTP ${response.status}.`,
        );
      }
      throw new RunledgerAdminContractError("Runledger returned a non-JSON response.");
    }
    if (!response.ok) {
      const detail = isRecord(payload) && isRecord(payload.error) ? payload.error : undefined;
      const code = detail !== undefined && isString(detail.code) ? detail.code : "admin.http_error";
      const message =
        detail !== undefined && isString(detail.message)
          ? detail.message
          : `Runledger request failed with HTTP ${response.status}.`;
      throw new RunledgerAdminHttpError(response.status, code, message);
    }
    return decode(payload, decoder, resource);
  }

  return {
    capabilities: (requestOptions) =>
      request("/capabilities", isCapabilities, "capabilities", requestOptions?.signal),
    metrics: (params = {}) =>
      request(
        `/metrics${queryString({ job_type: params.jobType })}`,
        listDecoder(isMetrics),
        "metrics",
        params.signal,
      ),
    jobs: (params = {}) =>
      request(
        `/jobs${queryString({
          job_type: params.jobType,
          limit: params.limit,
          offset: params.offset,
          status: params.status,
        })}`,
        pagedDecoder(isJob),
        "jobs",
        params.signal,
      ),
    job: (jobId, requestOptions) =>
      request(`/jobs/${encodeURIComponent(jobId)}`, isJobResponse, "job", requestOptions?.signal),
    jobEvents: (jobId, params = {}) =>
      request(
        `/jobs/${encodeURIComponent(jobId)}/events${queryString({
          cursor: params.cursor,
          limit: params.limit,
          order: params.order,
        })}`,
        historyDecoder(isJobEvent),
        "job events",
        params.signal,
      ),
    jobLogs: (jobId, params = {}) =>
      request(
        `/jobs/${encodeURIComponent(jobId)}/logs${queryString({
          cursor: params.cursor,
          limit: params.limit,
          order: params.order,
        })}`,
        historyDecoder(isJobLog),
        "job logs",
        params.signal,
      ),
    workflows: (params = {}) =>
      request(
        `/workflows${queryString({
          limit: params.limit,
          offset: params.offset,
          status: params.status,
          workflow_type: params.workflowType,
        })}`,
        pagedDecoder(isWorkflow),
        "workflows",
        params.signal,
      ),
    workflow: (workflowId, params = {}) =>
      request(
        `/workflows/${encodeURIComponent(workflowId)}${queryString({
          dependency_limit: params.dependencyLimit,
          dependency_offset: params.dependencyOffset,
          step_limit: params.stepLimit,
          step_offset: params.stepOffset,
        })}`,
        isWorkflowResponse,
        "workflow",
        params.signal,
      ),
    definitions: (params = {}) =>
      request(
        `/definitions${queryString({
          job_type: params.jobType,
          limit: params.limit,
          offset: params.offset,
        })}`,
        pagedDecoder(isDefinition),
        "definitions",
        params.signal,
      ),
  };
}
