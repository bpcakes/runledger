import createClient from "openapi-fetch";

import type { components, paths } from "./generated/schema.js";

type Schemas = components["schemas"];

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { readonly [key: string]: JsonValue };

export type DataVisibility = Schemas["DataVisibility"];
export type AdminScope = Schemas["AdminScope"];
export type Capabilities = Schemas["Capabilities"];
export type Page = Schemas["Page"];
export type HistoryOrder = Schemas["HistoryOrder"];
export type HistoryPage = Schemas["HistoryPage"];
export type JobMetrics = Schemas["JobMetrics"];
export type MetricsResponse = Schemas["MetricsResponse"];

type JsonFields<T, Keys extends keyof T> = Omit<T, Keys> & {
  readonly [Key in Keys]?: JsonValue;
};

export type Job = JsonFields<
  Schemas["Job"],
  "checkpoint" | "output" | "payload"
>;
export type JobsResponse = Omit<Schemas["JobsResponse"], "items"> & {
  readonly items: readonly Job[];
};
export type JobResponse = { readonly job: Job };

export type JobEvent = JsonFields<Schemas["JobEvent"], "payload">;
export type JobEventsResponse = Omit<Schemas["JobEventsResponse"], "items"> & {
  readonly items: readonly JobEvent[];
};

export type JobLog = JsonFields<Schemas["JobLog"], "payload">;
export type JobLogsResponse = Omit<Schemas["JobLogsResponse"], "items"> & {
  readonly items: readonly JobLog[];
};

export type Workflow = JsonFields<Schemas["Workflow"], "metadata">;
export type WorkflowsResponse = Omit<Schemas["WorkflowsResponse"], "items"> & {
  readonly items: readonly Workflow[];
};

export type WorkflowStep = JsonFields<
  Schemas["WorkflowStep"],
  "output" | "payload"
>;
export type WorkflowDependency = Schemas["WorkflowDependency"];
export type WorkflowResponse = Omit<
  Schemas["WorkflowResponse"],
  "steps" | "workflow"
> & {
  readonly steps: readonly WorkflowStep[];
  readonly workflow: Workflow;
};

export type JobDefinition = Schemas["JobDefinition"];
export type DefinitionsResponse = Schemas["DefinitionsResponse"];

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
  workflow(
    workflowId: string,
    params?: WorkflowParams,
  ): Promise<WorkflowResponse>;
  definitions(params?: DefinitionsParams): Promise<DefinitionsResponse>;
}

export type FetchLike = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;
export interface RunledgerAdminClientOptions {
  readonly baseUrl?: string;
  readonly fetch?: FetchLike;
  readonly credentials?: RequestCredentials;
  readonly headers?:
    | Readonly<Record<string, string>>
    | (() =>
        | Readonly<Record<string, string>>
        | Promise<Readonly<Record<string, string>>>);
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

interface InternalResult {
  readonly data?: unknown;
  readonly error?: unknown;
  readonly response: Response;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function requestInit(request: Request): RequestInit {
  return {
    cache: request.cache,
    credentials: request.credentials,
    headers: request.headers,
    integrity: request.integrity,
    keepalive: request.keepalive,
    method: request.method,
    mode: request.mode,
    redirect: request.redirect,
    referrer: request.referrer,
    referrerPolicy: request.referrerPolicy,
    signal: request.signal,
  };
}

function clientBaseUrl(baseUrl: string): {
  readonly url: string;
  readonly relative: boolean;
} {
  try {
    return { relative: false, url: new URL(baseUrl).toString() };
  } catch {
    const origin = globalThis.location?.origin ?? "http://runledger.invalid";
    const path = baseUrl.startsWith("/") ? baseUrl : `/${baseUrl}`;
    return { relative: true, url: new URL(path, origin).toString() };
  }
}

function externalUrl(url: string, relative: boolean): string {
  if (!relative) return url;
  const parsed = new URL(url);
  return `${parsed.pathname}${parsed.search}${parsed.hash}`;
}

function signalOption(
  signal: AbortSignal | undefined,
): { readonly signal?: never } | { readonly signal: AbortSignal } {
  return signal === undefined ? {} : { signal };
}

function httpError(
  response: Response,
  payload: unknown,
): RunledgerAdminHttpError {
  const detail =
    isRecord(payload) && isRecord(payload.error) ? payload.error : undefined;
  const code =
    typeof detail?.code === "string" ? detail.code : "admin.http_error";
  const message =
    typeof detail?.message === "string"
      ? detail.message
      : `Runledger request failed with HTTP ${response.status}.`;
  return new RunledgerAdminHttpError(response.status, code, message);
}

async function unwrap<T>(
  operation: Promise<InternalResult>,
  resource: string,
): Promise<T> {
  let result: InternalResult;
  try {
    result = await operation;
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new RunledgerAdminContractError(
        `Runledger returned a non-JSON ${resource} response.`,
      );
    }
    throw error;
  }

  if (!result.response.ok) throw httpError(result.response, result.error);
  if (result.data === undefined) {
    throw new RunledgerAdminContractError(
      `Runledger returned an empty ${resource} response.`,
    );
  }

  // openapi-fetch checks request construction and expected response shapes
  // against the generated type at compile time. It does not validate response
  // values at runtime. JSON payload fields are narrowed from `unknown` to
  // JsonValue here because every value produced by JSON.parse satisfies that
  // union.
  return result.data as T;
}

export function createRunledgerAdminClient(
  options: RunledgerAdminClientOptions = {},
): RunledgerAdminClient {
  const configuredBaseUrl = (
    options.baseUrl ?? "/api/admin/runledger/v1"
  ).replace(/\/$/, "");
  const baseUrl = clientBaseUrl(configuredBaseUrl);
  const fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  const api = createClient<paths>({
    baseUrl: baseUrl.url,
    credentials: options.credentials ?? "same-origin",
    fetch: (request) =>
      fetcher(externalUrl(request.url, baseUrl.relative), requestInit(request)),
    headers: { Accept: "application/json" },
  });

  api.use({
    async onRequest({ request }) {
      const configuredHeaders =
        typeof options.headers === "function"
          ? await options.headers()
          : options.headers;
      if (configuredHeaders === undefined) return;

      const headers = new Headers(request.headers);
      for (const [name, value] of Object.entries(configuredHeaders)) {
        headers.set(name, value);
      }
      return new Request(request, { headers });
    },
  });

  return {
    capabilities: (requestOptions) =>
      unwrap<Capabilities>(
        api.GET("/capabilities", {
          ...signalOption(requestOptions?.signal),
        }),
        "capabilities",
      ),
    metrics: (params = {}) =>
      unwrap<MetricsResponse>(
        api.GET("/metrics", {
          params: {
            query: {
              ...(params.jobType === undefined
                ? {}
                : { job_type: params.jobType }),
            },
          },
          ...signalOption(params.signal),
        }),
        "metrics",
      ),
    jobs: (params = {}) =>
      unwrap<JobsResponse>(
        api.GET("/jobs", {
          params: {
            query: {
              ...(params.jobType === undefined
                ? {}
                : { job_type: params.jobType }),
              ...(params.limit === undefined ? {} : { limit: params.limit }),
              ...(params.offset === undefined ? {} : { offset: params.offset }),
              ...(params.status === undefined ? {} : { status: params.status }),
            },
          },
          ...signalOption(params.signal),
        }),
        "jobs",
      ),
    job: (jobId, requestOptions) =>
      unwrap<JobResponse>(
        api.GET("/jobs/{job_id}", {
          params: { path: { job_id: jobId } },
          ...signalOption(requestOptions?.signal),
        }),
        "job",
      ),
    jobEvents: (jobId, params = {}) =>
      unwrap<JobEventsResponse>(
        api.GET("/jobs/{job_id}/events", {
          params: {
            path: { job_id: jobId },
            query: {
              ...(params.cursor === undefined ? {} : { cursor: params.cursor }),
              ...(params.limit === undefined ? {} : { limit: params.limit }),
              ...(params.order === undefined ? {} : { order: params.order }),
            },
          },
          ...signalOption(params.signal),
        }),
        "job events",
      ),
    jobLogs: (jobId, params = {}) =>
      unwrap<JobLogsResponse>(
        api.GET("/jobs/{job_id}/logs", {
          params: {
            path: { job_id: jobId },
            query: {
              ...(params.cursor === undefined ? {} : { cursor: params.cursor }),
              ...(params.limit === undefined ? {} : { limit: params.limit }),
              ...(params.order === undefined ? {} : { order: params.order }),
            },
          },
          ...signalOption(params.signal),
        }),
        "job logs",
      ),
    workflows: (params = {}) =>
      unwrap<WorkflowsResponse>(
        api.GET("/workflows", {
          params: {
            query: {
              ...(params.limit === undefined ? {} : { limit: params.limit }),
              ...(params.offset === undefined ? {} : { offset: params.offset }),
              ...(params.status === undefined ? {} : { status: params.status }),
              ...(params.workflowType === undefined
                ? {}
                : { workflow_type: params.workflowType }),
            },
          },
          ...signalOption(params.signal),
        }),
        "workflows",
      ),
    workflow: (workflowId, params = {}) =>
      unwrap<WorkflowResponse>(
        api.GET("/workflows/{workflow_id}", {
          params: {
            path: { workflow_id: workflowId },
            query: {
              ...(params.dependencyLimit === undefined
                ? {}
                : { dependency_limit: params.dependencyLimit }),
              ...(params.dependencyOffset === undefined
                ? {}
                : { dependency_offset: params.dependencyOffset }),
              ...(params.stepLimit === undefined
                ? {}
                : { step_limit: params.stepLimit }),
              ...(params.stepOffset === undefined
                ? {}
                : { step_offset: params.stepOffset }),
            },
          },
          ...signalOption(params.signal),
        }),
        "workflow",
      ),
    definitions: (params = {}) =>
      unwrap<DefinitionsResponse>(
        api.GET("/definitions", {
          params: {
            query: {
              ...(params.jobType === undefined
                ? {}
                : { job_type: params.jobType }),
              ...(params.limit === undefined ? {} : { limit: params.limit }),
              ...(params.offset === undefined ? {} : { offset: params.offset }),
            },
          },
          ...signalOption(params.signal),
        }),
        "definitions",
      ),
  };
}
