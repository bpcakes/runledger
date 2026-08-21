import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useState,
} from "react";

import type {
  Capabilities,
  DefinitionsResponse,
  Job,
  JobEventsResponse,
  JobLogsResponse,
  JobResponse,
  JobsResponse,
  JsonValue,
  MetricsResponse,
  Page,
  RunledgerAdminClient,
  WorkflowResponse,
  WorkflowsResponse,
} from "./client.js";

export type RunledgerAdminRoute =
  | { readonly name: "overview" }
  | {
      readonly name: "jobs";
      readonly status?: string;
      readonly jobType?: string;
      readonly offset?: number;
    }
  | { readonly name: "job"; readonly jobId: string }
  | {
      readonly name: "workflows";
      readonly status?: string;
      readonly workflowType?: string;
      readonly offset?: number;
    }
  | { readonly name: "workflow"; readonly workflowId: string }
  | {
      readonly name: "definitions";
      readonly jobType?: string;
      readonly offset?: number;
    };

export interface RunledgerAdminPanelProps {
  readonly client: RunledgerAdminClient;
  readonly route: RunledgerAdminRoute;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly className?: string;
  /** Operational data polling interval. Set to zero to disable. Defaults to ten seconds. */
  readonly pollIntervalMs?: number;
  /** Capability polling interval. Set to zero to load once. Defaults to zero. */
  readonly capabilitiesPollIntervalMs?: number;
  readonly title?: string;
}

type ResourceState<T> =
  | { readonly status: "loading" }
  | { readonly status: "ready"; readonly data: T }
  | { readonly status: "error"; readonly error: Error };

function useResource<T>(
  loader: (signal: AbortSignal) => Promise<T>,
  pollIntervalMs: number,
): readonly [ResourceState<T>, () => void] {
  const [version, setVersion] = useState(0);
  const [state, setState] = useState<ResourceState<T>>({ status: "loading" });

  useEffect(() => {
    const controller = new AbortController();
    let timeout: number | undefined;
    const load = async () => {
      try {
        const data = await loader(controller.signal);
        if (!controller.signal.aborted) setState({ status: "ready", data });
      } catch (reason: unknown) {
        if (controller.signal.aborted) return;
        setState({
          status: "error",
          error:
            reason instanceof Error
              ? reason
              : new Error("Runledger request failed."),
        });
      } finally {
        if (!controller.signal.aborted && pollIntervalMs > 0) {
          timeout = window.setTimeout(load, pollIntervalMs);
        }
      }
    };
    setState({ status: "loading" });
    void load();
    return () => {
      controller.abort();
      if (timeout !== undefined) window.clearTimeout(timeout);
    };
  }, [loader, pollIntervalMs, version]);

  return [state, () => setVersion((current) => current + 1)] as const;
}

function Resource<T>({
  state,
  retry,
  children,
}: {
  readonly state: ResourceState<T>;
  readonly retry: () => void;
  readonly children: (data: T) => ReactNode;
}) {
  if (state.status === "loading") {
    return <p role="status">Loading Runledger data…</p>;
  }
  if (state.status === "error") {
    return (
      <div className="rla-alert" role="alert">
        <p>{state.error.message}</p>
        <button className="rla-button" onClick={retry} type="button">
          Try again
        </button>
      </div>
    );
  }
  return children(state.data);
}

function formatDate(value: string | null): string {
  if (value === null) return "—";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString();
}

function Status({ value }: { readonly value: string }) {
  return (
    <span className={`rla-status rla-status--${value.toLowerCase()}`}>
      {value}
    </span>
  );
}

function RedactionNotice({ fields }: { readonly fields: readonly string[] }) {
  if (fields.length === 0) return null;
  return (
    <p className="rla-redaction" role="note">
      Sensitive fields hidden: {fields.join(", ")}.
    </p>
  );
}

function JsonBlock({
  label,
  value,
}: {
  readonly label: string;
  readonly value: JsonValue | undefined;
}) {
  if (value === undefined) return null;
  return (
    <details className="rla-json">
      <summary>{label}</summary>
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}

function BackButton({
  onClick,
  label,
}: {
  readonly onClick: () => void;
  readonly label: string;
}) {
  return (
    <button className="rla-link-button" onClick={onClick} type="button">
      ← {label}
    </button>
  );
}

function scopeLabel(capabilities: Capabilities): string {
  return capabilities.scope.kind === "all"
    ? "All organizations"
    : `Organization ${capabilities.scope.organization_id}`;
}

function OverviewScreen({
  client,
  capabilities,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly capabilities: Capabilities;
  readonly pollIntervalMs: number;
}) {
  const loader = useCallback(
    (signal: AbortSignal) => client.metrics({ signal }),
    [client],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  return (
    <section aria-labelledby="rla-overview-heading">
      <h2 id="rla-overview-heading">Overview</h2>
      <Resource retry={retry} state={state}>
        {(metrics) => (
          <>
            <dl className="rla-summary">
              <div>
                <dt>Access scope</dt>
                <dd>{scopeLabel(capabilities)}</dd>
              </div>
              <div>
                <dt>Data visibility</dt>
                <dd>
                  {capabilities.visibility === "full"
                    ? "Full"
                    : "Metadata only"}
                </dd>
              </div>
              <div>
                <dt>API version</dt>
                <dd>{capabilities.api_version}</dd>
              </div>
            </dl>
            <MetricsTable metrics={metrics} />
          </>
        )}
      </Resource>
    </section>
  );
}

function MetricsTable({ metrics }: { readonly metrics: MetricsResponse }) {
  if (metrics.items.length === 0) return <p>No registered job metrics.</p>;
  return (
    <div className="rla-table-wrap">
      <table>
        <caption>Current job health</caption>
        <thead>
          <tr>
            <th scope="col">Job type</th>
            <th scope="col">Pending</th>
            <th scope="col">Leased</th>
            <th scope="col">Stale</th>
            <th scope="col">Succeeded 24h</th>
            <th scope="col">Dead-lettered 24h</th>
          </tr>
        </thead>
        <tbody>
          {metrics.items.map((metric) => (
            <tr key={metric.job_type}>
              <th scope="row">{metric.job_type}</th>
              <td>{metric.pending_count}</td>
              <td>{metric.leased_count}</td>
              <td>{metric.stale_leases}</td>
              <td>{metric.succeeded_24h}</td>
              <td>{metric.dead_lettered_24h}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JobsScreen({
  client,
  route,
  onRouteChange,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly route: Extract<RunledgerAdminRoute, { name: "jobs" }>;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
}) {
  const offset = route.offset ?? 0;
  const loader = useCallback(
    (signal: AbortSignal) =>
      client.jobs({
        ...(route.status === undefined ? {} : { status: route.status }),
        ...(route.jobType === undefined ? {} : { jobType: route.jobType }),
        limit: 50,
        offset,
        signal,
      }),
    [client, offset, route.jobType, route.status],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const status = String(data.get("status") ?? "");
    const jobType = String(data.get("job_type") ?? "").trim();
    onRouteChange({
      name: "jobs",
      ...(status === "" ? {} : { status }),
      ...(jobType === "" ? {} : { jobType }),
    });
  };
  return (
    <section aria-labelledby="rla-jobs-heading">
      <h2 id="rla-jobs-heading">Jobs</h2>
      <form
        className="rla-filters"
        key={`${route.status}:${route.jobType}`}
        onSubmit={submit}
      >
        <label>
          Status
          <select defaultValue={route.status ?? ""} name="status">
            <option value="">Any</option>
            {[
              "PENDING",
              "LEASED",
              "SUCCEEDED",
              "DEAD_LETTERED",
              "CANCELED",
            ].map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
          </select>
        </label>
        <label>
          Job type contains
          <input
            defaultValue={route.jobType ?? ""}
            name="job_type"
            type="search"
          />
        </label>
        <button className="rla-button" type="submit">
          Apply filters
        </button>
      </form>
      <Resource retry={retry} state={state}>
        {(jobs) => (
          <>
            <JobsTable
              jobs={jobs}
              onOpen={(jobId) => onRouteChange({ name: "job", jobId })}
            />
            <Pagination
              itemCount={jobs.items.length}
              label="Job list pagination"
              onOffset={(nextOffset) =>
                onRouteChange(jobRouteAtOffset(route, nextOffset))
              }
              page={jobs.page}
            />
          </>
        )}
      </Resource>
    </section>
  );
}

function jobRouteAtOffset(
  route: Extract<RunledgerAdminRoute, { name: "jobs" }>,
  offset: number,
): Extract<RunledgerAdminRoute, { name: "jobs" }> {
  const { offset: _currentOffset, ...withoutOffset } = route;
  return offset === 0 ? withoutOffset : { ...withoutOffset, offset };
}

function JobsTable({
  jobs,
  onOpen,
}: {
  readonly jobs: JobsResponse;
  readonly onOpen: (id: string) => void;
}) {
  if (jobs.items.length === 0) return <p>No jobs match these filters.</p>;
  return (
    <div className="rla-table-wrap">
      <table>
        <caption>Durable jobs</caption>
        <thead>
          <tr>
            <th scope="col">Job</th>
            <th scope="col">Type</th>
            <th scope="col">Status</th>
            <th scope="col">Progress</th>
            <th scope="col">Updated</th>
          </tr>
        </thead>
        <tbody>
          {jobs.items.map((job) => (
            <tr key={job.id}>
              <th scope="row">
                <button
                  className="rla-link-button"
                  onClick={() => onOpen(job.id)}
                  type="button"
                >
                  {job.id}
                </button>
              </th>
              <td>{job.job_type}</td>
              <td>
                <Status value={job.status} />
              </td>
              <td>
                {job.progress_done === null
                  ? "—"
                  : `${job.progress_done}/${job.progress_total ?? "?"}`}
              </td>
              <td>{formatDate(job.updated_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Pagination({
  itemCount,
  label,
  page,
  onOffset,
}: {
  readonly itemCount: number;
  readonly label: string;
  readonly page: Page;
  readonly onOffset: (offset: number) => void;
}) {
  const firstRow = itemCount === 0 ? 0 : page.offset + 1;
  const lastRow = page.offset + itemCount;
  return (
    <nav aria-label={label} className="rla-pagination">
      <button
        className="rla-button"
        disabled={page.offset === 0}
        onClick={() => onOffset(Math.max(0, page.offset - page.limit))}
        type="button"
      >
        Previous
      </button>
      <span>
        Rows {firstRow}–{lastRow}
      </span>
      <button
        className="rla-button"
        disabled={!page.has_more}
        onClick={() => onOffset(page.offset + page.limit)}
        type="button"
      >
        Next
      </button>
    </nav>
  );
}

interface JobDetailData {
  readonly job: JobResponse;
  readonly events: JobEventsResponse;
  readonly logs: JobLogsResponse;
}

function JobScreen({
  client,
  jobId,
  onRouteChange,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly jobId: string;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
}) {
  const [eventCursors, setEventCursors] = useState<readonly string[]>([]);
  const [logCursors, setLogCursors] = useState<readonly string[]>([]);
  const eventCursor = eventCursors.at(-1);
  const logCursor = logCursors.at(-1);
  const loader = useCallback(
    async (signal: AbortSignal): Promise<JobDetailData> => {
      const [job, events, logs] = await Promise.all([
        client.job(jobId, { signal }),
        client.jobEvents(jobId, {
          ...(eventCursor === undefined ? {} : { cursor: eventCursor }),
          signal,
        }),
        client.jobLogs(jobId, {
          ...(logCursor === undefined ? {} : { cursor: logCursor }),
          signal,
        }),
      ]);
      return { job, events, logs };
    },
    [client, eventCursor, jobId, logCursor],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  return (
    <section aria-labelledby="rla-job-heading">
      <BackButton
        label="Jobs"
        onClick={() => onRouteChange({ name: "jobs" })}
      />
      <h2 id="rla-job-heading">Job {jobId}</h2>
      <Resource retry={retry} state={state}>
        {(data) => (
          <JobDetail
            data={data}
            eventPage={eventCursors.length + 1}
            logPage={logCursors.length + 1}
            onNextEvents={(cursor) =>
              setEventCursors((current) => [...current, cursor])
            }
            onNextLogs={(cursor) =>
              setLogCursors((current) => [...current, cursor])
            }
            onPreviousEvents={() =>
              setEventCursors((current) => current.slice(0, -1))
            }
            onPreviousLogs={() =>
              setLogCursors((current) => current.slice(0, -1))
            }
          />
        )}
      </Resource>
    </section>
  );
}

function JobDetail({
  data,
  eventPage,
  logPage,
  onNextEvents,
  onNextLogs,
  onPreviousEvents,
  onPreviousLogs,
}: {
  readonly data: JobDetailData;
  readonly eventPage: number;
  readonly logPage: number;
  readonly onNextEvents: (cursor: string) => void;
  readonly onNextLogs: (cursor: string) => void;
  readonly onPreviousEvents: () => void;
  readonly onPreviousLogs: () => void;
}) {
  const job = data.job.job;
  return (
    <>
      <RedactionNotice fields={job.redacted_fields} />
      <dl className="rla-details">
        <div>
          <dt>Type</dt>
          <dd>{job.job_type}</dd>
        </div>
        <div>
          <dt>Status</dt>
          <dd>
            <Status value={job.status} />
          </dd>
        </div>
        <div>
          <dt>Organization</dt>
          <dd>{job.organization_id ?? "Global"}</dd>
        </div>
        <div>
          <dt>Run / attempt</dt>
          <dd>
            {job.run_number} / {job.attempt}
          </dd>
        </div>
        <div>
          <dt>Stage</dt>
          <dd>{job.stage}</dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>{formatDate(job.updated_at)}</dd>
        </div>
      </dl>
      <JsonBlock label="Payload" value={job.payload} />
      <JsonBlock label="Checkpoint" value={job.checkpoint} />
      <JsonBlock label="Output" value={job.output} />
      <h3>Events</h3>
      {data.events.items.length === 0 ? (
        <p>No events.</p>
      ) : (
        <ol className="rla-timeline">
          {data.events.items.map((event) => (
            <li key={event.id}>
              <Status value={event.event_type} />{" "}
              <time dateTime={event.occurred_at}>
                {formatDate(event.occurred_at)}
              </time>
              <RedactionNotice fields={event.redacted_fields} />
              <JsonBlock label="Event payload" value={event.payload} />
            </li>
          ))}
        </ol>
      )}
      <HistoryPagination
        label="Event history pagination"
        onNext={onNextEvents}
        onPrevious={onPreviousEvents}
        page={eventPage}
        response={data.events.page}
      />
      <h3>Logs</h3>
      {data.logs.items.length === 0 ? (
        <p>No application logs.</p>
      ) : (
        <ol className="rla-timeline">
          {data.logs.items.map((log) => (
            <li key={log.id}>
              <strong>{log.level}</strong> {log.message ?? "Message hidden"}{" "}
              <time dateTime={log.occurred_at}>
                {formatDate(log.occurred_at)}
              </time>
              <RedactionNotice fields={log.redacted_fields} />
              <JsonBlock label="Log payload" value={log.payload} />
            </li>
          ))}
        </ol>
      )}
      <HistoryPagination
        label="Log history pagination"
        onNext={onNextLogs}
        onPrevious={onPreviousLogs}
        page={logPage}
        response={data.logs.page}
      />
      {job.organization_id !== null ? null : (
        <p className="rla-muted">This is a global job.</p>
      )}
    </>
  );
}

function HistoryPagination({
  label,
  onNext,
  onPrevious,
  page,
  response,
}: {
  readonly label: string;
  readonly onNext: (cursor: string) => void;
  readonly onPrevious: () => void;
  readonly page: number;
  readonly response: JobEventsResponse["page"];
}) {
  const nextCursor = response.has_more ? response.next_cursor : null;
  return (
    <nav aria-label={label} className="rla-pagination">
      <button
        className="rla-button"
        disabled={page === 1}
        onClick={onPrevious}
        type="button"
      >
        Newer
      </button>
      <span>Page {page} · newest first</span>
      <button
        className="rla-button"
        disabled={nextCursor === null}
        onClick={() => {
          if (nextCursor !== null) onNext(nextCursor);
        }}
        type="button"
      >
        Older
      </button>
    </nav>
  );
}

function WorkflowsScreen({
  client,
  route,
  onRouteChange,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly route: Extract<RunledgerAdminRoute, { name: "workflows" }>;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
}) {
  const offset = route.offset ?? 0;
  const loader = useCallback(
    (signal: AbortSignal) =>
      client.workflows({
        ...(route.status === undefined ? {} : { status: route.status }),
        ...(route.workflowType === undefined
          ? {}
          : { workflowType: route.workflowType }),
        limit: 50,
        offset,
        signal,
      }),
    [client, offset, route.status, route.workflowType],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const status = String(data.get("status") ?? "");
    const workflowType = String(data.get("workflow_type") ?? "").trim();
    onRouteChange({
      name: "workflows",
      ...(status === "" ? {} : { status }),
      ...(workflowType === "" ? {} : { workflowType }),
    });
  };
  return (
    <section aria-labelledby="rla-workflows-heading">
      <h2 id="rla-workflows-heading">Workflows</h2>
      <form
        className="rla-filters"
        key={`${route.status}:${route.workflowType}`}
        onSubmit={submit}
      >
        <label>
          Status
          <select defaultValue={route.status ?? ""} name="status">
            <option value="">Any</option>
            {[
              "RUNNING",
              "WAITING_FOR_EXTERNAL",
              "SUCCEEDED",
              "COMPLETED_WITH_ERRORS",
              "CANCELED",
            ].map((status) => (
              <option key={status}>{status}</option>
            ))}
          </select>
        </label>
        <label>
          Workflow type contains
          <input
            defaultValue={route.workflowType ?? ""}
            name="workflow_type"
            type="search"
          />
        </label>
        <button className="rla-button" type="submit">
          Apply filters
        </button>
      </form>
      <Resource retry={retry} state={state}>
        {(workflows) => (
          <>
            <WorkflowsTable
              onOpen={(workflowId) =>
                onRouteChange({ name: "workflow", workflowId })
              }
              workflows={workflows}
            />
            <Pagination
              itemCount={workflows.items.length}
              label="Workflow list pagination"
              page={workflows.page}
              onOffset={(nextOffset) =>
                onRouteChange(workflowRouteAtOffset(route, nextOffset))
              }
            />
          </>
        )}
      </Resource>
    </section>
  );
}

function workflowRouteAtOffset(
  route: Extract<RunledgerAdminRoute, { name: "workflows" }>,
  offset: number,
): Extract<RunledgerAdminRoute, { name: "workflows" }> {
  const { offset: _currentOffset, ...withoutOffset } = route;
  return offset === 0 ? withoutOffset : { ...withoutOffset, offset };
}

function WorkflowsTable({
  workflows,
  onOpen,
}: {
  readonly workflows: WorkflowsResponse;
  readonly onOpen: (id: string) => void;
}) {
  if (workflows.items.length === 0)
    return <p>No workflows match these filters.</p>;
  return (
    <div className="rla-table-wrap">
      <table>
        <caption>Workflow runs</caption>
        <thead>
          <tr>
            <th scope="col">Workflow</th>
            <th scope="col">Type</th>
            <th scope="col">Status</th>
            <th scope="col">Started</th>
          </tr>
        </thead>
        <tbody>
          {workflows.items.map((workflow) => (
            <tr key={workflow.id}>
              <th scope="row">
                <button
                  className="rla-link-button"
                  onClick={() => onOpen(workflow.id)}
                  type="button"
                >
                  {workflow.id}
                </button>
              </th>
              <td>{workflow.workflow_type}</td>
              <td>
                <Status value={workflow.status} />
              </td>
              <td>{formatDate(workflow.started_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function WorkflowScreen({
  client,
  workflowId,
  onRouteChange,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly workflowId: string;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
}) {
  const [stepOffset, setStepOffset] = useState(0);
  const [dependencyOffset, setDependencyOffset] = useState(0);
  const loader = useCallback(
    (signal: AbortSignal) =>
      client.workflow(workflowId, {
        dependencyLimit: 50,
        dependencyOffset,
        signal,
        stepLimit: 50,
        stepOffset,
      }),
    [client, dependencyOffset, stepOffset, workflowId],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  return (
    <section aria-labelledby="rla-workflow-heading">
      <BackButton
        label="Workflows"
        onClick={() => onRouteChange({ name: "workflows" })}
      />
      <h2 id="rla-workflow-heading">Workflow {workflowId}</h2>
      <Resource retry={retry} state={state}>
        {(data) => (
          <WorkflowDetail
            data={data}
            onDependencyOffset={setDependencyOffset}
            onRouteChange={onRouteChange}
            onStepOffset={setStepOffset}
          />
        )}
      </Resource>
    </section>
  );
}

function WorkflowDetail({
  data,
  onDependencyOffset,
  onRouteChange,
  onStepOffset,
}: {
  readonly data: WorkflowResponse;
  readonly onDependencyOffset: (offset: number) => void;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly onStepOffset: (offset: number) => void;
}) {
  const workflow = data.workflow;
  return (
    <>
      <RedactionNotice fields={workflow.redacted_fields} />
      <dl className="rla-details">
        <div>
          <dt>Type</dt>
          <dd>{workflow.workflow_type}</dd>
        </div>
        <div>
          <dt>Status</dt>
          <dd>
            <Status value={workflow.status} />
          </dd>
        </div>
        <div>
          <dt>Organization</dt>
          <dd>{workflow.organization_id ?? "Global"}</dd>
        </div>
        <div>
          <dt>Started</dt>
          <dd>{formatDate(workflow.started_at)}</dd>
        </div>
      </dl>
      <JsonBlock label="Metadata" value={workflow.metadata} />
      <h3>Steps</h3>
      {data.steps.length === 0 ? (
        <p>No workflow steps on this page.</p>
      ) : (
        <div className="rla-table-wrap">
          <table>
            <caption>Workflow steps</caption>
            <thead>
              <tr>
                <th scope="col">Step</th>
                <th scope="col">Kind</th>
                <th scope="col">Status</th>
                <th scope="col">Job</th>
              </tr>
            </thead>
            <tbody>
              {data.steps.map((step) => (
                <tr key={step.id}>
                  <th scope="row">{step.step_key}</th>
                  <td>{step.execution_kind}</td>
                  <td>
                    <Status value={step.status} />
                  </td>
                  <td>
                    {step.job_id === null ? (
                      "—"
                    ) : (
                      <button
                        className="rla-link-button"
                        onClick={() =>
                          onRouteChange({
                            name: "job",
                            jobId: step.job_id ?? "",
                          })
                        }
                        type="button"
                      >
                        {step.job_id}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <Pagination
        itemCount={data.steps.length}
        label="Workflow step pagination"
        onOffset={onStepOffset}
        page={data.steps_page}
      />
      <h3>Dependencies</h3>
      {data.dependencies.length === 0 ? (
        <p>No step dependencies on this page.</p>
      ) : (
        <ul>
          {data.dependencies.map((dependency) => (
            <li
              key={`${dependency.prerequisite_step_id}:${dependency.dependent_step_id}`}
            >
              {dependency.prerequisite_step_id} → {dependency.dependent_step_id}{" "}
              ({dependency.release_mode})
            </li>
          ))}
        </ul>
      )}
      <Pagination
        itemCount={data.dependencies.length}
        label="Workflow dependency pagination"
        onOffset={onDependencyOffset}
        page={data.dependencies_page}
      />
    </>
  );
}

function DefinitionsScreen({
  client,
  route,
  onRouteChange,
  pollIntervalMs,
}: {
  readonly client: RunledgerAdminClient;
  readonly route: Extract<RunledgerAdminRoute, { name: "definitions" }>;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
}) {
  const offset = route.offset ?? 0;
  const loader = useCallback(
    (signal: AbortSignal) =>
      client.definitions({
        ...(route.jobType === undefined ? {} : { jobType: route.jobType }),
        limit: 50,
        offset,
        signal,
      }),
    [client, offset, route.jobType],
  );
  const [state, retry] = useResource(loader, pollIntervalMs);
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const jobType = String(
      new FormData(event.currentTarget).get("job_type") ?? "",
    ).trim();
    onRouteChange({
      name: "definitions",
      ...(jobType === "" ? {} : { jobType }),
    });
  };
  return (
    <section aria-labelledby="rla-definitions-heading">
      <h2 id="rla-definitions-heading">Definitions</h2>
      <form className="rla-filters" key={route.jobType} onSubmit={submit}>
        <label>
          Job type contains
          <input
            defaultValue={route.jobType ?? ""}
            name="job_type"
            type="search"
          />
        </label>
        <button className="rla-button" type="submit">
          Apply filter
        </button>
      </form>
      <Resource retry={retry} state={state}>
        {(definitions) => (
          <>
            <DefinitionsTable definitions={definitions} />
            <Pagination
              itemCount={definitions.items.length}
              label="Definition list pagination"
              page={definitions.page}
              onOffset={(nextOffset) =>
                onRouteChange(definitionRouteAtOffset(route, nextOffset))
              }
            />
          </>
        )}
      </Resource>
    </section>
  );
}

function definitionRouteAtOffset(
  route: Extract<RunledgerAdminRoute, { name: "definitions" }>,
  offset: number,
): Extract<RunledgerAdminRoute, { name: "definitions" }> {
  const { offset: _currentOffset, ...withoutOffset } = route;
  return offset === 0 ? withoutOffset : { ...withoutOffset, offset };
}

function DefinitionsTable({
  definitions,
}: {
  readonly definitions: DefinitionsResponse;
}) {
  if (definitions.items.length === 0)
    return <p>No job definitions match this filter.</p>;
  return (
    <div className="rla-table-wrap">
      <table>
        <caption>Registered job definitions</caption>
        <thead>
          <tr>
            <th scope="col">Job type</th>
            <th scope="col">Version</th>
            <th scope="col">Attempts</th>
            <th scope="col">Timeout</th>
            <th scope="col">Enabled</th>
          </tr>
        </thead>
        <tbody>
          {definitions.items.map((definition) => (
            <tr key={definition.job_type}>
              <th scope="row">{definition.job_type}</th>
              <td>{definition.version}</td>
              <td>{definition.max_attempts}</td>
              <td>{definition.default_timeout_seconds}s</td>
              <td>{definition.is_enabled ? "Yes" : "No"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function navName(
  route: RunledgerAdminRoute,
): "overview" | "jobs" | "workflows" | "definitions" {
  if (route.name === "job") return "jobs";
  if (route.name === "workflow") return "workflows";
  return route.name;
}

function PanelBody({
  capabilities,
  client,
  onRouteChange,
  pollIntervalMs,
  route,
}: {
  readonly capabilities: Capabilities;
  readonly client: RunledgerAdminClient;
  readonly onRouteChange: (route: RunledgerAdminRoute) => void;
  readonly pollIntervalMs: number;
  readonly route: RunledgerAdminRoute;
}) {
  const active = navName(route);
  const hasDefinitions = capabilities.resources.includes("definitions");
  const content = (() => {
    switch (route.name) {
      case "overview":
        return (
          <OverviewScreen
            capabilities={capabilities}
            client={client}
            pollIntervalMs={pollIntervalMs}
          />
        );
      case "jobs":
        return (
          <JobsScreen
            client={client}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
            route={route}
          />
        );
      case "job":
        return (
          <JobScreen
            client={client}
            jobId={route.jobId}
            key={route.jobId}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
          />
        );
      case "workflows":
        return (
          <WorkflowsScreen
            client={client}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
            route={route}
          />
        );
      case "workflow":
        return (
          <WorkflowScreen
            client={client}
            key={route.workflowId}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
            workflowId={route.workflowId}
          />
        );
      case "definitions":
        return hasDefinitions ? (
          <DefinitionsScreen
            client={client}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
            route={route}
          />
        ) : (
          <section aria-labelledby="rla-unavailable-heading">
            <h2 id="rla-unavailable-heading">Definitions unavailable</h2>
            <p>
              This admin access does not include the service-wide definition
              catalog.
            </p>
          </section>
        );
    }
  })();
  const navigation = (
    [
      ["overview", "Overview"],
      ["jobs", "Jobs"],
      ["workflows", "Workflows"],
      ["definitions", "Definitions"],
    ] as const
  ).filter(([name]) => name !== "definitions" || hasDefinitions);
  return (
    <>
      <nav aria-label="Runledger sections" className="rla-nav">
        {navigation.map(([name, label]) => (
          <button
            aria-current={active === name ? "page" : undefined}
            className="rla-nav-button"
            key={name}
            onClick={() => onRouteChange({ name })}
            type="button"
          >
            {label}
          </button>
        ))}
      </nav>
      <div className="rla-content">{content}</div>
    </>
  );
}

export function RunledgerAdminPanel({
  client,
  route,
  onRouteChange,
  className,
  pollIntervalMs = 10_000,
  capabilitiesPollIntervalMs = 0,
  title = "Runledger",
}: RunledgerAdminPanelProps) {
  const loader = useCallback(
    (signal: AbortSignal) => client.capabilities({ signal }),
    [client],
  );
  const [state, retry] = useResource(loader, capabilitiesPollIntervalMs);
  return (
    <div className={["rla-panel", className].filter(Boolean).join(" ")}>
      <header className="rla-header">
        <h1>{title}</h1>
        <p>Read-only durable work operations</p>
      </header>
      <Resource retry={retry} state={state}>
        {(capabilities) => (
          <PanelBody
            capabilities={capabilities}
            client={client}
            onRouteChange={onRouteChange}
            pollIntervalMs={pollIntervalMs}
            route={route}
          />
        )}
      </Resource>
    </div>
  );
}
