import { useState } from "react";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { RunledgerAdminClient } from "../src/client.js";
import { RunledgerAdminPanel, type RunledgerAdminRoute } from "../src/react.js";
import {
  capabilities,
  definitions,
  events,
  jobId,
  jobResponse,
  jobs,
  logs,
  metrics,
  workflowResponse,
  workflows,
} from "./fixtures.js";

const client: RunledgerAdminClient = {
  capabilities: async () => capabilities,
  definitions: async () => definitions,
  job: async () => jobResponse,
  jobEvents: async () => events,
  jobLogs: async () => logs,
  jobs: async () => jobs,
  metrics: async () => metrics,
  workflow: async () => workflowResponse,
  workflows: async () => workflows,
};

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

function Harness() {
  const [route, setRoute] = useState<RunledgerAdminRoute>({ name: "overview" });
  return (
    <RunledgerAdminPanel
      client={client}
      onRouteChange={setRoute}
      pollIntervalMs={0}
      route={route}
    />
  );
}

describe("RunledgerAdminPanel", () => {
  it("renders overview metrics and effective access", async () => {
    render(<Harness />);
    expect(
      await screen.findByRole("heading", { level: 2, name: "Overview" }),
    ).toBeTruthy();
    expect(
      await screen.findByText("Organization aaaaaaaa", { exact: false }),
    ).toBeTruthy();
    expect(
      screen.getByRole("table", { name: "Current job health" }),
    ).toBeTruthy();
    expect(screen.getByText("jobs.customer.import")).toBeTruthy();
    expect(
      screen.getByRole("columnheader", { name: "Dead-lettered 24h" }),
    ).toBeTruthy();
    expect(screen.getByRole("cell", { name: "2" })).toBeTruthy();
  });

  it("uses controlled navigation and communicates redaction", async () => {
    render(<Harness />);
    fireEvent.click(await screen.findByRole("button", { name: "Jobs" }));
    expect(
      await screen.findByRole("heading", { level: 2, name: "Jobs" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("navigation", { name: "Job list pagination" }),
    ).toBeTruthy();
    fireEvent.click(await screen.findByRole("button", { name: jobId }));
    expect(
      await screen.findByRole("heading", { level: 2, name: `Job ${jobId}` }),
    ).toBeTruthy();
    expect(screen.getAllByRole("note")[0]?.textContent).toContain(
      "Sensitive fields hidden",
    );
    expect(screen.queryByText("private@example.test")).toBeNull();
    expect(screen.getByText("Message hidden")).toBeTruthy();
    expect(
      screen.getByRole("navigation", { name: "Event history pagination" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("navigation", { name: "Log history pagination" }),
    ).toBeTruthy();
  });

  it("does not navigate beyond the server-owned offset window", async () => {
    render(
      <RunledgerAdminPanel
        client={{
          ...client,
          jobs: async () => ({
            ...jobs,
            page: {
              ...jobs.page,
              has_more: true,
              offset: jobs.page.max_offset,
            },
          }),
        }}
        onRouteChange={() => undefined}
        pollIntervalMs={0}
        route={{ name: "jobs", offset: jobs.page.max_offset }}
      />,
    );

    const next = await screen.findByRole("button", { name: "Next" });
    expect((next as HTMLButtonElement).disabled).toBe(true);
  });

  it("renders an empty out-of-range page without a phantom row range", async () => {
    render(
      <RunledgerAdminPanel
        client={{
          ...client,
          jobs: async () => ({
            items: [],
            page: { ...jobs.page, offset: 50 },
          }),
        }}
        onRouteChange={() => undefined}
        pollIntervalMs={0}
        route={{ name: "jobs", offset: 50 }}
      />,
    );

    expect(await screen.findByText("Rows 0–0")).toBeTruthy();
  });

  it("navigates from a workflow step to its job without a router dependency", async () => {
    render(<Harness />);
    fireEvent.click(await screen.findByRole("button", { name: "Workflows" }));
    expect(
      await screen.findByRole("heading", { level: 2, name: "Workflows" }),
    ).toBeTruthy();
    fireEvent.click(
      await screen.findByRole("button", { name: workflows.items[0]!.id }),
    );
    expect(
      await screen.findByRole("heading", { level: 2, name: /Workflow/ }),
    ).toBeTruthy();
    expect(
      screen.getByRole("navigation", { name: "Workflow step pagination" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("navigation", {
        name: "Workflow dependency pagination",
      }),
    ).toBeTruthy();
    expect(
      screen.getByText("some prerequisites hidden", { exact: false }),
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: jobId }));
    expect(
      await screen.findByRole("heading", { level: 2, name: `Job ${jobId}` }),
    ).toBeTruthy();
  });

  it("does not request service-wide definitions when capabilities omit them", async () => {
    const definitionsSpy = vi.fn(client.definitions);
    const restrictedClient: RunledgerAdminClient = {
      ...client,
      capabilities: async () => ({
        ...capabilities,
        resources: capabilities.resources.filter(
          (resource) => resource !== "definitions",
        ),
      }),
      definitions: definitionsSpy,
    };
    render(
      <RunledgerAdminPanel
        client={restrictedClient}
        onRouteChange={() => undefined}
        pollIntervalMs={0}
        route={{ name: "definitions" }}
      />,
    );

    expect(
      await screen.findByRole("heading", { name: "Definitions unavailable" }),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Definitions" })).toBeNull();
    expect(definitionsSpy).not.toHaveBeenCalled();
  });

  it("pages backward through newest-first job history", async () => {
    const job = vi.fn(client.job);
    const jobEvents = vi.fn<RunledgerAdminClient["jobEvents"]>(
      async (_jobId, params) => ({
        ...events,
        items: [
          {
            ...events.items[0]!,
            id: params?.cursor === undefined ? "51" : "1",
          },
        ],
        page: {
          ...events.page,
          cursor: params?.cursor ?? null,
          has_more: params?.cursor === undefined,
          next_cursor: params?.cursor === undefined ? "2" : "1",
        },
      }),
    );
    const jobLogs = vi.fn(client.jobLogs);
    render(
      <RunledgerAdminPanel
        client={{ ...client, job, jobEvents, jobLogs }}
        onRouteChange={() => undefined}
        pollIntervalMs={0}
        route={{ name: "job", jobId }}
      />,
    );

    fireEvent.click(
      (await screen.findAllByRole("button", { name: "Older" }))[0]!,
    );
    expect(await screen.findByText("Page 2 · newest first")).toBeTruthy();
    expect(jobEvents).toHaveBeenLastCalledWith(
      jobId,
      expect.objectContaining({ cursor: "2" }),
    );
    expect(job).toHaveBeenCalledTimes(1);
    expect(jobLogs).toHaveBeenCalledTimes(1);
  });

  it("waits for each poll to settle before scheduling the next one", async () => {
    vi.useFakeTimers();
    let resolveFirst: ((value: typeof jobs) => void) | undefined;
    const first = new Promise<typeof jobs>((resolve) => {
      resolveFirst = resolve;
    });
    const jobsLoader = vi
      .fn<RunledgerAdminClient["jobs"]>()
      .mockImplementationOnce(async () => first)
      .mockResolvedValue(jobs);
    render(
      <RunledgerAdminPanel
        client={{ ...client, jobs: jobsLoader }}
        onRouteChange={() => undefined}
        pollIntervalMs={100}
        route={{ name: "jobs" }}
      />,
    );
    await act(async () => undefined);
    expect(jobsLoader).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(500));
    expect(jobsLoader).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveFirst?.(jobs);
      await Promise.resolve();
    });
    await act(async () => vi.advanceTimersByTimeAsync(100));
    expect(jobsLoader).toHaveBeenCalledTimes(2);
  });

  it("loads list data once when polling is not explicitly enabled", async () => {
    vi.useFakeTimers();
    const jobsLoader = vi.fn(client.jobs);
    render(
      <RunledgerAdminPanel
        client={{ ...client, jobs: jobsLoader }}
        onRouteChange={() => undefined}
        route={{ name: "jobs" }}
      />,
    );

    await act(async () => undefined);
    expect(jobsLoader).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(jobsLoader).toHaveBeenCalledTimes(1);
  });

  it("does not refetch aggregate metrics on the list and detail interval", async () => {
    vi.useFakeTimers();
    const capabilitiesLoader = vi.fn(client.capabilities);
    const metricsLoader = vi.fn(client.metrics);
    render(
      <RunledgerAdminPanel
        client={{
          ...client,
          capabilities: capabilitiesLoader,
          metrics: metricsLoader,
        }}
        onRouteChange={() => undefined}
        pollIntervalMs={100}
        route={{ name: "overview" }}
      />,
    );

    await act(async () => undefined);
    expect(capabilitiesLoader).toHaveBeenCalledTimes(1);
    expect(metricsLoader).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(300));
    expect(capabilitiesLoader).toHaveBeenCalledTimes(1);
    expect(metricsLoader).toHaveBeenCalledTimes(1);
  });

  it("polls aggregate metrics only on their explicit interval", async () => {
    vi.useFakeTimers();
    const capabilitiesLoader = vi.fn(client.capabilities);
    const metricsLoader = vi.fn(client.metrics);
    render(
      <RunledgerAdminPanel
        client={{
          ...client,
          capabilities: capabilitiesLoader,
          metrics: metricsLoader,
        }}
        metricsPollIntervalMs={100}
        onRouteChange={() => undefined}
        pollIntervalMs={0}
        route={{ name: "overview" }}
      />,
    );

    await act(async () => undefined);
    expect(capabilitiesLoader).toHaveBeenCalledTimes(1);
    expect(metricsLoader).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(300));
    expect(capabilitiesLoader).toHaveBeenCalledTimes(1);
    expect(metricsLoader.mock.calls.length).toBeGreaterThan(1);
  });

  it("reloads scoped resources when effective capabilities change", async () => {
    vi.useFakeTimers();
    const organizationB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const capabilitiesLoader = vi
      .fn<RunledgerAdminClient["capabilities"]>()
      .mockResolvedValueOnce({ ...capabilities, visibility: "full" })
      .mockResolvedValue({
        ...capabilities,
        scope: { kind: "organization", organization_id: organizationB },
        visibility: "metadata_only",
      });
    const jobsLoader = vi
      .fn<RunledgerAdminClient["jobs"]>()
      .mockResolvedValueOnce(jobs)
      .mockResolvedValue({
        ...jobs,
        items: [
          {
            ...jobs.items[0]!,
            job_type: "jobs.organization-b.import",
            organization_id: organizationB,
          },
        ],
      });

    render(
      <RunledgerAdminPanel
        capabilitiesPollIntervalMs={100}
        client={{
          ...client,
          capabilities: capabilitiesLoader,
          jobs: jobsLoader,
        }}
        onRouteChange={() => undefined}
        route={{ name: "jobs" }}
      />,
    );

    await act(async () => undefined);
    expect(screen.getByText("jobs.customer.import")).toBeTruthy();
    expect(jobsLoader).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(100));

    expect(capabilitiesLoader).toHaveBeenCalledTimes(2);
    expect(jobsLoader).toHaveBeenCalledTimes(2);
    expect(screen.queryByText("jobs.customer.import")).toBeNull();
    expect(screen.getByText("jobs.organization-b.import")).toBeTruthy();
  });
});
