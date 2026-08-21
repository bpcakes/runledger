import { describe, expect, it } from "vitest";

import {
  RunledgerAdminContractError,
  RunledgerAdminHttpError,
  createRunledgerAdminClient,
  type FetchLike,
} from "../src/client.js";
import {
  capabilities,
  definitions,
  events,
  jobId,
  jobResponse,
  jobs,
  logs,
  metrics,
  workflowId,
  workflowResponse,
  workflows,
} from "./fixtures.js";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": "application/json" },
    status,
  });
}

describe("createRunledgerAdminClient", () => {
  it("calls every endpoint through the generated v1 contract", async () => {
    const calls: Array<{
      readonly input: string;
      readonly init?: RequestInit;
    }> = [];
    const fetcher: FetchLike = async (input, init) => {
      const url = String(input);
      calls.push({ input: url, ...(init === undefined ? {} : { init }) });
      if (url.endsWith("/capabilities")) return jsonResponse(capabilities);
      if (url.includes("/metrics")) return jsonResponse(metrics);
      if (url.includes(`/jobs/${jobId}/events`)) return jsonResponse(events);
      if (url.includes(`/jobs/${jobId}/logs`)) return jsonResponse(logs);
      if (url.endsWith(`/jobs/${jobId}`)) return jsonResponse(jobResponse);
      if (url.includes("/jobs")) return jsonResponse(jobs);
      if (url.includes(`/workflows/${workflowId}`))
        return jsonResponse(workflowResponse);
      if (url.includes("/workflows")) return jsonResponse(workflows);
      if (url.includes("/definitions")) return jsonResponse(definitions);
      return jsonResponse(
        { error: { code: "test.missing", message: "Missing fixture." } },
        500,
      );
    };
    const client = createRunledgerAdminClient({
      baseUrl: "/custom/runledger/",
      credentials: "include",
      fetch: fetcher,
      headers: async () => ({ "X-CSRF-Token": "csrf-token" }),
    });

    await client.capabilities();
    await client.metrics({ jobType: "jobs.customer" });
    await client.jobs({ limit: 25, offset: 50, status: "PENDING" });
    await client.job(jobId);
    await client.jobEvents(jobId, {
      cursor: "9007199254740993",
      limit: 10,
      order: "oldest_first",
    });
    await client.jobLogs(jobId);
    await client.workflows({ workflowType: "customer" });
    await client.workflow(workflowId, { dependencyOffset: 25, stepLimit: 10 });
    await client.definitions({ jobType: "import" });

    expect(calls).toHaveLength(9);
    expect(calls[1]?.input).toBe(
      "/custom/runledger/metrics?job_type=jobs.customer",
    );
    expect(calls[2]?.input).toBe(
      "/custom/runledger/jobs?limit=25&offset=50&status=PENDING",
    );
    expect(calls[4]?.input).toBe(
      `/custom/runledger/jobs/${jobId}/events?cursor=9007199254740993&limit=10&order=oldest_first`,
    );
    expect(calls[7]?.input).toBe(
      `/custom/runledger/workflows/${workflowId}?dependency_offset=25&step_limit=10`,
    );
    expect(calls[0]?.init?.credentials).toBe("include");
    const headers = new Headers(calls[0]?.init?.headers);
    expect(headers.get("Accept")).toBe("application/json");
    expect(headers.get("X-CSRF-Token")).toBe("csrf-token");
  });

  it("returns a typed safe HTTP error", async () => {
    const client = createRunledgerAdminClient({
      fetch: async () =>
        jsonResponse(
          {
            error: {
              code: "admin.unauthorized",
              message: "Admin access is required.",
            },
          },
          401,
        ),
    });

    const error = await client
      .capabilities()
      .catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(RunledgerAdminHttpError);
    expect(error).toMatchObject({ code: "admin.unauthorized", status: 401 });
    expect((error as Error).message).toBe("Admin access is required.");
  });

  it("preserves HTTP status when an upstream error is not JSON", async () => {
    const client = createRunledgerAdminClient({
      fetch: async () => new Response("Bad gateway", { status: 502 }),
    });

    const error = await client
      .capabilities()
      .catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(RunledgerAdminHttpError);
    expect(error).toMatchObject({ code: "admin.http_error", status: 502 });
  });

  it("rejects a successful response that is not JSON", async () => {
    const client = createRunledgerAdminClient({
      fetch: async () => new Response("not JSON"),
    });
    await expect(client.metrics()).rejects.toBeInstanceOf(
      RunledgerAdminContractError,
    );
  });

  it("rejects an empty successful response", async () => {
    const client = createRunledgerAdminClient({
      fetch: async () =>
        new Response(null, {
          headers: { "Content-Length": "0" },
          status: 200,
        }),
    });
    await expect(client.metrics()).rejects.toBeInstanceOf(
      RunledgerAdminContractError,
    );
  });
});
