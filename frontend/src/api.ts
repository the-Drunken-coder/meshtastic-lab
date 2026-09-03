import type {
  Capability,
  DaemonLogs,
  DirectedLink,
  Lifecycle,
  NodeView,
  PacketEvent,
  Scenario,
  TopologyPreset,
  TrafficRequest,
  TrafficResult,
} from "./types";

interface ErrorEnvelope {
  error?: { code?: string; message?: string };
  detail?: string | Array<{ msg?: string }>;
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as ErrorEnvelope;
    const validation = Array.isArray(body.detail)
      ? body.detail.map((entry) => entry.msg).filter(Boolean).join("; ")
      : body.detail;
    throw new ApiError(
      body.error?.message ?? validation ?? response.statusText,
      body.error?.code ?? `HTTP_${response.status}`,
      response.status,
    );
  }
  return (await response.json()) as T;
}

const post = <T>(path: string, body?: object) =>
  request<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined });

export const api = {
  capabilities: () => request<Capability>("/api/capabilities"),
  lifecycle: () => request<Lifecycle>("/api/state"),
  scenario: () => request<Scenario>("/api/scenario"),
  nodes: () => request<NodeView[]>("/api/nodes"),
  events: () => request<PacketEvent[]>("/api/events?limit=500"),
  traffic: () => request<TrafficResult>("/api/traffic/runs/current"),
  logs: (nodeId: string, stream: "stdout" | "stderr") =>
    request<DaemonLogs>(`/api/nodes/${encodeURIComponent(nodeId)}/logs?stream=${stream}&limit=100`),
  replaceScenario: (scenario: Scenario) =>
    request<Scenario>("/api/scenario", { method: "PUT", body: JSON.stringify(scenario) }),
  updateLink: (link: DirectedLink) =>
    request<DirectedLink>("/api/links", { method: "PUT", body: JSON.stringify(link) }),
  updateLinks: (links: DirectedLink[]) =>
    request<DirectedLink[]>("/api/links/batch", {
      method: "PUT",
      body: JSON.stringify(links),
    }),
  applyTopology: (preset: TopologyPreset) => post<Scenario>("/api/topology", { preset }),
  start: () => post<Lifecycle>("/api/simulation/start"),
  stop: () => post<Lifecycle>("/api/simulation/stop"),
  reset: () => post<Lifecycle>("/api/simulation/reset"),
  startTraffic: (traffic: TrafficRequest) => post<{ runId: string; state: string }>("/api/traffic/runs", traffic),
  stopTraffic: () => post<TrafficResult>("/api/traffic/runs/stop"),
};
