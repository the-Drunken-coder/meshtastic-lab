import { useCallback, useEffect, useMemo, useState } from "react";
import { api, ApiError } from "./api";
import type {
  Capability,
  DaemonLogs,
  DirectedLink,
  Lifecycle,
  Metrics,
  NodeRole,
  NodeView,
  PacketEvent,
  Scenario,
  TopologyPreset,
  TrafficRequest,
  TrafficResult,
} from "./types";

const roles: NodeRole[] = ["CLIENT", "CLIENT_MUTE", "ROUTER", "REPEATER"];
const presets = [
  ["Full mesh", "full-mesh"],
  ["Line", "line"],
  ["Star", "star"],
  ["All isolated", "all-isolated"],
] as const;
const eventTypes = [
  "rf_transmit",
  "link_disabled",
  "rx_injected",
  "application_receive",
  "acknowledgment",
  "routing_error",
  "link_updated",
  "collision",
  "node_state",
  "lifecycle",
  "traffic",
  "ui_events_dropped",
] as const;

interface Notice {
  tone: "info" | "good" | "bad";
  text: string;
}

const emptyMetrics: Metrics = {
  generatedApplicationMessages: 0,
  uniqueApplicationMessagesDelivered: 0,
  deliveryRatio: null,
  receiverDeliveries: 0,
  receiverDeliveryRatio: null,
  acknowledgmentSuccessRatio: null,
  medianLatencyMs: null,
  p95LatencyMs: null,
  p99LatencyMs: null,
  rfTransmissions: 0,
  rfTransmissionsPerDelivery: null,
  relayTransmissions: 0,
  duplicateReceptions: 0,
  failedReceptions: 0,
  dropsByReason: {},
  observedAirtimeMs: 0,
  perNodeTransmitCounts: {},
  eventLoopLagMs: null,
};

const defaultTraffic: TrafficRequest = {
  kind: "broadcast-text",
  sourceNodes: ["node-1"],
  destinationStrategy: "fixed",
  fixedDestination: "node-2",
  messagesPerMinute: 12,
  payloadBytes: 64,
  durationSeconds: 20,
  acknowledgmentRequested: true,
  seed: 1,
};

function mergeEvents(current: PacketEvent[], incoming: PacketEvent[]): PacketEvent[] {
  const sequenced = new Map<number, PacketEvent>();
  const unsequenced: PacketEvent[] = [];
  for (const event of [...current, ...incoming]) {
    if (event.sequence > 0) sequenced.set(event.sequence, event);
    else unsequenced.push(event);
  }
  return [...unsequenced, ...sequenced.values()]
    .sort((left, right) => left.sequence - right.sequence)
    .slice(-500);
}

function trafficDraftForScenario(current: TrafficRequest, nextScenario: Scenario): TrafficRequest {
  return {
    ...current,
    sourceNodes: [nextScenario.nodes[0]?.id ?? "node-1"],
    fixedDestination: nextScenario.nodes[1]?.id,
  };
}

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return `${error.code}: ${error.message}`;
  return error instanceof Error ? error.message : "Unexpected request failure";
}

function formatRatio(value: number | null): string {
  return value === null ? "Unavailable" : `${(value * 100).toFixed(1)}%`;
}

function formatMilliseconds(value: number | null): string {
  if (value === null) return "Unavailable";
  return value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value.toFixed(0)} ms`;
}

function shortDigest(digest: string): string {
  return digest.replace("sha256:", "").slice(0, 12);
}

function makeNodes(count: number, previous: Scenario["nodes"]): Scenario["nodes"] {
  return Array.from({ length: count }, (_, offset) => {
    const index = offset + 1;
    return (
      previous[offset] ?? {
        id: `node-${index}`,
        displayName: `Node ${index}`,
        role: "CLIENT" as const,
        apiPort: 45000 + index,
      }
    );
  });
}

function makeLinks(nodeIds: string[], preset: TopologyPreset): DirectedLink[] {
  const hub = nodeIds[0];
  return nodeIds.flatMap((source, sourceIndex) =>
    nodeIds
      .filter((target) => target !== source)
      .map((target) => {
        const targetIndex = nodeIds.indexOf(target);
        const enabled =
          preset === "full-mesh" ||
          (preset === "line" && Math.abs(sourceIndex - targetIndex) === 1) ||
          (preset === "star" && (source === hub || target === hub));
        return { from: source, to: target, enabled, rssiDbm: -85, snrDb: 8 };
      }),
  );
}

function App() {
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice | null>(null);
  const [lifecycle, setLifecycle] = useState<Lifecycle | null>(null);
  const [capability, setCapability] = useState<Capability | null>(null);
  const [scenario, setScenario] = useState<Scenario | null>(null);
  const [savedScenario, setSavedScenario] = useState<Scenario | null>(null);
  const [nodes, setNodes] = useState<NodeView[]>([]);
  const [traffic, setTraffic] = useState<TrafficResult>({ state: "IDLE" });
  const [trafficDraft, setTrafficDraft] = useState<TrafficRequest>(defaultTraffic);
  const [events, setEvents] = useState<PacketEvent[]>([]);
  const [streamConnected, setStreamConnected] = useState(false);
  const [nodeFilter, setNodeFilter] = useState("");
  const [eventFilter, setEventFilter] = useState("");
  const [symmetric, setSymmetric] = useState(false);
  const [logNode, setLogNode] = useState("node-1");
  const [logStream, setLogStream] = useState<"stdout" | "stderr">("stderr");
  const [logs, setLogs] = useState<DaemonLogs | null>(null);

  const refreshCore = useCallback(async () => {
    const [nextLifecycle, nextNodes, nextTraffic] = await Promise.all([
      api.lifecycle(),
      api.nodes(),
      api.traffic(),
    ]);
    setLifecycle(nextLifecycle);
    setNodes(nextNodes);
    setTraffic(nextTraffic);
  }, []);

  useEffect(() => {
    let active = true;
    let retryTimer: number | undefined;
    const loadInitialState = () => {
      Promise.all([
        api.capabilities(),
        api.lifecycle(),
        api.scenario(),
        api.nodes(),
        api.events(),
        api.traffic(),
      ])
        .then(([nextCapability, nextLifecycle, nextScenario, nextNodes, nextEvents, nextTraffic]) => {
          if (!active) return;
          setCapability(nextCapability);
          setLifecycle(nextLifecycle);
          setScenario(nextScenario);
          setSavedScenario(nextScenario);
          setNodes(nextNodes);
          setEvents((current) => mergeEvents(current, nextEvents));
          setTraffic(nextTraffic);
          setTrafficDraft((current) => trafficDraftForScenario(current, nextScenario));
          setNotice(null);
          setLoading(false);
        })
        .catch((error: unknown) => {
          if (!active) return;
          setNotice({ tone: "bad", text: errorMessage(error) });
          retryTimer = window.setTimeout(loadInitialState, 1500);
        });
    };
    loadInitialState();
    return () => {
      active = false;
      if (retryTimer !== undefined) window.clearTimeout(retryTimer);
    };
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      refreshCore().catch((error: unknown) => {
        setNotice((current) => current ?? { tone: "bad", text: errorMessage(error) });
      });
    }, 1500);
    return () => window.clearInterval(timer);
  }, [refreshCore]);

  useEffect(() => {
    let reconnectTimer: number | undefined;
    let closed = false;
    let socket: WebSocket | undefined;
    const connect = () => {
      const protocol = window.location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${protocol}://${window.location.host}/api/events/ws`);
      socket.addEventListener("open", () => setStreamConnected(true));
      socket.addEventListener("message", (message) => {
        const event = JSON.parse(String(message.data)) as PacketEvent;
        setEvents((current) => mergeEvents(current, [event]));
      });
      socket.addEventListener("close", () => {
        setStreamConnected(false);
        if (!closed) reconnectTimer = window.setTimeout(connect, 2000);
      });
      socket.addEventListener("error", () => socket?.close());
    };
    connect();
    return () => {
      closed = true;
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  useEffect(() => {
    if (lifecycle?.state === "STOPPED") {
      setLogs(null);
      return;
    }
    api.logs(logNode, logStream)
      .then(setLogs)
      .catch(() => setLogs(null));
  }, [lifecycle?.state, logNode, logStream, nodes]);

  const stopped = lifecycle?.state === "STOPPED";
  const running = lifecycle?.state === "RUNNING";
  const trafficActive = traffic.state === "RUNNING" || traffic.state === "STOPPING";
  const dirty = Boolean(scenario && savedScenario && JSON.stringify(scenario) !== JSON.stringify(savedScenario));
  const metrics = traffic.metrics ?? emptyMetrics;
  const selectedEvents = useMemo(
    () =>
      events
        .filter((event) => {
          const matchesNode =
            !nodeFilter ||
            event.transmitter === nodeFilter ||
            event.receiver === nodeFilter ||
            event.receiverSet.includes(nodeFilter);
          return matchesNode && (!eventFilter || event.eventType === eventFilter);
        })
        .slice(-250)
        .reverse(),
    [eventFilter, events, nodeFilter],
  );

  const perform = async (label: string, action: () => Promise<void>) => {
    setBusy(label);
    try {
      await action();
      await refreshCore();
    } catch (error: unknown) {
      setNotice({ tone: "bad", text: errorMessage(error) });
    } finally {
      setBusy(null);
    }
  };

  const saveScenario = () => {
    if (!scenario) return;
    void perform("save", async () => {
      const updated = await api.replaceScenario(scenario);
      setScenario(updated);
      setSavedScenario(updated);
      setNotice({ tone: "good", text: `Saved scenario ${updated.name}.` });
    });
  };

  const lifecycleCommand = (command: "start" | "stop" | "reset") => {
    void perform(command, async () => {
      if (command === "start") await api.start();
      if (command === "stop") await api.stop();
      if (command === "reset") {
        await api.reset();
        const updated = await api.scenario();
        setScenario(updated);
        setSavedScenario(updated);
        setTrafficDraft((current) => trafficDraftForScenario(current, updated));
      }
    });
  };

  const updateScenarioCount = (count: number) => {
    if (!scenario) return;
    const nextNodes = makeNodes(count, scenario.nodes);
    setScenario({
      ...scenario,
      name: count === 5 ? "five-node-full-mesh" : `${count}-node-full-mesh`,
      nodeCount: count,
      nodes: nextNodes,
      links: makeLinks(nextNodes.map((node) => node.id), "full-mesh"),
    });
    setTrafficDraft((current) => ({
      ...current,
      sourceNodes: current.sourceNodes.filter((source) => nextNodes.some((node) => node.id === source)),
      fixedDestination: nextNodes.find((node) => node.id !== current.sourceNodes[0])?.id,
    }));
  };

  const applyPreset = (preset: TopologyPreset) => {
    void perform(`topology-${preset}`, async () => {
      if (dirty && stopped) throw new Error("Save scenario changes before applying a topology preset.");
      const updated = await api.applyTopology(preset);
      setScenario(updated);
      setSavedScenario(updated);
      setNotice({ tone: "good", text: `Applied ${preset} directed topology.` });
    });
  };

  const toggleLink = (link: DirectedLink) => {
    if (!scenario) return;
    const nextLinks = [
      { ...link, enabled: !link.enabled },
      ...(symmetric
        ? scenario.links
            .filter((candidate) => candidate.from === link.to && candidate.to === link.from)
            .map((candidate) => ({ ...candidate, enabled: !link.enabled }))
        : []),
    ];
    void perform(`link-${link.from}-${link.to}`, async () => {
      if (running) {
        await Promise.all(nextLinks.map((nextLink) => api.updateLink(nextLink)));
        const updated = await api.scenario();
        setScenario(updated);
        setSavedScenario(updated);
      } else if (stopped) {
        const keys = new Set(nextLinks.map((candidate) => `${candidate.from}:${candidate.to}`));
        const updated: Scenario = {
          ...scenario,
          links: scenario.links.map((candidate) => {
            const key = `${candidate.from}:${candidate.to}`;
            return keys.has(key)
              ? nextLinks.find((nextLink) => `${nextLink.from}:${nextLink.to}` === key) ?? candidate
              : candidate;
          }),
        };
        const saved = await api.replaceScenario(updated);
        setScenario(saved);
        setSavedScenario(saved);
      }
      setNotice({
        tone: "good",
        text: `${link.from} to ${link.to} ${link.enabled ? "disabled" : "enabled"}. Event recorded.`,
      });
    });
  };

  const toggleSource = (nodeId: string) => {
    setTrafficDraft((current) => ({
      ...current,
      sourceNodes: current.sourceNodes.includes(nodeId)
        ? current.sourceNodes.filter((source) => source !== nodeId)
        : [...current.sourceNodes, nodeId],
    }));
  };

  const runTraffic = () => {
    void perform("traffic", async () => {
      const request = { ...trafficDraft };
      if (request.kind === "broadcast-text") delete request.fixedDestination;
      const started = await api.startTraffic(request);
      setNotice({ tone: "good", text: `Traffic run ${started.runId.slice(0, 8)} started.` });
    });
  };

  if (loading || !lifecycle || !scenario || !capability) {
    return (
      <main className="loading-shell" aria-busy="true">
        <h1>Meshtastic Lab</h1>
        <p>Loading simulator state and native capability…</p>
        <div className="loading-lines" aria-hidden="true"><i /><i /><i /></div>
      </main>
    );
  }

  const firmwareVersion = nodes.find((node) => node.firmwareVersion)?.firmwareVersion;
  const phase = trafficActive || traffic.state === "COMPLETED" ? 4 : running ? 3 : stopped ? 1 : 2;

  return (
    <div className="app-shell">
      <header className="simulation-header">
        <div className="brand-block">
          <h1>Meshtastic Lab</h1>
          <span className="scenario-name mono">{scenario.name}</span>
        </div>
        <div className={`state-readout state-${lifecycle.state.toLowerCase()}`}>
          <span className="status-dot" />
          <strong>{lifecycle.state}</strong>
          <span>{lifecycle.message}</span>
        </div>
        <div className="header-actions">
          <button onClick={() => lifecycleCommand("start")} disabled={!stopped || dirty || busy !== null || !capability.collisionAvailable}>Start</button>
          <button onClick={() => lifecycleCommand("stop")} disabled={stopped || busy !== null}>Stop</button>
          <button onClick={() => lifecycleCommand("reset")} disabled={!stopped || busy !== null}>Reset</button>
        </div>
      </header>

      <nav className="workflow" aria-label="Simulation workflow">
        {["Scenario", "Native nodes", "Route warmup", "Traffic", "Results"].map((label, index) => {
          const number = index + 1;
          const status = number < phase ? "done" : number === phase ? "active" : "pending";
          return <div className={`workflow-step ${status}`} key={label}><b>{status === "done" ? "✓" : number}</b><span>{label}</span></div>;
        })}
      </nav>

      <section className="metric-strip" aria-label="Current traffic metrics">
        <Metric label="Generated" value={String(metrics.generatedApplicationMessages)} />
        <Metric label="Delivered" value={String(metrics.uniqueApplicationMessagesDelivered)} />
        <Metric label="Delivery" value={formatRatio(metrics.deliveryRatio)} />
        <Metric label="ACK success" value={formatRatio(metrics.acknowledgmentSuccessRatio)} />
        <Metric label="Median" value={formatMilliseconds(metrics.medianLatencyMs)} />
        <Metric label="P95" value={formatMilliseconds(metrics.p95LatencyMs)} />
        <Metric label="RF TX" value={String(metrics.rfTransmissions)} />
        <Metric label="Airtime" value={formatMilliseconds(metrics.observedAirtimeMs)} />
      </section>

      {notice && (
        <div className={`global-notice notice-${notice.tone}`} role={notice.tone === "bad" ? "alert" : "status"}>
          <span>{notice.text}</span>
          <button className="quiet" onClick={() => setNotice(null)} aria-label="Dismiss notice">Dismiss</button>
        </div>
      )}

      <div className="workspace">
        <aside className="control-rail">
          <section>
            <div className="section-heading"><h2>RF profile</h2>{dirty && <span className="changed">Unsaved</span>}</div>
            <div className="field-grid">
              <Field label="Nodes"><input type="number" min={2} max={10} value={scenario.nodeCount} disabled={!stopped} onChange={(event) => updateScenarioCount(Number(event.currentTarget.value))} /></Field>
              <Field label="Region"><input value={scenario.rf.region} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, rf: { ...scenario.rf, region: event.currentTarget.value } })} /></Field>
              <Field label="Modem preset"><select value={scenario.rf.modemPreset} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, rf: { ...scenario.rf, modemPreset: event.currentTarget.value } })}>{["LONG_FAST", "LONG_SLOW", "MEDIUM_SLOW", "MEDIUM_FAST", "SHORT_SLOW", "SHORT_FAST", "LONG_MODERATE", "SHORT_TURBO", "LONG_TURBO"].map((preset) => <option key={preset}>{preset}</option>)}</select></Field>
              <Field label="Frequency slot"><input type="number" min={0} max={255} value={scenario.rf.frequencySlot} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, rf: { ...scenario.rf, frequencySlot: Number(event.currentTarget.value) } })} /></Field>
              <Field label="Hop limit"><input type="number" min={1} max={7} value={scenario.rf.hopLimit} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, rf: { ...scenario.rf, hopLimit: Number(event.currentTarget.value) } })} /></Field>
              <Field label="Fresh state"><select value={String(scenario.freshState)} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, freshState: event.currentTarget.value === "true" })}><option value="true">Yes</option><option value="false">No</option></select></Field>
            </div>
            <h3>Encrypted logical channel</h3>
            <Field label="Primary channel name"><input value={scenario.channel.name} maxLength={12} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, channel: { ...scenario.channel, name: event.currentTarget.value } })} /></Field>
            {!stopped && <p className="field-note">Stop the simulation to change firmware-owned settings.</p>}
            {stopped && <button className="primary full" onClick={saveScenario} disabled={!dirty || busy !== null}>Save scenario</button>}
          </section>

          <section>
            <h2>Node roles and endpoints</h2>
            <div className="node-rows">
              {scenario.nodes.map((node) => {
                const live = nodes.find((candidate) => candidate.id === node.id);
                return (
                  <div className="node-row" key={node.id}>
                    <div><strong>{node.displayName}</strong><span className="mono">{live?.publicEndpoint ?? `127.0.0.1:${node.apiPort}`}</span></div>
                    <select aria-label={`${node.displayName} role`} value={node.role} disabled={!stopped} onChange={(event) => setScenario({ ...scenario, nodes: scenario.nodes.map((candidate) => candidate.id === node.id ? { ...candidate, role: event.currentTarget.value as NodeRole } : candidate) })}>{roles.map((role) => <option key={role}>{role}</option>)}</select>
                    <button className="copy" onClick={() => { void navigator.clipboard.writeText(live?.publicEndpoint ?? `127.0.0.1:${node.apiPort}`); setNotice({ tone: "info", text: `Copied ${live?.publicEndpoint ?? `127.0.0.1:${node.apiPort}`}.` }); }}>Copy</button>
                    <div className="node-facts"><span className={live?.processState === "RUNNING" ? "good" : "muted"}>{live?.processState ?? "STOPPED"}</span><span>Gateway {live?.gatewayState ?? "STOPPED"}</span><span>Client {live?.externalClientConnected ? "connected" : "free"}</span><span>TX {live?.transmitCount ?? 0} · RX {live?.receiveCount ?? 0}</span><span>Util {live?.channelUtilization == null ? "Unavailable" : `${live.channelUtilization.toFixed(1)}%`}</span></div>
                  </div>
                );
              })}
            </div>
          </section>

          <section>
            <h2>Runtime facts</h2>
            <dl className="facts">
              <div><dt>Firmware</dt><dd className="mono">{firmwareVersion ?? "Unavailable"} · {capability.firmwareCommit.slice(0, 7)}</dd></div>
              <div><dt>Binary</dt><dd className="mono">{shortDigest(capability.firmwareBinarySha256)}</dd></div>
              <div><dt>Build</dt><dd className="mono">{capability.buildArchitecture} · client {capability.clientLibraryVersion}</dd></div>
              <div><dt>Collision</dt><dd className={capability.collisionAvailable ? "good" : "bad"}>{capability.collisionModel} · {capability.collisionAvailable ? "available" : "unavailable"}</dd></div>
              <div><dt>Event stream</dt><dd className={streamConnected ? "good" : "warn"}>{streamConnected ? "connected" : "reconnecting"}</dd></div>
              <div><dt>Loop lag</dt><dd>{formatMilliseconds(metrics.eventLoopLagMs)}</dd></div>
            </dl>
            <p className="field-note">{capability.collisionDetail}</p>
          </section>

          <section>
            <h2>Daemon diagnostics</h2>
            <div className="diagnostic-controls">
              <select aria-label="Log node" value={logNode} onChange={(event) => setLogNode(event.currentTarget.value)}>{scenario.nodes.map((node) => <option value={node.id} key={node.id}>{node.displayName}</option>)}</select>
              <select aria-label="Log stream" value={logStream} onChange={(event) => setLogStream(event.currentTarget.value as "stdout" | "stderr")}><option value="stderr">stderr</option><option value="stdout">stdout</option></select>
            </div>
            <pre className="daemon-log">{logs?.lines.slice(-18).join("\n") || (stopped ? "Start the simulation to inspect native logs." : "No recent lines for this stream.")}</pre>
          </section>
        </aside>

        <main className="experiment-area">
          <section className="topology-section">
            <div className="section-toolbar">
              <div><h2>Directed topology</h2><p>Rows transmit. Columns receive. RSSI −85 dBm · SNR 8 dB.</p></div>
              <label className="inline-check"><input type="checkbox" checked={symmetric} onChange={(event) => setSymmetric(event.currentTarget.checked)} /> Edit symmetrically</label>
              <div className="preset-actions">{presets.map(([label, preset]) => <button key={preset} onClick={() => applyPreset(preset)} disabled={busy !== null || (!stopped && !running)}>{label}</button>)}</div>
            </div>
            <div className="matrix-scroll">
              <table className="topology-matrix">
                <thead><tr><th>TX \ RX</th>{scenario.nodes.map((node) => <th key={node.id}>{node.displayName}</th>)}</tr></thead>
                <tbody>{scenario.nodes.map((source) => <tr key={source.id}><th>{source.displayName}</th>{scenario.nodes.map((target) => {
                  if (source.id === target.id) return <td className="diagonal" key={target.id}>·</td>;
                  const link = scenario.links.find((candidate) => candidate.from === source.id && candidate.to === target.id);
                  if (!link) return <td key={target.id}>Missing</td>;
                  return <td key={target.id}><button className={`link-toggle ${link.enabled ? "enabled" : "disabled"}`} aria-label={`${link.enabled ? "Disable" : "Enable"} link from ${source.displayName} to ${target.displayName}`} onClick={() => toggleLink(link)} disabled={busy !== null || (!stopped && !running)}>{link.enabled ? "ON" : "OFF"}</button></td>;
                })}</tr>)}</tbody>
              </table>
            </div>
          </section>

          <section className="run-section">
            <div className="traffic-config">
              <div className="section-heading"><h2>Traffic definition</h2><span className={`traffic-state state-${traffic.state.toLowerCase()}`}>{traffic.state}</span></div>
              <div className="traffic-fields">
                <Field label="Message kind"><select value={trafficDraft.kind} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, kind: event.currentTarget.value as TrafficRequest["kind"] })}><option value="broadcast-text">Broadcast text</option><option value="direct-text">Direct text</option></select></Field>
                <Field label="Destination strategy"><select value={trafficDraft.destinationStrategy} disabled={trafficActive || trafficDraft.kind === "broadcast-text"} onChange={(event) => setTrafficDraft({ ...trafficDraft, destinationStrategy: event.currentTarget.value as TrafficRequest["destinationStrategy"] })}><option value="fixed">Fixed</option><option value="round-robin">Round robin</option><option value="deterministic-random">Deterministic random</option></select></Field>
                <Field label="Fixed destination"><select value={trafficDraft.fixedDestination} disabled={trafficActive || trafficDraft.kind === "broadcast-text" || trafficDraft.destinationStrategy !== "fixed"} onChange={(event) => setTrafficDraft({ ...trafficDraft, fixedDestination: event.currentTarget.value })}>{scenario.nodes.map((node) => <option value={node.id} key={node.id}>{node.displayName}</option>)}</select></Field>
                <Field label="Messages/min/source"><input type="number" min={0.1} max={600} step={0.1} value={trafficDraft.messagesPerMinute} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, messagesPerMinute: Number(event.currentTarget.value) })} /></Field>
                <Field label="Payload bytes"><input type="number" min={48} max={233} value={trafficDraft.payloadBytes} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, payloadBytes: Number(event.currentTarget.value) })} /></Field>
                <Field label="Duration seconds"><input type="number" min={1} max={3600} value={trafficDraft.durationSeconds} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, durationSeconds: Number(event.currentTarget.value) })} /></Field>
                <Field label="Random seed"><input type="number" value={trafficDraft.seed} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, seed: Number(event.currentTarget.value) })} /></Field>
              </div>
              <fieldset className="source-set"><legend>Source nodes</legend>{scenario.nodes.map((node) => <label key={node.id}><input type="checkbox" checked={trafficDraft.sourceNodes.includes(node.id)} disabled={trafficActive} onChange={() => toggleSource(node.id)} /> {node.displayName}</label>)}</fieldset>
              <label className="inline-check"><input type="checkbox" checked={trafficDraft.acknowledgmentRequested} disabled={trafficActive} onChange={(event) => setTrafficDraft({ ...trafficDraft, acknowledgmentRequested: event.currentTarget.checked })} /> Request acknowledgments</label>
              <div className="traffic-actions"><button className="primary" onClick={runTraffic} disabled={!running || trafficActive || trafficDraft.sourceNodes.length === 0 || busy !== null}>Start traffic run</button><button onClick={() => { void perform("stop-traffic", () => api.stopTraffic().then(() => undefined)); }} disabled={!trafficActive || busy !== null}>Stop run</button>{traffic.runId && <a href={`/api/traffic/runs/${traffic.runId}/export`}>Export result</a>}</div>
              {traffic.failure && <p className="error-text">{traffic.failure}</p>}
            </div>

            <div className="metric-detail">
              <h2>Run accounting</h2>
              <dl className="facts two-column">
                <div><dt>Requested / submitted</dt><dd>{traffic.requested ?? 0} / {traffic.submitted ?? 0}</dd></div>
                <div><dt>Submission failed</dt><dd>{traffic.submissionFailed ?? 0}</dd></div>
                <div><dt>Transmitted / delivered</dt><dd>{traffic.transmitted ?? 0} / {traffic.delivered ?? 0}</dd></div>
                <div><dt>RF TX / delivery</dt><dd>{metrics.rfTransmissionsPerDelivery?.toFixed(2) ?? "Unavailable"}</dd></div>
                <div><dt>Receiver deliveries</dt><dd>{metrics.receiverDeliveries}</dd></div>
                <div><dt>Receiver delivery</dt><dd>{formatRatio(metrics.receiverDeliveryRatio)}</dd></div>
                <div><dt>Receivers / broadcast</dt><dd>Completed export</dd></div>
                <div><dt>Relay TX</dt><dd>{metrics.relayTransmissions}</dd></div>
                <div><dt>Duplicate RX</dt><dd>{metrics.duplicateReceptions}</dd></div>
                <div><dt>Failed/bad RX</dt><dd>{metrics.failedReceptions}{traffic.state !== "IDLE" && !traffic.failedReceptionMetricsComplete ? ` (incomplete: ${traffic.missingLocalStatsNodes.join(", ")})` : ""}</dd></div>
                <div><dt>P99</dt><dd>{formatMilliseconds(metrics.p99LatencyMs)}</dd></div>
                <div><dt>Drops</dt><dd>{Object.keys(metrics.dropsByReason).length ? Object.entries(metrics.dropsByReason).map(([reason, count]) => `${reason}: ${count}`).join(", ") : "None observed"}</dd></div>
                <div><dt>Per-node TX</dt><dd>{Object.keys(metrics.perNodeTransmitCounts).length ? Object.entries(metrics.perNodeTransmitCounts).map(([node, count]) => `${node}: ${count}`).join(", ") : "None observed"}</dd></div>
              </dl>
            </div>
          </section>

          <section className="evidence-section">
            <div className="section-toolbar evidence-toolbar">
              <div><h2>Packet evidence</h2><p>{selectedEvents.length} recent matching events. Newest first.</p></div>
              <select aria-label="Filter events by node" value={nodeFilter} onChange={(event) => setNodeFilter(event.currentTarget.value)}><option value="">All nodes</option>{scenario.nodes.map((node) => <option value={node.id} key={node.id}>{node.displayName}</option>)}</select>
              <select aria-label="Filter events by type" value={eventFilter} onChange={(event) => setEventFilter(event.currentTarget.value)}><option value="">All event types</option>{eventTypes.map((eventType) => <option key={eventType}>{eventType}</option>)}</select>
              <a href="/api/scenario/export">Export scenario</a>
            </div>
            <div className="evidence-scroll">
              <table className="evidence-table">
                <thead><tr><th>Time</th><th>Event</th><th>TX</th><th>Destination</th><th>Receiver(s)</th><th>Packet ID</th><th>Run seq</th><th>Hop</th><th>RSSI</th><th>SNR</th><th>Port</th><th>Bytes</th><th>Result</th></tr></thead>
                <tbody>{selectedEvents.length ? selectedEvents.map((event) => <tr key={event.sequence}><td>{new Date(event.utcTimestamp).toLocaleTimeString([], { hour12: false, fractionalSecondDigits: 3 })}</td><td>{event.eventType}</td><td>{event.transmitter ?? "·"}</td><td>{event.intendedDestination ?? "·"}</td><td>{event.receiver ?? (event.receiverSet.join(", ") || "·")}</td><td className="mono">{event.meshPacketId?.toString(16) ?? "·"}</td><td>{event.trafficSequence ?? "·"}</td><td>{event.hopLimit == null ? "·" : `${event.hopLimit}/${event.hopStart ?? "·"}`}</td><td>{event.rssiDbm ?? "·"}</td><td>{event.snrDb ?? "·"}</td><td>{event.portNumber ?? "·"}</td><td>{event.packetLength ?? "·"}</td><td className={event.result?.includes("fail") || event.result === "dropped" ? "bad" : ""}>{event.result ?? event.detail ?? "·"}</td></tr>) : <tr><td colSpan={13} className="empty-row">No packet events match these filters.</td></tr>}</tbody>
              </table>
            </div>
          </section>
        </main>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><strong>{value}</strong><span>{label}</span></div>;
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return <label className="field"><span>{label}</span>{children}</label>;
}

export default App;
