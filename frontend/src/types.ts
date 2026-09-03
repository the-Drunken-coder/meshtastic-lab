export type LifecycleState =
  | "STOPPED"
  | "STARTING"
  | "WARMING_UP"
  | "RUNNING"
  | "STOPPING"
  | "FAILED";

export type NodeRole = "CLIENT" | "CLIENT_MUTE" | "ROUTER" | "REPEATER";
export type TopologyPreset = "full-mesh" | "line" | "star" | "all-isolated";

export interface Lifecycle {
  state: LifecycleState;
  simulationId: string | null;
  message: string;
  warmingUpUntil: string | null;
  activeTrafficRunId: string | null;
}

export interface Capability {
  collisionModel: string;
  collisionAvailable: boolean;
  collisionDetail: string;
  maximumNodes: number;
  supportedContainerArchitectures: string[];
  provenanceAvailable: boolean;
  firmwareCommit: string;
  collisionPatchSha256: string;
  firmwareBinarySha256: string;
  buildArchitecture: string;
  clientLibraryVersion: string;
  upstreamBaseImageDigest: string;
  meshtasticatorCommit: string;
}

export interface ScenarioNode {
  id: string;
  displayName: string;
  role: NodeRole;
  apiPort: number;
}

export interface DirectedLink {
  from: string;
  to: string;
  enabled: boolean;
  rssiDbm: number;
  snrDb: number;
}

export interface Scenario {
  schemaVersion: 1;
  name: string;
  seed: number;
  nodeCount: number;
  rf: {
    region: string;
    modemPreset: string;
    frequencySlot: number;
    hopLimit: number;
    collisionMode: "native";
  };
  channel: { name: string; psk: string };
  nodes: ScenarioNode[];
  links: DirectedLink[];
  freshState: boolean;
}

export interface NodeView {
  id: string;
  name: string;
  role: NodeRole;
  processState: string;
  processId: number | null;
  gatewayState: string;
  externalClientConnected: boolean;
  publicEndpoint: string;
  firmwareVersion: string | null;
  nodeNumber: number | null;
  transmitCount: number;
  receiveCount: number;
  failedReceiveCount: number;
  duplicateReceiveCount: number;
  channelUtilization: number | null;
}

export interface Metrics {
  generatedApplicationMessages: number;
  uniqueApplicationMessagesDelivered: number;
  deliveryRatio: number | null;
  receiverDeliveries: number;
  receiverDeliveryRatio: number | null;
  acknowledgmentSuccessRatio: number | null;
  medianLatencyMs: number | null;
  p95LatencyMs: number | null;
  p99LatencyMs: number | null;
  rfTransmissions: number;
  rfTransmissionsPerDelivery: number | null;
  relayTransmissions: number;
  duplicateReceptions: number;
  failedReceptions: number;
  dropsByReason: Record<string, number>;
  observedAirtimeMs: number;
  perNodeTransmitCounts: Record<string, number>;
  perNodeAirtimeMs: Record<string, number>;
  eventLoopLagMs: number | null;
}

export type TrafficState = "IDLE" | "RUNNING" | "STOPPING" | "COMPLETED" | "CANCELLED" | "FAILED";

interface IdleTrafficResult {
  state: "IDLE";
  runId?: never;
  requested?: never;
  submitted?: never;
  submissionFailed?: never;
  transmitted?: never;
  delivered?: never;
  metrics?: never;
  failure?: never;
}

interface TrafficRunSummary {
  schemaVersion: 1;
  runId: string;
  state: Exclude<TrafficState, "IDLE">;
  request: TrafficRequest;
  scenarioSnapshot: Scenario;
  firmwareCommit: string;
  collisionPatchSha256: string;
  firmwareBinarySha256: string;
  buildArchitecture: string;
  upstreamBaseImageDigest: string;
  meshtasticatorCommit: string;
  clientLibraryVersion: string;
  collisionModel: "native";
  startedAt: string;
  finishedAt: string | null;
  randomSeed: number;
  requested: number;
  submitted: number;
  submissionFailed: number;
  transmitted: number;
  delivered: number;
  failedReceptionMetricsComplete: boolean;
  missingLocalStatsNodes: string[];
  metrics: Metrics;
  failure: string | null;
}

export type TrafficResult = IdleTrafficResult | TrafficRunSummary;

export interface PacketEvent {
  schemaVersion: 1;
  streamId: string;
  sequence: number;
  utcTimestamp: string;
  monotonicSeconds: number;
  eventType: string;
  transmitter: string | null;
  intendedDestination: string | null;
  receiver: string | null;
  receiverSet: string[];
  meshPacketId: number | null;
  trafficRunId: string | null;
  trafficSequence: number | null;
  hopLimit: number | null;
  hopStart: number | null;
  rssiDbm: number | null;
  snrDb: number | null;
  portNumber: number | null;
  packetLength: number | null;
  airtimeMs: number | null;
  metricUpdate: Record<string, number | Record<string, number> | null>;
  result: string | null;
  detail: string | null;
}

export interface EventHistoryPage {
  schemaVersion: 1;
  streamId: string;
  streamChanged: boolean;
  events: PacketEvent[];
  firstAvailableSequence: number | null;
  latestSequence: number;
  historyGap: boolean;
  hasMore: boolean;
}

export interface TrafficRequest {
  kind: "broadcast-text" | "direct-text";
  sourceNodes: string[];
  destinationStrategy: "fixed" | "round-robin" | "deterministic-random";
  fixedDestination?: string;
  messagesPerMinute: number;
  payloadBytes: number;
  durationSeconds: number;
  acknowledgmentRequested: boolean;
  seed: number;
}

export interface DaemonLogs {
  nodeId: string;
  stream: "stdout" | "stderr";
  lines: string[];
}
