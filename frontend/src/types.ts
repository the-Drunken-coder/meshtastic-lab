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
  firmwareImageDigest: string;
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
  receiversPerBroadcast: Record<string, number>;
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
  eventLoopLagMs: number | null;
}

export type TrafficState = "IDLE" | "RUNNING" | "STOPPING" | "COMPLETED" | "CANCELLED" | "FAILED";

export interface TrafficResult {
  runId?: string;
  state: TrafficState;
  requested?: number;
  submitted?: number;
  submissionFailed?: number;
  transmitted?: number;
  delivered?: number;
  metrics?: Metrics;
  failure?: string | null;
}

export interface PacketEvent {
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
  result: string | null;
  detail: string | null;
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
