// 后端响应类型 — 跟 backend 的 Java records 一一对应。
// 字段命名统一用 camelCase（Spring Boot 默认序列化）。

export interface SlotHealth {
  slotName: string;
  plugin: string;
  active: boolean;
  walRetainedBytes: number;
  confirmedFlushLagBytes: number;
}

export interface FlinkJob {
  jid: string;
  name: string;
  state: string;
  duration: number;
  tasks: { running: number; total: number; failed: number };
}

export interface FlinkOverview {
  taskmanagers: number;
  'slots-total': number;
  'slots-available': number;
  'jobs-running': number;
  'flink-version': string;
}

export interface SyncStatus {
  flinkOverview: FlinkOverview;
  flinkJobs: FlinkJob[];
  slots: SlotHealth[];
}

export interface SyncMetricsPoint {
  windowStart: string;     // ISO timestamp
  windowEnd: string;
  jobName: string;
  recordsIn: number;
  recordsOut: number;
  recordsDlq: number;
  p50LatencyMs: number;
  p99LatencyMs: number;
}

export interface DlqEntry {
  id: number;
  srcId: number | null;
  op: string | null;
  errorClass: string;
  errorMessage: string;
  rawPayload: string;
  occurredAt: string;
  replayedAt: string | null;
  replayCount: number;
}

export interface LivePoint {
  id: number;
  name: string;
  lon: number;
  lat: number;
  syncTime: string;
}

// WebSocket 推送的 CDC 事件（backend CdcKafkaConsumer 生成的扁平 JSON）
export interface CdcWsEvent {
  id: number | null;
  op: string;       // c / d
  name: string | null;
  geom_ewkt: string | null;
  src_update_time: number | null;
  received_at: number;
}
