import type { DlqEntry, LivePoint, SyncMetricsPoint, SyncStatus } from '../types/api';

async function get<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path} ${r.status}`);
  return r.json();
}

async function post<T>(path: string): Promise<T> {
  const r = await fetch(path, { method: 'POST' });
  if (!r.ok) {
    const body = await r.text();
    throw new Error(body || `${path} ${r.status}`);
  }
  return r.json();
}

export const api = {
  status: () => get<SyncStatus>('/api/sync/status'),
  metrics: (fromMs: number, toMs: number) =>
    get<SyncMetricsPoint[]>(`/api/sync/metrics?from=${fromMs}&to=${toMs}`),
  livePoints: (limit = 200) =>
    get<LivePoint[]>(`/api/sync/live-points?limit=${limit}`),
  dlq: (limit = 50, unreplayed = false) =>
    get<DlqEntry[]>(`/api/dlq?limit=${limit}&unreplayed=${unreplayed}`),
  dlqReplay: (id: number) =>
    post<{ status: string; delivery: string }>(`/api/dlq/${id}/replay`),
};
