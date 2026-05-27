import { ConfigProvider } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { useCallback, useState } from 'react';
import { api } from './api/client';
import { DlqList } from './components/DlqList';
import { KpiBar } from './components/KpiBar';
import { LiveStream } from './components/LiveStream';
import { MapPanel } from './components/MapPanel';
import { MetricsChart } from './components/MetricsChart';
import { useCdcStream } from './hooks/useCdcStream';
import { usePolling } from './hooks/usePolling';

function App() {
  // 触发 DLQ 列表刷新的 nonce
  const [dlqVersion, setDlqVersion] = useState(0);
  const refreshDlq = useCallback(() => setDlqVersion((v) => v + 1), []);

  // 各个数据源
  const { data: status } = usePolling(() => api.status(), 5000);
  const { data: metrics } = usePolling(
    () => {
      const now = Date.now();
      return api.metrics(now - 30 * 60 * 1000, now);
    },
    10000,
  );
  const { data: livePoints } = usePolling(() => api.livePoints(200), 30000);
  const { data: dlq } = usePolling(() => api.dlq(50, false), 8000, [dlqVersion]);

  // WebSocket 实时流
  const { events: liveEvents, connected: wsConnected } = useCdcStream(80);

  const unreplayed = (dlq ?? []).filter((d) => !d.replayedAt).length;

  return (
    <ConfigProvider locale={zhCN}>
      <div className="app-container">
        <header className="app-header">
          <h1>GIS Sync 看板</h1>
          <span style={{ fontSize: 12, color: '#666' }}>
            Flink {status?.flinkOverview?.['flink-version']} ·
            slots {status?.flinkOverview?.['slots-available']}/{status?.flinkOverview?.['slots-total']}
          </span>
        </header>

        <KpiBar status={status} metrics={metrics} unreplayed={unreplayed} />

        <div className="app-grid">
          <MetricsChart data={metrics ?? []} />
          <MapPanel initialPoints={livePoints ?? []} liveEvents={liveEvents} />
          <DlqList data={dlq ?? []} refresh={refreshDlq} />
          <LiveStream events={liveEvents} connected={wsConnected} />
        </div>
      </div>
    </ConfigProvider>
  );
}

export default App;
