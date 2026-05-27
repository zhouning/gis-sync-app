import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { SyncMetricsPoint } from '../types/api';

interface Props {
  data: SyncMetricsPoint[];
}

export function MetricsChart({ data }: Props) {
  // recharts 需要扁平化 + 时间戳数字（用 windowStart 的小时:分钟做 X 轴 label）
  const chartData = data.map((d) => {
    const t = new Date(d.windowStart);
    return {
      ts: t.getTime(),
      label: `${t.getHours().toString().padStart(2, '0')}:${t.getMinutes().toString().padStart(2, '0')}:${t.getSeconds().toString().padStart(2, '0')}`,
      out: d.recordsOut,
      dlq: d.recordsDlq,
      p50: d.p50LatencyMs,
      p99: d.p99LatencyMs,
    };
  });

  return (
    <div className="panel">
      <h3 className="panel-title">吞吐 + 延迟 时序</h3>
      <div className="panel-body">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="label" tick={{ fontSize: 11 }} />
            <YAxis yAxisId="count" orientation="left" tick={{ fontSize: 11 }} />
            <YAxis yAxisId="lat" orientation="right" tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line yAxisId="count" type="monotone" dataKey="out" name="成功 / 窗口" stroke="#3f8600" strokeWidth={2} dot={false} />
            <Line yAxisId="count" type="monotone" dataKey="dlq" name="DLQ / 窗口" stroke="#cf1322" strokeWidth={2} dot={false} />
            <Line yAxisId="lat" type="monotone" dataKey="p99" name="P99 (ms)" stroke="#1677ff" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line yAxisId="lat" type="monotone" dataKey="p50" name="P50 (ms)" stroke="#69b1ff" strokeWidth={1.5} dot={false} strokeDasharray="2 2" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
