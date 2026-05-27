import { Card, Col, Row, Statistic, Tag, Tooltip } from 'antd';
import { useMemo } from 'react';
import type { SyncMetricsPoint, SyncStatus } from '../types/api';

interface Props {
  status: SyncStatus | null;
  metrics: SyncMetricsPoint[] | null;
  unreplayed: number;
}

function formatBytes(b: number): string {
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KiB`;
  if (b < 1024 * 1024 * 1024) return `${(b / 1024 / 1024).toFixed(1)} MiB`;
  return `${(b / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}

export function KpiBar({ status, metrics, unreplayed }: Props) {
  // 复制槽：取第一个 slot；显示活跃状态 + WAL 滞后
  const slot = status?.slots?.[0];

  // 吞吐：最近 1 个窗口的 records_out / windowDuration（秒）
  const { tps, p99 } = useMemo(() => {
    const last = metrics?.[metrics.length - 1];
    if (!last) return { tps: 0, p99: 0 };
    const dur = (new Date(last.windowEnd).getTime() - new Date(last.windowStart).getTime()) / 1000;
    const r = dur > 0 ? last.recordsOut / dur : 0;
    return { tps: Number(r.toFixed(1)), p99: last.p99LatencyMs };
  }, [metrics]);

  // Flink 状态汇总
  const jobs = status?.flinkJobs ?? [];
  const running = jobs.filter((j) => j.state === 'RUNNING').length;

  // 槽 WAL 滞后阈值：>1GiB 黄，>5GiB 红（与 alert 规则一致）
  const slotWalSeverity =
    !slot ? 'default' : slot.walRetainedBytes > 5 * 2 ** 30 ? 'red'
    : slot.walRetainedBytes > 2 ** 30 ? 'orange' : 'green';

  return (
    <Row gutter={12} style={{ marginBottom: 12 }}>
      <Col flex={1}>
        <Card size="small">
          <Statistic
            title="同步吞吐 (条/秒)"
            value={tps}
            valueStyle={{ color: tps > 0 ? '#3f8600' : '#999' }}
          />
        </Card>
      </Col>
      <Col flex={1}>
        <Card size="small">
          <Statistic
            title="端到端 P99 延迟 (ms)"
            value={p99}
            valueStyle={{ color: p99 > 30000 ? '#cf1322' : p99 > 5000 ? '#faad14' : '#3f8600' }}
          />
        </Card>
      </Col>
      <Col flex={1}>
        <Card size="small">
          <Statistic
            title="复制槽 WAL 滞后"
            value={slot ? formatBytes(slot.walRetainedBytes) : '—'}
            prefix={
              slot ? (
                <Tooltip title={`slot=${slot.slotName}, plugin=${slot.plugin}, active=${slot.active}`}>
                  <Tag color={slotWalSeverity}>{slot.active ? 'ACTIVE' : 'INACTIVE'}</Tag>
                </Tooltip>
              ) : null
            }
          />
        </Card>
      </Col>
      <Col flex={1}>
        <Card size="small">
          <Statistic
            title="未重投的 DLQ"
            value={unreplayed}
            valueStyle={{ color: unreplayed > 0 ? '#faad14' : '#999' }}
          />
        </Card>
      </Col>
      <Col flex={1}>
        <Card size="small">
          <Statistic
            title="Flink RUNNING jobs"
            value={running}
            suffix={` / ${jobs.length}`}
          />
        </Card>
      </Col>
    </Row>
  );
}
