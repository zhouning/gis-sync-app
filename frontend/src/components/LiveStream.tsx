import { Badge, List, Tag } from 'antd';
import type { CdcWsEvent } from '../types/api';

interface Props {
  events: CdcWsEvent[];
  connected: boolean;
}

export function LiveStream({ events, connected }: Props) {
  return (
    <div className="panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <h3 className="panel-title" style={{ margin: 0 }}>实时变更流 (WebSocket)</h3>
        <Badge
          status={connected ? 'success' : 'default'}
          text={connected ? '已连接' : '未连接'}
        />
      </div>
      <div className="panel-body">
        <List<CdcWsEvent>
          size="small"
          dataSource={events}
          locale={{ emptyText: connected ? '等待 CDC 事件...' : '未连接' }}
          renderItem={(ev, idx) => (
            <List.Item
              key={`${ev.id}-${ev.received_at}-${idx}`}
              style={{ padding: '6px 0', fontSize: 12 }}
            >
              <span style={{ color: '#999', minWidth: 80 }}>
                {new Date(ev.received_at).toLocaleTimeString()}
              </span>
              <Tag color={ev.op === 'd' ? 'red' : 'green'} style={{ marginLeft: 8 }}>
                {ev.op}
              </Tag>
              <span style={{ marginLeft: 8 }}>id={ev.id}</span>
              {ev.name && <span style={{ marginLeft: 8 }}>{ev.name}</span>}
              {ev.geom_ewkt && (
                <span style={{ marginLeft: 8, color: '#888', fontFamily: 'monospace' }}>
                  {ev.geom_ewkt}
                </span>
              )}
            </List.Item>
          )}
        />
      </div>
    </div>
  );
}
