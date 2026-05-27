import { Button, message, Switch, Table, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useState } from 'react';
import { api } from '../api/client';
import type { DlqEntry } from '../types/api';

interface Props {
  data: DlqEntry[];
  refresh: () => void;
}

export function DlqList({ data, refresh }: Props) {
  const [onlyUnreplayed, setOnlyUnreplayed] = useState(false);
  const [replayingId, setReplayingId] = useState<number | null>(null);

  const filtered = onlyUnreplayed ? data.filter((d) => !d.replayedAt) : data;

  async function handleReplay(id: number) {
    setReplayingId(id);
    try {
      const r = await api.dlqReplay(id);
      message.success(`重投成功：${r.delivery}`);
      refresh();
    } catch (e: unknown) {
      message.error(`重投失败：${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setReplayingId(null);
    }
  }

  const columns: ColumnsType<DlqEntry> = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    {
      title: '时间',
      dataIndex: 'occurredAt',
      width: 160,
      render: (t: string) => new Date(t).toLocaleString(),
    },
    { title: 'src_id', dataIndex: 'srcId', width: 70 },
    {
      title: 'op',
      dataIndex: 'op',
      width: 50,
      render: (op: string | null) => op ? <Tag>{op}</Tag> : '—',
    },
    {
      title: '错误',
      dataIndex: 'errorMessage',
      ellipsis: true,
      render: (msg: string, row: DlqEntry) => (
        <span title={`${row.errorClass}\n${msg}`}>
          <Tag color="red">{row.errorClass.split('.').pop()}</Tag>
          {msg}
        </span>
      ),
    },
    {
      title: '重投',
      width: 130,
      render: (_: unknown, row: DlqEntry) =>
        row.replayedAt ? (
          <Tag color="green">已重投 ({row.replayCount})</Tag>
        ) : (
          <Button
            size="small"
            type="primary"
            loading={replayingId === row.id}
            onClick={() => handleReplay(row.id)}
          >
            重投
          </Button>
        ),
    },
  ];

  return (
    <div className="panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <h3 className="panel-title" style={{ margin: 0 }}>死信队列 (DLQ)</h3>
        <span style={{ fontSize: 12 }}>
          只看未重投&nbsp;<Switch size="small" checked={onlyUnreplayed} onChange={setOnlyUnreplayed} />
        </span>
      </div>
      <div className="panel-body">
        <Table<DlqEntry>
          rowKey="id"
          columns={columns}
          dataSource={filtered}
          size="small"
          pagination={{ pageSize: 8, showSizeChanger: false }}
        />
      </div>
    </div>
  );
}
