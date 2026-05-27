import { useEffect, useRef, useState } from 'react';
import type { CdcWsEvent } from '../types/api';

/**
 * 维持一个 WebSocket 连接到 /ws/cdc，自动重连。
 * 返回最近 N 条事件（环形缓冲）。
 */
export function useCdcStream(maxBuffer = 100) {
  const [events, setEvents] = useState<CdcWsEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${proto}://${window.location.host}/ws/cdc`;

    const connect = () => {
      if (cancelled) return;
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!cancelled) {
          reconnectTimer.current = window.setTimeout(connect, 2000);
        }
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (msg) => {
        try {
          const ev = JSON.parse(msg.data) as CdcWsEvent;
          setEvents((prev) => {
            const next = [ev, ...prev];
            if (next.length > maxBuffer) next.length = maxBuffer;
            return next;
          });
        } catch (e) {
          console.warn('parse ws message failed', e);
        }
      };
    };

    connect();
    return () => {
      cancelled = true;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [maxBuffer]);

  return { events, connected };
}
