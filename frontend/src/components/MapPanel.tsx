import maplibregl, { Map as MapLibreMap, Marker } from 'maplibre-gl';
import { useEffect, useRef } from 'react';
import type { CdcWsEvent, LivePoint } from '../types/api';

interface Props {
  initialPoints: LivePoint[];
  liveEvents: CdcWsEvent[];
}

/** 从 EWKT "SRID=4326;POINT(116.4 39.9)" 抽出 [lon, lat]。 */
function parseEwkt(ewkt: string | null): [number, number] | null {
  if (!ewkt) return null;
  const m = ewkt.match(/POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)/i);
  if (!m) return null;
  return [parseFloat(m[1]), parseFloat(m[2])];
}

export function MapPanel({ initialPoints, liveEvents }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  // 用 id 索引 marker，op=c 时 set/update，op=d 时 remove
  const markersRef = useRef<Map<number, Marker>>(new Map());

  // 初始化地图
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    mapRef.current = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          osm: {
            type: 'raster',
            tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '© OpenStreetMap',
          },
        },
        layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
      },
      center: [116.404, 39.915], // 北京
      zoom: 4,
    });
    mapRef.current.addControl(new maplibregl.NavigationControl(), 'top-right');
    return () => {
      mapRef.current?.remove();
      mapRef.current = null;
      markersRef.current.clear();
    };
  }, []);

  // 用初始点填充
  useEffect(() => {
    if (!mapRef.current) return;
    initialPoints.forEach((p) => putMarker(p.id, p.lon, p.lat, p.name, 'init'));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialPoints]);

  // 处理 WS 实时事件：只看 latest 一条还没处理的（events 是从新到旧）
  useEffect(() => {
    if (!mapRef.current || liveEvents.length === 0) return;
    const ev = liveEvents[0];
    if (ev.id == null) return;
    if (ev.op === 'd') {
      const m = markersRef.current.get(ev.id);
      if (m) {
        m.remove();
        markersRef.current.delete(ev.id);
      }
      return;
    }
    const ll = parseEwkt(ev.geom_ewkt);
    if (!ll) return;
    putMarker(ev.id, ll[0], ll[1], ev.name ?? `id=${ev.id}`, 'live');
    // 飞过去看看
    mapRef.current.easeTo({ center: ll, duration: 600 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveEvents]);

  function putMarker(id: number, lon: number, lat: number, label: string, source: 'init' | 'live') {
    const map = mapRef.current!;
    const existing = markersRef.current.get(id);
    if (existing) existing.remove();
    const el = document.createElement('div');
    el.style.width = '12px';
    el.style.height = '12px';
    el.style.borderRadius = '50%';
    el.style.background = source === 'live' ? '#ff4d4f' : '#1677ff';
    el.style.border = '2px solid white';
    el.style.boxShadow = '0 0 4px rgba(0,0,0,0.3)';
    const m = new maplibregl.Marker(el)
      .setLngLat([lon, lat])
      .setPopup(new maplibregl.Popup({ offset: 12 }).setText(`${label} (id=${id})`))
      .addTo(map);
    markersRef.current.set(id, m);
  }

  return (
    <div className="panel">
      <h3 className="panel-title">实时空间数据 (蓝=初始 / 红=刚同步)</h3>
      <div ref={containerRef} className="map-container" />
    </div>
  );
}
