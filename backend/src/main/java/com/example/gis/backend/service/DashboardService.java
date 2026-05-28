package com.example.gis.backend.service;

import com.example.gis.backend.model.DlqEntry;
import com.example.gis.backend.model.LivePoint;
import com.example.gis.backend.model.SyncMetricsPoint;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * 看板侧的所有"读"操作集中在这。
 * 所有查询都走目标库（@Primary 的 JdbcTemplate），sync_metrics / cdc_dlq / spatial_data_xfm 都在这。
 */
@Service
public class DashboardService {

    private final JdbcTemplate jdbc;

    public DashboardService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 查时序指标，from/to 为 epoch 毫秒。 */
    public List<SyncMetricsPoint> metrics(long fromMs, long toMs) {
        return jdbc.query(
            "SELECT window_start, window_end, job_name, records_in, records_out, records_dlq, " +
            "       p50_latency_ms, p99_latency_ms " +
            "  FROM sync_metrics " +
            " WHERE window_start BETWEEN ? AND ? " +
            " ORDER BY window_start",
            (rs, n) -> new SyncMetricsPoint(
                rs.getTimestamp("window_start").toInstant(),
                rs.getTimestamp("window_end").toInstant(),
                rs.getString("job_name"),
                rs.getLong("records_in"),
                rs.getLong("records_out"),
                rs.getLong("records_dlq"),
                rs.getLong("p50_latency_ms"),
                rs.getLong("p99_latency_ms")
            ),
            new Timestamp(fromMs), new Timestamp(toMs)
        );
    }

    public List<DlqEntry> listDlq(int limit, boolean onlyUnreplayed) {
        StringBuilder sb = new StringBuilder()
            .append("SELECT id, src_id, op, error_class, error_message, ")
            .append("       raw_payload::text AS raw_payload, ")
            .append("       occurred_at, replayed_at, replay_count ")
            .append("  FROM cdc_dlq ");
        if (onlyUnreplayed) sb.append("WHERE replayed_at IS NULL ");
        sb.append("ORDER BY occurred_at DESC LIMIT ?");

        return jdbc.query(sb.toString(),
            (rs, n) -> new DlqEntry(
                rs.getLong("id"),
                rs.getObject("src_id") == null ? null : rs.getInt("src_id"),
                rs.getString("op"),
                rs.getString("error_class"),
                rs.getString("error_message"),
                rs.getString("raw_payload"),
                rs.getTimestamp("occurred_at").toInstant(),
                rs.getTimestamp("replayed_at") != null ? rs.getTimestamp("replayed_at").toInstant() : null,
                rs.getInt("replay_count")
            ),
            limit);
    }

    /**
     * 取最近 N 条已同步成功的几何，用于地图打点初始加载。
     * 任意几何（含 Polygon）都用 ST_Centroid 转成代表点，前端按 lon/lat 画 marker。
     */
    public List<LivePoint> recentPoints(int limit) {
        return jdbc.query(
            "SELECT id, name, " +
            "       ST_X(ST_Centroid(geom_4326)) AS lon, " +
            "       ST_Y(ST_Centroid(geom_4326)) AS lat, " +
            "       sync_time " +
            "  FROM spatial_data_xfm " +
            " WHERE geom_4326 IS NOT NULL " +
            " ORDER BY sync_time DESC LIMIT ?",
            (rs, n) -> new LivePoint(
                rs.getInt("id"),
                rs.getString("name"),
                rs.getDouble("lon"),
                rs.getDouble("lat"),
                rs.getTimestamp("sync_time").toInstant()
            ),
            limit);
    }

    /** 标记 DLQ 条目为已重投。 */
    public void markReplayed(long dlqId) {
        jdbc.update(
            "UPDATE cdc_dlq SET replayed_at = ?, replay_count = replay_count + 1 WHERE id = ?",
            Timestamp.from(Instant.now()), dlqId);
    }

    /** 取一条 DLQ 的 raw_payload 用于重投。 */
    public String fetchRawPayload(long dlqId) {
        return jdbc.queryForObject(
            "SELECT raw_payload::text FROM cdc_dlq WHERE id = ?",
            String.class, dlqId);
    }
}
