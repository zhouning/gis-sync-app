package com.example.gis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 把窗口聚合后的 SyncMetricsRecord 写入 sync_metrics 表。
 *
 * <p>主键 (window_start, job_name) + ON CONFLICT 保证幂等：
 * 若 task 重启从 checkpoint 恢复重发同窗口结果，会覆盖而不是重复。
 */
public class SyncMetricsJdbcSink extends RichSinkFunction<SyncMetricsRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncMetricsJdbcSink.class);

    private static final String UPSERT_SQL =
        "INSERT INTO sync_metrics " +
        "    (window_start, window_end, job_name, records_in, records_out, records_dlq, " +
        "     p50_latency_ms, p99_latency_ms) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
        "ON CONFLICT (window_start, job_name) DO UPDATE SET " +
        "    window_end = EXCLUDED.window_end, " +
        "    records_in = EXCLUDED.records_in, " +
        "    records_out = EXCLUDED.records_out, " +
        "    records_dlq = EXCLUDED.records_dlq, " +
        "    p50_latency_ms = EXCLUDED.p50_latency_ms, " +
        "    p99_latency_ms = EXCLUDED.p99_latency_ms";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private transient Connection conn;
    private transient PreparedStatement stmt;

    public SyncMetricsJdbcSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        conn.setAutoCommit(true);
        stmt = conn.prepareStatement(UPSERT_SQL);
        LOG.info("SyncMetricsJdbcSink connected to {}", jdbcUrl);
    }

    @Override
    public void invoke(SyncMetricsRecord r, Context context) throws Exception {
        stmt.setTimestamp(1, r.windowStart);
        stmt.setTimestamp(2, r.windowEnd);
        stmt.setString(3, r.jobName);
        stmt.setLong(4, r.recordsIn);
        stmt.setLong(5, r.recordsOut);
        stmt.setLong(6, r.recordsDlq);
        stmt.setLong(7, r.p50LatencyMs);
        stmt.setLong(8, r.p99LatencyMs);
        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        try { if (stmt != null) stmt.close(); } catch (Exception ignore) {}
        try { if (conn != null) conn.close(); } catch (Exception ignore) {}
    }
}
