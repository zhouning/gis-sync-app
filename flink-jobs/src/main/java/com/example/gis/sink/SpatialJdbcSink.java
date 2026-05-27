package com.example.gis.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * 把 SpatialChangeEvent 写入目标 PG 的 spatial_data_xfm 表。
 *
 * <p>语义：
 * <ul>
 *   <li>op == "d" → DELETE</li>
 *   <li>其他 → INSERT ... ON CONFLICT DO UPDATE（upsert）</li>
 * </ul>
 *
 * <p>几何字段通过 SQL 内 ST_GeomFromText 解析 WKT，避开 PreparedStatement
 * 不能直接传 JTS Geometry 的限制。
 *
 * <p>失败处理：直接抛出，让 task 失败 → Flink restart strategy 接手 →
 * 从最近 checkpoint（含 Kafka offset）回放。这是基础设施级故障的正确处理方式。
 */
public class SpatialJdbcSink extends RichSinkFunction<SpatialChangeEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SpatialJdbcSink.class);

    private static final String UPSERT_SQL =
        "INSERT INTO spatial_data_xfm " +
        "    (id, name, geom_4326, geom_3857, src_update_time, sync_time) " +
        "VALUES (?, ?, ST_GeomFromText(?, 4326), ST_GeomFromText(?, 3857), ?, CURRENT_TIMESTAMP) " +
        "ON CONFLICT (id) DO UPDATE SET " +
        "    name = EXCLUDED.name, " +
        "    geom_4326 = EXCLUDED.geom_4326, " +
        "    geom_3857 = EXCLUDED.geom_3857, " +
        "    src_update_time = EXCLUDED.src_update_time, " +
        "    sync_time = CURRENT_TIMESTAMP";

    private static final String DELETE_SQL = "DELETE FROM spatial_data_xfm WHERE id = ?";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private transient Connection conn;
    private transient PreparedStatement upsertStmt;
    private transient PreparedStatement deleteStmt;

    public SpatialJdbcSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        conn.setAutoCommit(true);
        upsertStmt = conn.prepareStatement(UPSERT_SQL);
        deleteStmt = conn.prepareStatement(DELETE_SQL);
        LOG.info("SpatialJdbcSink connected to {}", jdbcUrl);
    }

    @Override
    public void invoke(SpatialChangeEvent ev, Context context) throws Exception {
        if (ev.isDelete()) {
            deleteStmt.setInt(1, ev.id);
            deleteStmt.executeUpdate();
        } else {
            upsertStmt.setInt(1, ev.id);
            upsertStmt.setString(2, ev.name);
            upsertStmt.setString(3, ev.wkt4326);
            upsertStmt.setString(4, ev.wkt3857);
            upsertStmt.setTimestamp(5, new Timestamp(ev.srcUpdateTimeMs));
            upsertStmt.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        try { if (upsertStmt != null) upsertStmt.close(); } catch (Exception ignore) {}
        try { if (deleteStmt != null) deleteStmt.close(); } catch (Exception ignore) {}
        try { if (conn != null) conn.close(); } catch (Exception ignore) {}
    }
}
