package com.example.gis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 把 DlqEvent 写入 cdc_dlq 表。
 *
 * <p>raw_payload 列是 jsonb，用 PGobject 显式标 type=jsonb 避免 PG 拒收文本。
 *
 * <p>本 sink 自身的失败（连不上目标库 / SQL 异常）不再吞，
 * 让上游 task 失败 → 重启 → 从 checkpoint 恢复。
 */
public class DlqJdbcSink extends RichSinkFunction<DlqEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DlqJdbcSink.class);

    private static final String INSERT_SQL =
        "INSERT INTO cdc_dlq (src_id, op, error_class, error_message, raw_payload) " +
        "VALUES (?, ?, ?, ?, ?::jsonb)";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private transient Connection conn;
    private transient PreparedStatement stmt;

    public DlqJdbcSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        conn.setAutoCommit(true);
        stmt = conn.prepareStatement(INSERT_SQL);
        LOG.info("DlqJdbcSink connected to {}", jdbcUrl);
    }

    @Override
    public void invoke(DlqEvent ev, Context context) throws Exception {
        if (ev.srcId == null) stmt.setNull(1, java.sql.Types.INTEGER);
        else stmt.setInt(1, ev.srcId);
        stmt.setString(2, ev.op);
        stmt.setString(3, ev.errorClass);
        stmt.setString(4, ev.errorMessage);

        PGobject payload = new PGobject();
        payload.setType("jsonb");
        payload.setValue(ev.rawPayloadJson != null ? ev.rawPayloadJson : "{}");
        stmt.setObject(5, payload);

        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        try { if (stmt != null) stmt.close(); } catch (Exception ignore) {}
        try { if (conn != null) conn.close(); } catch (Exception ignore) {}
    }
}
