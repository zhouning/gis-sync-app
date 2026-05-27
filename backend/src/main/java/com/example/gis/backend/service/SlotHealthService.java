package com.example.gis.backend.service;

import com.example.gis.backend.model.SlotHealth;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;

/**
 * 复制槽监控：从源库 pg_replication_slots 视图查 WAL 滞后信息。
 * 业界 PostGIS CDC #1 事故源就是 slot 不被消费导致 WAL 堆积。
 *
 * <p>显式从 sourceDataSource 构造 JdbcTemplate，避开 Spring 在多 JdbcTemplate
 * 候选时按 @Primary 默认归一化的歧义。
 */
@Service
public class SlotHealthService {

    private final JdbcTemplate sourceJdbc;

    public SlotHealthService(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        this.sourceJdbc = new JdbcTemplate(sourceDataSource);
    }

    @PostConstruct
    void logBinding() {
        // 启动时打一行确认连的是源库
        Integer port = sourceJdbc.queryForObject(
            "SELECT inet_server_port()", Integer.class);
        String db = sourceJdbc.queryForObject("SELECT current_database()", String.class);
        System.out.println("[SlotHealthService] connected to db=" + db + " port=" + port);
    }

    public List<SlotHealth> list() {
        return sourceJdbc.query(
            "SELECT " +
            "    slot_name, " +
            "    plugin, " +
            "    coalesce(active, false) AS active, " +
            "    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS wal_retained_bytes, " +
            "    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS confirmed_flush_lag_bytes " +
            "FROM pg_replication_slots " +
            "ORDER BY slot_name",
            (rs, n) -> new SlotHealth(
                rs.getString("slot_name"),
                rs.getString("plugin"),
                rs.getBoolean("active"),
                rs.getLong("wal_retained_bytes"),
                rs.getLong("confirmed_flush_lag_bytes"))
        );
    }
}
