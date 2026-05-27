package com.example.gis.it;

import com.example.gis.sink.KafkaToSinkJob;
import com.example.gis.source.SourceCdcToKafkaJob;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 端到端集成测试：PG_src → Source Job → Kafka → Sink Job → PG_dst。
 *
 * <p>用 Testcontainers 起真实的 PG/Kafka/SR，Flink Job 跑在 test JVM 内嵌 mini-cluster。
 * Source Job 提交后非阻塞返回（executeSql 模式），Sink Job 用 daemon 线程包装。
 *
 * <p>测试覆盖：
 * <ul>
 *   <li>{@link #snapshot_then_streaming_inserts_propagate} — 快照已有数据 + 实时 INSERT 都同步</li>
 *   <li>{@link #invalid_geom_goes_to_dlq} — 坏数据进 cdc_dlq，主流不阻塞</li>
 * </ul>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)   // 单测兜底超时
class EndToEndIT {

    private static Thread sinkThread;

    @BeforeAll
    static void startEnv() {
        TestEnvironment.startContainers();
        configureSystemProperties();
    }

    @AfterAll
    static void stopEnv() {
        if (sinkThread != null) sinkThread.interrupt();
        TestEnvironment.stopContainers();
    }

    @BeforeEach
    void cleanState() throws Exception {
        // 删 slot 让 Source Job 重新做 snapshot
        try (Connection c = TestEnvironment.connectSrc(); Statement s = c.createStatement()) {
            s.execute("SELECT pg_drop_replication_slot('" + TestEnvironment.SLOT_NAME + "') " +
                "WHERE EXISTS (SELECT 1 FROM pg_replication_slots " +
                "  WHERE slot_name='" + TestEnvironment.SLOT_NAME + "')");
        } catch (Exception ignored) { /* 第一次跑还没 slot 是正常的 */ }
        TestEnvironment.truncateAll();
    }

    @AfterEach
    void stopJobs() {
        // Job 通过 mini-cluster 自动结束（test JVM 进程级）；
        // 实际生产是 daemon thread，进程退出会自动清理
    }

    /** Source/Sink Job 启动前注入测试容器的连接信息。 */
    private static void configureSystemProperties() {
        // PG src（postgres-cdc 直连，从 host JVM 连到 testcontainer 暴露端口）
        String srcUrl = TestEnvironment.srcJdbcUrl();
        String srcHostPort = parseHostPortFromJdbc(srcUrl);
        System.setProperty("POSTGRES_HOST", srcHostPort.split(":")[0]);
        System.setProperty("POSTGRES_PORT", srcHostPort.split(":")[1]);
        System.setProperty("POSTGRES_USER", TestEnvironment.PG_USER);
        System.setProperty("POSTGRES_PASSWORD", TestEnvironment.PG_PASS);
        System.setProperty("POSTGRES_DB", TestEnvironment.SRC_DB);
        System.setProperty("POSTGRES_TABLE", "spatial_data");
        System.setProperty("POSTGRES_SLOT", TestEnvironment.SLOT_NAME);
        System.setProperty("POSTGRES_PUBLICATION", TestEnvironment.PUBLICATION);

        // Kafka & SR 都从 host JVM 连
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", TestEnvironment.kafkaBootstrap());
        System.setProperty("SCHEMA_REGISTRY_URL", TestEnvironment.schemaRegistryUrl());
        System.setProperty("KAFKA_TOPIC", TestEnvironment.CDC_TOPIC);
        System.setProperty("KAFKA_DLQ_TOPIC", TestEnvironment.DLQ_TOPIC);

        // PG dst（Sink Job JDBC sink 连这里）
        String dstUrl = TestEnvironment.dstJdbcUrl();
        String dstHostPort = parseHostPortFromJdbc(dstUrl);
        System.setProperty("POSTGRES_DST_HOST", dstHostPort.split(":")[0]);
        System.setProperty("POSTGRES_DST_PORT", dstHostPort.split(":")[1]);
        System.setProperty("POSTGRES_DST_DB", TestEnvironment.DST_DB);
        System.setProperty("POSTGRES_DST_USER", TestEnvironment.PG_USER);
        System.setProperty("POSTGRES_DST_PASSWORD", TestEnvironment.PG_PASS);

        // 测试里 mini-cluster 用 1 并行度，避免抢 slot
        System.setProperty("FLINK_PARALLELISM", "1");
    }

    /** 从 jdbc:postgresql://localhost:32789/foo?... 抠出 host:port。 */
    private static String parseHostPortFromJdbc(String url) {
        // jdbc:postgresql://HOST:PORT/...
        int s = url.indexOf("//");
        int e = url.indexOf('/', s + 2);
        return url.substring(s + 2, e);
    }

    @Test
    void snapshot_then_streaming_inserts_propagate() throws Exception {
        // 1) 先放一条数据进 src（Source Job 快照阶段会读到它）
        insertRow(1, "天安门", 116.404, 39.915);

        // 2) 启动两个 Job
        startJobs();

        // 3) 等快照那一条出现在 dst
        waitForRowInDst(1, Duration.ofSeconds(60));
        assertRowConverted(1, "天安门", 116.404, 39.915);

        // 4) 实时 INSERT 一条新数据，验证流式阶段
        insertRow(2, "上海外滩", 121.473, 31.230);
        waitForRowInDst(2, Duration.ofSeconds(30));
        assertRowConverted(2, "上海外滩", 121.473, 31.230);

        // 5) 同 id UPDATE，验证 changelog 路径
        updateGeom(1, 116.405, 39.916);
        waitForGeomMatchInDst(1, 116.405, 39.916, Duration.ofSeconds(30));
    }

    @Test
    void invalid_geom_goes_to_dlq() throws Exception {
        startJobs();

        // 插一条好数据 + 一条坏数据（geom_ewkt 直接覆盖成非法字符串）
        insertRow(10, "正常", 113.264, 23.129);
        try (Connection c = TestEnvironment.connectSrc(); Statement s = c.createStatement()) {
            s.execute("INSERT INTO spatial_data (id, name, geom) VALUES " +
                "(11, '坏数据', ST_SetSRID(ST_MakePoint(110, 30), 4326))");
            s.execute("ALTER TABLE spatial_data DISABLE TRIGGER spatial_data_set_ewkt");
            s.execute("UPDATE spatial_data SET geom_ewkt = 'GARBAGE' WHERE id = 11");
            s.execute("ALTER TABLE spatial_data ENABLE TRIGGER spatial_data_set_ewkt");
        }

        // 好数据应到目标库，坏数据应到 DLQ
        waitForRowInDst(10, Duration.ofSeconds(60));
        waitForDlqEntry(11, Duration.ofSeconds(30));

        try (Connection c = TestEnvironment.connectDst();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT count(*) FROM spatial_data_xfm WHERE id = 11");
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).as("坏数据不应进入主目标表").isZero();
        }
    }

    // ============== 启动 Job 的工具方法 ==============

    private void startJobs() throws Exception {
        // Source Job 用 SQL executeSql，提交后 main 立即返回（detached），
        // 内部 mini-cluster 会异步跑作业。
        SourceCdcToKafkaJob.main(new String[]{});

        // Sink Job 调 env.execute()，会阻塞，包到 daemon 线程
        if (sinkThread == null || !sinkThread.isAlive()) {
            sinkThread = new Thread(() -> {
                try { KafkaToSinkJob.main(new String[]{}); }
                catch (InterruptedException ignored) {}
                catch (Exception e) { e.printStackTrace(); }
            }, "sink-job-thread");
            sinkThread.setDaemon(true);
            sinkThread.start();
        }
    }

    // ============== 数据库辅助 ==============

    private void insertRow(int id, String name, double lon, double lat) throws Exception {
        try (Connection c = TestEnvironment.connectSrc();
             PreparedStatement ps = c.prepareStatement(
                 "INSERT INTO spatial_data (id, name, geom) VALUES (?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326))")) {
            ps.setInt(1, id); ps.setString(2, name);
            ps.setDouble(3, lon); ps.setDouble(4, lat);
            ps.executeUpdate();
        }
    }

    private void updateGeom(int id, double lon, double lat) throws Exception {
        try (Connection c = TestEnvironment.connectSrc();
             PreparedStatement ps = c.prepareStatement(
                 "UPDATE spatial_data SET geom = ST_SetSRID(ST_MakePoint(?, ?), 4326) WHERE id = ?")) {
            ps.setDouble(1, lon); ps.setDouble(2, lat); ps.setInt(3, id);
            ps.executeUpdate();
        }
    }

    private void waitForRowInDst(int id, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection c = TestEnvironment.connectDst();
                 PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM spatial_data_xfm WHERE id = ?")) {
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) return;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("dst row id=" + id + " not appeared within " + timeout);
    }

    private void waitForGeomMatchInDst(int id, double lon, double lat, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection c = TestEnvironment.connectDst();
                 PreparedStatement ps = c.prepareStatement(
                     "SELECT ST_X(geom_4326), ST_Y(geom_4326) FROM spatial_data_xfm WHERE id = ?")) {
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && Math.abs(rs.getDouble(1) - lon) < 1e-6
                                  && Math.abs(rs.getDouble(2) - lat) < 1e-6) return;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("dst geom for id=" + id + " never matched (" + lon + "," + lat + ")");
    }

    private void waitForDlqEntry(int srcId, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection c = TestEnvironment.connectDst();
                 PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM cdc_dlq WHERE src_id = ?")) {
                ps.setInt(1, srcId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) return;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("dlq entry for src_id=" + srcId + " not appeared");
    }

    private void assertRowConverted(int id, String expectedName, double lon4326, double lat4326)
            throws Exception {
        try (Connection c = TestEnvironment.connectDst();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT name, ST_X(geom_4326), ST_Y(geom_4326), ST_X(geom_3857), ST_Y(geom_3857) " +
                 "  FROM spatial_data_xfm WHERE id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo(expectedName);
                assertThat(rs.getDouble(2)).isCloseTo(lon4326, org.assertj.core.data.Offset.offset(1e-6));
                assertThat(rs.getDouble(3)).isCloseTo(lat4326, org.assertj.core.data.Offset.offset(1e-6));
                // 3857 投影非零（具体值看 EPSG，断言落在合理范围即可）
                assertThat(rs.getDouble(4)).as("3857 X 应非零").isNotEqualTo(0.0);
                assertThat(rs.getDouble(5)).as("3857 Y 应非零").isNotEqualTo(0.0);
            }
        }
    }
}
