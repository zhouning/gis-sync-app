package com.example.gis;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.sedona.flink.SedonaFlinkRegistrator;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Flink CDC 直连 PostGIS 示例 Job
 *
 * 架构优势（相比 Debezium + Kafka 独立部署方案）：
 * 1. 简化部署拓扑：无需独立维护 Kafka 集群和 Debezium Connector，
 *    Flink CDC 内嵌 Debezium 引擎，单个 Flink 作业即可完成 CDC 采集与处理。
 * 2. 降低运维成本：减少了 Kafka broker、ZooKeeper/KRaft、Debezium Connect Worker
 *    等组件的监控和运维负担。
 * 3. 端到端 Exactly-Once：Flink 的 checkpoint 机制天然保证从 CDC 读取到
 *    下游写入的端到端一致性，无需额外协调 Kafka 事务。
 * 4. 更低延迟：数据从 PostgreSQL WAL 直接进入 Flink 算子处理，
 *    省去了 Kafka 中间层的序列化/反序列化和网络开销。
 * 5. 统一计算引擎：CDC 采集、空间函数转换、数据写入均在 Flink SQL 中完成，
 *    便于统一管理和调试。
 */
public class GisStreamingJobCdc {
    public static void main(String[] args) throws Exception {
        // ========== 1. 配置 Flink 流处理环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生产级 Checkpoint 配置（与 GisStreamingJob 增强配置一致）
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint 超时时间 10 分钟，防止大状态场景下超时失败
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 两次 checkpoint 之间最小间隔 30 秒，避免频繁触发
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        // 最多允许 3 次 checkpoint 连续失败，超过则作业失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 作业取消时保留 checkpoint，便于恢复
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // ========== 2. 注册 Sedona 空间函数与序列化器 ==========
        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);

        // Kryo 序列化器注册：为 JTS Geometry 类型注册，减少序列化开销
        env.getConfig().registerKryoType(Point.class);
        env.getConfig().registerKryoType(LineString.class);
        env.getConfig().registerKryoType(Polygon.class);
        env.getConfig().registerKryoType(MultiPoint.class);
        env.getConfig().registerKryoType(MultiLineString.class);
        env.getConfig().registerKryoType(MultiPolygon.class);
        env.getConfig().registerKryoType(GeometryCollection.class);

        System.out.println("Sedona 空间函数与 Kryo 序列化器注册完成。");

        // ========== 3. 创建 postgres-cdc Source 表 ==========
        // 直接从 PostgreSQL WAL 日志捕获变更，无需 Kafka 中间层
        // slot.name: 指定复制槽名称，确保 WAL 日志不被回收
        // decoding.plugin.name: 使用 pgoutput（PostgreSQL 10+ 内置逻辑解码插件）
        String sourceDdl = "CREATE TEMPORARY TABLE cdc_spatial_data ( " +
                           "    id INT, " +
                           "    geom_wkt STRING, " +
                           "    update_time TIMESTAMP(3), " +
                           "    PRIMARY KEY (id) NOT ENFORCED " +
                           ") WITH ( " +
                           "    'connector' = 'postgres-cdc', " +
                           "    'hostname' = 'localhost', " +
                           "    'port' = '5432', " +
                           "    'database-name' = 'geodb', " +
                           "    'schema-name' = 'public', " +
                           "    'table-name' = 'spatial_data', " +
                           "    'slot.name' = 'gis_sync_slot', " +
                           "    'decoding.plugin.name' = 'pgoutput' " +
                           ")";

        tableEnv.executeSql(sourceDdl);

        // ========== 4. 空间坐标转换查询 ==========
        // 处理流程：WKT 文本 -> Geometry 对象 -> 设置 SRID 4326 (WGS84)
        //          -> 投影转换至 EPSG:3857 (Web Mercator) -> 输出 WKT 文本
        // WHERE 条件过滤无效几何（geom_wkt 为空或 ST_IsValid 校验失败的记录）
        String query = "SELECT " +
                       "    id, " +
                       "    geom_wkt AS original_wkt, " +
                       "    ST_AsText( " +
                       "        ST_Transform( " +
                       "            ST_SetSRID(ST_GeomFromWKT(geom_wkt), 4326), " +
                       "            'EPSG:4326', " +
                       "            'EPSG:3857' " +
                       "        ) " +
                       "    ) AS transformed_mercator, " +
                       "    update_time " +
                       "FROM cdc_spatial_data " +
                       "WHERE geom_wkt IS NOT NULL " +
                       "  AND ST_IsValid(ST_GeomFromWKT(geom_wkt)) = true";

        // ========== 5. JDBC Sink 表（写入目标 PostGIS）==========
        // 以下为注释形式的 sink DDL，展示如何将转换结果写回 PostGIS
        // 实际使用时取消注释并配置目标数据库连接信息
        /*
        String sinkDdl = "CREATE TEMPORARY TABLE sink_spatial_data ( " +
                         "    id INT, " +
                         "    original_wkt STRING, " +
                         "    transformed_mercator STRING, " +
                         "    update_time TIMESTAMP(3), " +
                         "    PRIMARY KEY (id) NOT ENFORCED " +
                         ") WITH ( " +
                         "    'connector' = 'jdbc', " +
                         "    'url' = 'jdbc:postgresql://localhost:5432/geodb', " +
                         "    'table-name' = 'spatial_data_transformed', " +
                         "    'username' = 'postgres', " +
                         "    'password' = 'postgres', " +
                         "    'driver' = 'org.postgresql.Driver' " +
                         ")";

        tableEnv.executeSql(sinkDdl);
        tableEnv.executeSql("INSERT INTO sink_spatial_data " + query);
        */

        // ========== 6. 控制台输出（开发调试用）==========
        Table resultTable = tableEnv.sqlQuery(query);
        resultTable.execute().print();
    }
}
