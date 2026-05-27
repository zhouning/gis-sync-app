package com.example.gis.source;

import com.example.gis.common.EnvUtils;
import com.example.gis.common.FlinkEnvConfigurer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Source Job：把 PostgreSQL CDC 变更事件以 Debezium-Avro 格式写入 Kafka。
 *
 * <p>设计要点：
 * <ul>
 *   <li><b>纯 Flink SQL 实现</b>：postgres-cdc source 直连 PG WAL，
 *       通过 kafka connector + debezium-avro-confluent 格式落 Kafka。
 *       Avro schema 由 Flink 自动构造并注册到 Confluent Schema Registry。</li>
 *   <li><b>几何字段走 EWKT 文本</b>：源表用 TRIGGER 维护 geom_ewkt TEXT 列，
 *       CDC 看到的是稳定字符串，下游 Sink Job 用 Sedona ST_GeomFromEWKT 解析。
 *       回避了 PostGIS WKB struct 在 Flink SQL 里需要 ROW 嵌套的麻烦。</li>
 *   <li><b>职责单一</b>：本 Job 只做 PG → Kafka 的搬运，不做坐标转换。
 *       好处：Source 端只关心 CDC 进度落 Kafka 即视为成功；Sink 端可以
 *       独立扩缩容、重启、调整转换逻辑而不影响 CDC 复制槽。</li>
 * </ul>
 *
 * <p>所有外部连接通过环境变量配置：
 * <pre>
 *   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
 *   POSTGRES_DB, POSTGRES_SCHEMA, POSTGRES_TABLE,
 *   POSTGRES_SLOT, POSTGRES_PUBLICATION
 *   KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
 *   SCHEMA_REGISTRY_URL
 * </pre>
 */
public class SourceCdcToKafkaJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvConfigurer.configure(env);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // ============== 1. CDC 源表 DDL ==============
        // 直接从 PG WAL 拉取，无需 Kafka Connect 集群
        // 注意：列顺序与源表一致；几何走 geom_ewkt 文本而不是 geom 二进制
        String pgHost      = EnvUtils.get("POSTGRES_HOST", "postgis-src");
        String pgPort      = EnvUtils.get("POSTGRES_PORT", "5432");
        String pgUser      = EnvUtils.get("POSTGRES_USER", "postgres");
        String pgPassword  = EnvUtils.get("POSTGRES_PASSWORD", "postgres");
        String pgDb        = EnvUtils.get("POSTGRES_DB", "geodb_src");
        String pgSchema    = EnvUtils.get("POSTGRES_SCHEMA", "public");
        String pgTable     = EnvUtils.get("POSTGRES_TABLE", "spatial_data");
        String pgSlot      = EnvUtils.get("POSTGRES_SLOT", "gis_sync_slot");
        String pgPublic    = EnvUtils.get("POSTGRES_PUBLICATION", "gis_pub");

        String sourceDdl =
            "CREATE TABLE source_spatial_data ( " +
            "    id INT, " +
            "    name STRING, " +
            "    update_time TIMESTAMP(3), " +
            "    geom_ewkt STRING, " +
            "    PRIMARY KEY (id) NOT ENFORCED " +
            ") WITH ( " +
            "    'connector' = 'postgres-cdc', " +
            "    'hostname' = '" + pgHost + "', " +
            "    'port' = '" + pgPort + "', " +
            "    'username' = '" + pgUser + "', " +
            "    'password' = '" + pgPassword + "', " +
            "    'database-name' = '" + pgDb + "', " +
            "    'schema-name' = '" + pgSchema + "', " +
            "    'table-name' = '" + pgTable + "', " +
            "    'slot.name' = '" + pgSlot + "', " +
            "    'decoding.plugin.name' = 'pgoutput', " +
            // 显式 publication，避免运行时尝试 FOR ALL TABLES（需要超级用户）
            "    'debezium.publication.name' = '" + pgPublic + "', " +
            // 启动模式 initial：先快照后流式（默认行为，写出来更明确）
            "    'scan.startup.mode' = 'initial' " +
            ")";
        tEnv.executeSql(sourceDdl);

        // ============== 2. Kafka Sink DDL ==============
        // 使用 debezium-avro-confluent 格式：
        //   - 输出消息封装成 Debezium 标准 envelope（before/after/op/source 等）
        //   - 兼容下游任何 Debezium consumer
        //   - Avro schema 自动注册到 Confluent Schema Registry
        String kafkaBs     = EnvUtils.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094");
        String kafkaTopic  = EnvUtils.get("KAFKA_TOPIC", "spatial-data-cdc");
        String srUrl       = EnvUtils.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        String sinkDdl =
            "CREATE TABLE sink_kafka_cdc ( " +
            "    id INT, " +
            "    name STRING, " +
            "    update_time TIMESTAMP(3), " +
            "    geom_ewkt STRING, " +
            "    PRIMARY KEY (id) NOT ENFORCED " +
            ") WITH ( " +
            "    'connector' = 'kafka', " +
            "    'topic' = '" + kafkaTopic + "', " +
            "    'properties.bootstrap.servers' = '" + kafkaBs + "', " +
            "    'properties.transaction.timeout.ms' = '900000', " +
            // key：avro-confluent 编码 PK，便于 Kafka 按 key 分区/压缩
            "    'key.format' = 'avro-confluent', " +
            "    'key.avro-confluent.url' = '" + srUrl + "', " +
            "    'key.fields' = 'id', " +
            // value：debezium-avro-confluent 输出 Debezium envelope
            "    'value.format' = 'debezium-avro-confluent', " +
            "    'value.debezium-avro-confluent.url' = '" + srUrl + "', " +
            "    'value.fields-include' = 'ALL', " +
            // 投递语义：sink at-least-once + key 幂等 ≈ EOS 等价
            "    'sink.delivery-guarantee' = 'at-least-once', " +
            "    'sink.partitioner' = 'fixed' " +
            ")";
        tEnv.executeSql(sinkDdl);

        // ============== 3. 启动同步 ==============
        // executeInsert 阻塞返回 TableResult，detached 模式下立刻提交
        tEnv.executeSql("INSERT INTO sink_kafka_cdc SELECT id, name, update_time, geom_ewkt FROM source_spatial_data");

        // SQL Job 提交后由 Flink runtime 接管，main 方法可结束
    }
}
