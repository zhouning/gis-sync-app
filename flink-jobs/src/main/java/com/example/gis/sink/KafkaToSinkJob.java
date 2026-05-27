package com.example.gis.sink;

import com.example.gis.common.EnvUtils;
import com.example.gis.common.FlinkEnvConfigurer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Sink Job：Kafka(Avro) → 校验/转换/分流 → 目标 PG + DLQ。
 *
 * <p>主流：spatial_data_xfm（含 4326 + 3857 双 SRID 几何）
 * <p>DLQ：双写 cdc_dlq 表（看板查询用） + spatial-data-dlq topic（程序化重投用）
 *
 * <p>注意：Source Job 那边把 UPDATE 拆成了 d+c 两条事件，
 * 所以这边只需要正确处理 c/d 即可，u 实测不会出现。
 */
public class KafkaToSinkJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvConfigurer.configure(env);

        // ============== 配置项 ==============
        String kafkaBs    = EnvUtils.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094");
        String srUrl      = EnvUtils.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        String srcTopic   = EnvUtils.get("KAFKA_TOPIC", "spatial-data-cdc");
        String dlqTopic   = EnvUtils.get("KAFKA_DLQ_TOPIC", "spatial-data-dlq");
        String groupId    = EnvUtils.get("KAFKA_GROUP_ID", "gis-sink-job");

        String dstHost     = EnvUtils.get("POSTGRES_DST_HOST", "postgis-dst");
        String dstPort     = EnvUtils.get("POSTGRES_DST_PORT", "5432");
        String dstDb       = EnvUtils.get("POSTGRES_DST_DB", "geodb_dst");
        String dstUser     = EnvUtils.get("POSTGRES_DST_USER", "postgres");
        String dstPassword = EnvUtils.get("POSTGRES_DST_PASSWORD", "postgres");
        String dstUrl      = "jdbc:postgresql://" + dstHost + ":" + dstPort + "/" + dstDb;

        // ============== Kafka Source ==============
        // 从 Schema Registry 拉取 spatial-data-cdc-value 的最新 schema 当 reader schema。
        // 实际反序列化时，Confluent format 会按消息头部的 schema id 自动拉对应 writer schema。
        Schema readerSchema = fetchLatestSchema(srUrl, srcTopic + "-value");

        KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
            .setBootstrapServers(kafkaBs)
            .setTopics(srcTopic)
            .setGroupId(groupId)
            // 业务上希望从最早消息读起，让运营能从历史回放（也由 checkpoint 接管 offset）
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, srUrl)
            )
            .build();

        DataStreamSource<GenericRecord> kafkaStream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "spatial-data-cdc-source");

        // ============== 转换 + 分流 ==============
        SingleOutputStreamOperator<SpatialChangeEvent> mainStream = kafkaStream
            .process(new TransformFunction())
            .name("transform-and-split")
            .uid("transform-and-split");

        DataStream<DlqEvent> dlqStream = mainStream.getSideOutput(TransformFunction.DLQ_TAG);

        // ============== 主流 sink：spatial_data_xfm ==============
        mainStream
            .addSink(new SpatialJdbcSink(dstUrl, dstUser, dstPassword))
            .name("spatial-data-xfm-sink")
            .uid("spatial-data-xfm-sink");

        // ============== DLQ sink #1：cdc_dlq 表 ==============
        dlqStream
            .addSink(new DlqJdbcSink(dstUrl, dstUser, dstPassword))
            .name("dlq-jdbc-sink")
            .uid("dlq-jdbc-sink");

        // ============== DLQ sink #2：spatial-data-dlq topic ==============
        // DLQ 消息格式选 JSON（不再走 Avro），因为消费者多是人/简单脚本，
        // schema registry 多一道也没收益。
        KafkaSink<String> dlqKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(kafkaBs)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(dlqTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        dlqStream
            .map(KafkaToSinkJob::toDlqJson)
            .name("dlq-to-json")
            .sinkTo(dlqKafkaSink)
            .name("dlq-kafka-sink")
            .uid("dlq-kafka-sink");

        // ============== 业务指标聚合 → sync_metrics 表 ==============
        // 把主流和 DLQ 流统一映射成 MetricEvent，按 jobName 分区，10 秒滚动窗口聚合。
        // 写入 PG 后由 postgres_exporter 暴露给 Prometheus，Grafana 业务面板读它。
        DataStream<MetricEvent> mainMetric = mainStream
            .map(ev -> MetricEvent.success(ev.srcUpdateTimeMs))
            .name("main-to-metric");
        DataStream<MetricEvent> dlqMetric = dlqStream
            .map(ev -> MetricEvent.dlq())
            .name("dlq-to-metric");

        final String jobName = "gis-sync";

        mainMetric.union(dlqMetric)
            .keyBy(ev -> jobName)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .aggregate(new MetricsAggregator(), new MetricsWindowFunction(jobName))
            .name("metrics-aggregate")
            .uid("metrics-aggregate")
            .addSink(new SyncMetricsJdbcSink(dstUrl, dstUser, dstPassword))
            .name("sync-metrics-sink")
            .uid("sync-metrics-sink");

        env.execute("KafkaToSinkJob");
    }

    /**
     * 从 Schema Registry 拉取 subject 最新 schema（HTTP REST）。
     * 不引 io.confluent SR client 依赖，避免 client 进程类加载冲突。
     *
     * <p>带重试：Sink Job 可能比 Source Job 先启动，那时 SR 还没注册过 schema
     * （Source Job 第一次写 Kafka 才会自动注册）。最长等 60 秒。
     */
    private static Schema fetchLatestSchema(String srUrl, String subject) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                return fetchSchemaOnce(srUrl, subject);
            } catch (SubjectNotReadyException e) {
                last = e;
                try { Thread.sleep(2000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for schema", ie);
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Cannot fetch schema " + subject + " from " + srUrl, e);
            }
        }
        throw new IllegalStateException(
            "Schema " + subject + " not registered to SR within 60s "
            + "(Source Job 还没启动？或没数据触发 schema 注册？)", last);
    }

    /** 单次调用：404 转成 SubjectNotReadyException 让外层重试，其他错直接抛。 */
    private static Schema fetchSchemaOnce(String srUrl, String subject) throws Exception {
        String endpoint = srUrl.replaceAll("/$", "") + "/subjects/" + subject + "/versions/latest";
        HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(15000);
        int code = conn.getResponseCode();
        if (code == 404) {
            throw new SubjectNotReadyException(subject);
        }
        if (code != 200) {
            throw new IllegalStateException("SR returned HTTP " + code + " for " + endpoint);
        }
        StringBuilder body = new StringBuilder();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) body.append(line);
        }
        // 极简提取 "schema":"..."（schema 字段是 JSON-escaped 字符串）
        String json = body.toString();
        int idx = json.indexOf("\"schema\":\"");
        if (idx < 0) throw new IllegalStateException("schema field missing in: " + json);
        int start = idx + "\"schema\":\"".length();
        int end = findUnescapedQuote(json, start);
        String escaped = json.substring(start, end);
        String schemaStr = unescapeJsonString(escaped);
        return new Schema.Parser().parse(schemaStr);
    }

    /** SR 返回 404：subject 还未注册。外层捕获后等待重试。 */
    private static class SubjectNotReadyException extends RuntimeException {
        SubjectNotReadyException(String subject) {
            super("subject not yet registered: " + subject);
        }
    }

    private static int findUnescapedQuote(String s, int from) {
        int i = from;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\') { i += 2; continue; }
            if (c == '"') return i;
            i++;
        }
        throw new IllegalStateException("unterminated string at " + from);
    }

    private static String unescapeJsonString(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(i + 1);
                switch (n) {
                    case '"':  sb.append('"');  i += 2; break;
                    case '\\': sb.append('\\'); i += 2; break;
                    case '/':  sb.append('/');  i += 2; break;
                    case 'n':  sb.append('\n'); i += 2; break;
                    case 'r':  sb.append('\r'); i += 2; break;
                    case 't':  sb.append('\t'); i += 2; break;
                    case 'u':
                        if (i + 5 >= s.length()) throw new IllegalStateException("bad \\u escape");
                        sb.append((char) Integer.parseInt(s.substring(i + 2, i + 6), 16));
                        i += 6;
                        break;
                    default: sb.append(n); i += 2;
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    private static String toDlqJson(DlqEvent ev) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"src_id\":").append(ev.srcId == null ? "null" : ev.srcId);
        sb.append(",\"op\":").append(jsonString(ev.op));
        sb.append(",\"error_class\":").append(jsonString(ev.errorClass));
        sb.append(",\"error_message\":").append(jsonString(ev.errorMessage));
        sb.append(",\"raw_payload\":").append(ev.rawPayloadJson != null ? ev.rawPayloadJson : "null");
        sb.append(",\"occurred_at\":\"").append(Instant.now().toString()).append("\"");
        sb.append("}");
        return sb.toString();
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder(s.length() + 8);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
