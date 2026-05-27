package com.example.gis.backend.kafka;

import com.example.gis.backend.config.GisProperties;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

/**
 * 把一条 DLQ 记录的 raw_payload（JSON 文本）重投到 spatial-data-cdc topic。
 *
 * <p>raw_payload 是当初 GenericRecord 序列化为 JSON 的结果（Sink Job 失败时存的），
 * 这里反过来：JSON → GenericRecord → KafkaAvroSerializer → Kafka。
 *
 * <p>注意：重投只对"业务数据问题"有意义。如果失败原因是 schema 漂移、目标库挂了，
 * 重投也会再次失败，这是预期。
 */
@Service
public class DlqReplayer {

    private static final Logger LOG = LoggerFactory.getLogger(DlqReplayer.class);

    private final GisProperties props;
    private final String bootstrapServers;
    private KafkaProducer<GenericRecord, GenericRecord> producer;
    private Schema cachedSchema;
    private Schema cachedKeySchema;

    public DlqReplayer(GisProperties props,
                       @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.props = props;
        this.bootstrapServers = bootstrapServers;
    }

    @PostConstruct
    public void init() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        p.put("schema.registry.url", props.getKafka().getSchemaRegistryUrl());
        // 重投的 schema 已经在 SR 注册过（Source Job 注册的）
        // auto.register.schemas=false 让 producer 直接按 subject+latest 取 schema id，
        // 不再尝试重新注册（避免与已有 schema 不匹配）
        p.put("auto.register.schemas", false);
        p.put("use.latest.version", true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "gis-backend-dlq-replayer");
        producer = new KafkaProducer<>(p);
        LOG.info("DlqReplayer initialized → {}", bootstrapServers);
    }

    @PreDestroy
    public void close() {
        if (producer != null) producer.close();
    }

    /**
     * 把一条 raw_payload JSON 重新序列化为 Avro 并写回 spatial-data-cdc。
     * 返回投递的 partition+offset 信息（可选展示）。
     *
     * <p>raw_payload 是 Avro JSON 编码（含 union 标签 {"string":"x"}），但字段顺序
     * 不保证与 schema 一致——直接用 JsonDecoder 会因位置错位失败。
     * 这里走 Jackson → 按字段名取值 → 手动构造 GenericRecord。
     */
    public String replay(String rawPayloadJson) throws Exception {
        Schema valueSchema = getOrFetchSchema(props.getKafka().getCdcTopic() + "-value", true);
        GenericRecord value = parseFlexible(rawPayloadJson, valueSchema);

        // key 也是 record 类型（{id: int}），按 SR 里注册的 schema 构造
        Schema keySchema = getOrFetchSchema(props.getKafka().getCdcTopic() + "-key", false);
        GenericData.Record keyRecord = new GenericData.Record(keySchema);
        keyRecord.put("id", extractId(value));

        ProducerRecord<GenericRecord, GenericRecord> pr = new ProducerRecord<>(
            props.getKafka().getCdcTopic(), keyRecord, value);

        var meta = producer.send(pr).get();
        return String.format("topic=%s partition=%d offset=%d", meta.topic(), meta.partition(), meta.offset());
    }

    /**
     * 解析存储的 Avro JSON（字段顺序任意），按 schema 字段顺序构造 GenericRecord。
     */
    @SuppressWarnings("unchecked")
    private static GenericRecord parseFlexible(String json, Schema schema) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, Object> root = om.readValue(json, Map.class);
        // schema 可能是 union（[null, record]），取 record 分支
        Schema recordSchema = schema;
        if (recordSchema.getType() == Schema.Type.UNION) {
            recordSchema = recordSchema.getTypes().stream()
                .filter(s -> s.getType() == Schema.Type.RECORD)
                .findFirst().orElseThrow(() -> new IOException("no record branch in union"));
        }
        return buildRecord(recordSchema, root);
    }

    @SuppressWarnings("unchecked")
    private static GenericRecord buildRecord(Schema schema, Map<String, Object> data) {
        GenericData.Record r = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            Object v = data.get(f.name());
            r.put(f.name(), buildValue(f.schema(), v));
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    private static Object buildValue(Schema schema, Object value) {
        if (value == null) return null;
        switch (schema.getType()) {
            case UNION:
                if (value instanceof Map<?, ?> m && m.size() == 1) {
                    Map.Entry<String, Object> e = ((Map<String, Object>) m).entrySet().iterator().next();
                    String branchName = e.getKey();
                    Schema branch = schema.getTypes().stream()
                        .filter(s -> branchName.equals(s.getFullName())
                                  || branchName.equals(s.getName())
                                  || branchName.equalsIgnoreCase(s.getType().getName()))
                        .findFirst().orElse(null);
                    if (branch != null) return buildValue(branch, e.getValue());
                }
                // 其他情况：按非 null 分支推断
                Schema nonNull = schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst().orElse(null);
                return nonNull != null ? buildValue(nonNull, value) : value;
            case RECORD:
                return buildRecord(schema, (Map<String, Object>) value);
            case STRING:
                return value.toString();
            case INT:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return value;
            default:
                return value;
        }
    }

    private Schema getOrFetchSchema(String subject, boolean isValue) throws IOException {
        if (isValue && cachedSchema != null) return cachedSchema;
        if (!isValue && cachedKeySchema != null) return cachedKeySchema;
        String url = props.getKafka().getSchemaRegistryUrl().replaceAll("/$", "")
                + "/subjects/" + subject + "/versions/latest";
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
        try (var in = conn.getInputStream()) {
            String body = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            int idx = body.indexOf("\"schema\":\"");
            if (idx < 0) throw new IOException("schema field not found in SR response");
            int start = idx + "\"schema\":\"".length();
            int end = findUnescapedQuote(body, start);
            String schemaStr = unescape(body.substring(start, end));
            Schema parsed = new Schema.Parser().parse(schemaStr);
            if (isValue) cachedSchema = parsed; else cachedKeySchema = parsed;
            return parsed;
        }
    }

    private static Integer extractId(GenericRecord envelope) {
        Object after = envelope.get("after");
        Object before = envelope.get("before");
        Object row = after != null ? after : before;
        if (row instanceof GenericData.Record gr) {
            return (Integer) gr.get("id");
        }
        return null;
    }

    private static int findUnescapedQuote(String s, int from) {
        int i = from;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\') { i += 2; continue; }
            if (c == '"') return i;
            i++;
        }
        throw new IllegalStateException("unterminated string");
    }

    private static String unescape(String s) {
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
                        sb.append((char) Integer.parseInt(s.substring(i + 2, i + 6), 16));
                        i += 6; break;
                    default: sb.append(n); i += 2;
                }
            } else { sb.append(c); i++; }
        }
        return sb.toString();
    }
}
