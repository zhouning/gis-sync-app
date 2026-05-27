package com.example.gis.backend.kafka;

import com.example.gis.backend.config.GisProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * 监听 spatial-data-cdc topic，把每条 CDC 事件序列化成精简 JSON 后广播给前端。
 *
 * <p>不直接转发 Avro envelope —— 那个嵌套结构（before/after/op + Avro union 包裹）
 * 前端处理起来很别扭。这里抽取关键字段，输出一个扁平 JSON：
 * <pre>
 * { "id":1, "name":"...", "op":"c", "geom_ewkt":"SRID=4326;POINT(...)",
 *   "src_update_time": 17xx, "received_at": 17xx }
 * </pre>
 */
@Component
public class CdcKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CdcKafkaConsumer.class);

    private final CdcBroadcaster broadcaster;
    private final String cdcTopic;

    public CdcKafkaConsumer(CdcBroadcaster broadcaster, GisProperties props) {
        this.broadcaster = broadcaster;
        this.cdcTopic = props.getKafka().getCdcTopic();
    }

    @KafkaListener(
        topics = "${gis.kafka.cdc-topic}",
        containerFactory = "cdcKafkaListenerFactory")
    public void onMessage(GenericRecord envelope) {
        if (broadcaster.sessionCount() == 0) {
            return; // 没有前端连接就不做无用功
        }
        try {
            String json = toFrontendJson(envelope);
            broadcaster.broadcast(json);
        } catch (Exception e) {
            LOG.warn("Failed to broadcast CDC event: {}", e.toString());
        }
    }

    private static String toFrontendJson(GenericRecord envelope) {
        String op = stringOf(envelope.get("op"));
        boolean isDelete = "d".equals(op);
        GenericData.Record row = (GenericData.Record) envelope.get(isDelete ? "before" : "after");
        if (row == null) row = (GenericData.Record) envelope.get(isDelete ? "after" : "before");

        Integer id = row != null ? (Integer) row.get("id") : null;
        String name = row != null ? stringOf(row.get("name")) : null;
        String ewkt = row != null ? stringOf(row.get("geom_ewkt")) : null;
        Long srcTs = row != null ? (Long) row.get("update_time") : null;

        StringBuilder sb = new StringBuilder(192);
        sb.append('{')
          .append("\"id\":").append(id == null ? "null" : id)
          .append(",\"op\":").append(jsonString(op))
          .append(",\"name\":").append(jsonString(name))
          .append(",\"geom_ewkt\":").append(jsonString(ewkt))
          .append(",\"src_update_time\":").append(srcTs == null ? "null" : srcTs)
          .append(",\"received_at\":").append(Instant.now().toEpochMilli())
          .append('}');
        return sb.toString();
    }

    private static String stringOf(Object v) { return v == null ? null : v.toString(); }

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
