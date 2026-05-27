package com.example.gis.sink;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * 主转换算子：把 Avro envelope 解析成 SpatialChangeEvent，转换坐标。
 *
 * <p>主流：成功记录 → SpatialChangeEvent
 * <p>side output (DLQ_TAG)：单条记录失败 → DlqEvent
 *
 * <p>不在这里 catch 整个 task 级别的异常（如 Kafka 断、目标 PG 宕），
 * 那些应当让 Flink 的 restart strategy 处理，强行 catch 反而会丢数据。
 */
public class TransformFunction extends ProcessFunction<GenericRecord, SpatialChangeEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TransformFunction.class);
    public static final OutputTag<DlqEvent> DLQ_TAG = new OutputTag<DlqEvent>("dlq") {};

    @Override
    public void processElement(GenericRecord envelope,
                               Context ctx,
                               Collector<SpatialChangeEvent> out) {
        try {
            SpatialChangeEvent ev = CdcEventParser.parse(envelope);
            out.collect(ev);
        } catch (Throwable t) {
            // 业务级失败：转 DLQ，不阻塞流
            Integer srcId = CdcEventParser.tryExtractId(envelope);
            String op = safeString(envelope.get("op"));
            String rawJson = toJson(envelope);
            LOG.warn("Transform failed for id={} op={}: {}", srcId, op, t.toString());
            ctx.output(DLQ_TAG, DlqEvent.of(srcId, op, t, rawJson));
        }
    }

    private static String safeString(Object v) {
        return v == null ? null : v.toString();
    }

    private static String toJson(GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            new GenericDatumWriter<>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toString("UTF-8");
        } catch (Throwable t) {
            // toString 不能再失败，否则 DLQ 也写不进
            return "{\"_serializationError\":\"" + t.getMessage() + "\"}";
        }
    }
}
