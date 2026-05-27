package com.example.gis.sink;

import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.FunctionsGeoTools;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

/**
 * 把 Kafka 里的 Debezium Avro envelope 解析成业务事件。
 *
 * <p>Avro 结构（来自 Source Job 注册的 schema）：
 * <pre>
 * {
 *   "before": null | {id, name, update_time, geom_ewkt},
 *   "after":  null | {id, name, update_time, geom_ewkt},
 *   "op":     "c" | "u" | "d"
 * }
 * </pre>
 *
 * <p>Source Job 那一段我们已经知道：Flink 把 UPDATE 拆成 op=d + op=c 两条，
 * 所以这里只需要处理 c/d 两种就够，u 在实测里不会出现，但兜底也能正确处理。
 *
 * <p>抛出的异常会被上游 ProcessFunction 捕获并转 DLQ。
 */
final class CdcEventParser {

    private static final WKTWriter WKT_WRITER = new WKTWriter();

    private CdcEventParser() {}

    static SpatialChangeEvent parse(GenericRecord envelope) {
        String op = stringOf(envelope.get("op"));
        if (op == null) throw new IllegalArgumentException("missing op field");

        // d 走 before，其余（c/r/u）走 after
        boolean useBefore = "d".equals(op);
        GenericRecord row = (GenericRecord) envelope.get(useBefore ? "before" : "after");
        if (row == null) {
            throw new IllegalArgumentException("op=" + op + " but " +
                (useBefore ? "before" : "after") + " is null");
        }

        int id = (Integer) row.get("id");
        String name = stringOf(row.get("name"));
        Long updateTimeMs = (Long) row.get("update_time");   // logicalType timestamp-millis
        String ewkt = stringOf(row.get("geom_ewkt"));

        if (ewkt == null || ewkt.isEmpty()) {
            throw new IllegalArgumentException("geom_ewkt is null/empty for id=" + id);
        }

        // 解析 EWKT 拿到 JTS Geometry（已带 SRID）
        Geometry g4326;
        try {
            g4326 = Constructors.geomFromEWKT(ewkt);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid EWKT: " + ewkt, e);
        }
        if (g4326 == null) {
            throw new IllegalArgumentException("Constructors.geomFromEWKT returned null: " + ewkt);
        }
        if (g4326.getSRID() != 4326) {
            throw new IllegalArgumentException("expected SRID=4326 but got " + g4326.getSRID());
        }

        String wkt4326 = WKT_WRITER.write(g4326);
        String wkt3857 = null;

        if (!useBefore) {
            // 仅非删除事件需要算 3857（删除时只要 id 即可）
            Geometry g3857;
            try {
                g3857 = FunctionsGeoTools.transform(g4326, "EPSG:4326", "EPSG:3857");
            } catch (Exception e) {
                throw new IllegalStateException("ST_Transform failed for " + wkt4326, e);
            }
            if (g3857 == null) {
                throw new IllegalStateException("ST_Transform returned null for " + wkt4326);
            }
            wkt3857 = WKT_WRITER.write(g3857);
        }

        long ts = updateTimeMs != null ? updateTimeMs : System.currentTimeMillis();
        return new SpatialChangeEvent(op, id, name, ts, wkt4326, wkt3857);
    }

    /**
     * 无侵入读 id：用于异常场景下尽量给 DLQ 填一个 src_id。
     * 失败就返回 null。
     */
    static Integer tryExtractId(GenericRecord envelope) {
        try {
            GenericRecord after = (GenericRecord) envelope.get("after");
            GenericRecord before = (GenericRecord) envelope.get("before");
            GenericRecord row = after != null ? after : before;
            return row == null ? null : (Integer) row.get("id");
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static String stringOf(Object v) {
        return v == null ? null : v.toString();   // CharSequence / Utf8 都能 toString
    }
}
