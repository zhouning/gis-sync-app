package com.example.gis.sink;

/**
 * Sink Job 流转中的业务事件：从 Kafka Avro 反序列化、Sedona 转换之后的形态。
 * 包含 4326 与 3857 两份几何（WKT 文本，便于 PreparedStatement 直接 setString）。
 *
 * <p>不可变值对象，字段直接 public final：内部 Job 间数据流转，没必要套 getter。
 */
public final class SpatialChangeEvent {

    /** Debezium 操作类型：c=create/insert, u=update, d=delete, r=read（snapshot 也归 c）。 */
    public final String op;

    public final int id;
    public final String name;
    public final long srcUpdateTimeMs;

    /** WGS84 (EPSG:4326) WKT，DELETE 事件时来自 before。 */
    public final String wkt4326;

    /** Web Mercator (EPSG:3857) WKT，DELETE 事件时为 null（不需要写目标）。 */
    public final String wkt3857;

    public SpatialChangeEvent(String op, int id, String name, long srcUpdateTimeMs,
                              String wkt4326, String wkt3857) {
        this.op = op;
        this.id = id;
        this.name = name;
        this.srcUpdateTimeMs = srcUpdateTimeMs;
        this.wkt4326 = wkt4326;
        this.wkt3857 = wkt3857;
    }

    public boolean isDelete() {
        return "d".equals(op);
    }
}
