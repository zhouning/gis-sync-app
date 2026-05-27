package com.example.gis.sink;

/**
 * 死信记录：业务级失败的样本。
 * 与 cdc_dlq PG 表字段一一对应。
 */
public final class DlqEvent {

    public final Integer srcId;          // 可空，解析阶段失败时连 id 都没有
    public final String op;              // 可空
    public final String errorClass;      // 异常类全限定名
    public final String errorMessage;    // 异常 message（可能多行）
    public final String rawPayloadJson;  // 原始 Avro 解码后的 JSON 化字符串

    public DlqEvent(Integer srcId, String op, String errorClass, String errorMessage,
                    String rawPayloadJson) {
        this.srcId = srcId;
        this.op = op;
        this.errorClass = errorClass;
        this.errorMessage = errorMessage;
        this.rawPayloadJson = rawPayloadJson;
    }

    public static DlqEvent of(Integer srcId, String op, Throwable t, String rawPayloadJson) {
        String msg = t.getMessage();
        if (msg != null && msg.length() > 4000) msg = msg.substring(0, 4000);
        return new DlqEvent(srcId, op, t.getClass().getName(), msg, rawPayloadJson);
    }
}
