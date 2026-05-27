package com.example.gis.sink;

import java.sql.Timestamp;

/**
 * 一个时间窗口内的同步指标快照。
 * 直接对应 sync_metrics 表的字段。
 */
public final class SyncMetricsRecord {

    public final Timestamp windowStart;
    public final Timestamp windowEnd;
    public final String jobName;
    public final long recordsIn;     // 窗口内进入 transform 的记录数
    public final long recordsOut;    // 窗口内成功落入主流（写目标库）的记录数
    public final long recordsDlq;    // 窗口内进入 DLQ 的记录数
    public final long p50LatencyMs;  // 端到端延迟（src.update_time → 处理时刻）
    public final long p99LatencyMs;

    public SyncMetricsRecord(Timestamp windowStart, Timestamp windowEnd, String jobName,
                             long recordsIn, long recordsOut, long recordsDlq,
                             long p50LatencyMs, long p99LatencyMs) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.jobName = jobName;
        this.recordsIn = recordsIn;
        this.recordsOut = recordsOut;
        this.recordsDlq = recordsDlq;
        this.p50LatencyMs = p50LatencyMs;
        this.p99LatencyMs = p99LatencyMs;
    }
}
