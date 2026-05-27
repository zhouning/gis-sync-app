package com.example.gis.sink;

/**
 * 喂给 MetricsAggregator 的轻量事件：
 * <ul>
 *   <li>主流成功 → MetricEvent(isDlq=false, latencyMs=now-srcUpdateTime)</li>
 *   <li>DLQ      → MetricEvent(isDlq=true,  latencyMs=-1)</li>
 * </ul>
 */
public final class MetricEvent {
    public final boolean isDlq;
    public final long latencyMs;

    public MetricEvent(boolean isDlq, long latencyMs) {
        this.isDlq = isDlq;
        this.latencyMs = latencyMs;
    }

    public static MetricEvent success(long srcUpdateTimeMs) {
        long now = System.currentTimeMillis();
        return new MetricEvent(false, Math.max(0, now - srcUpdateTimeMs));
    }

    public static MetricEvent dlq() {
        return new MetricEvent(true, -1);
    }
}
