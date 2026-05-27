package com.example.gis.sink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * MetricsAggregator 的输出 (Result) + 窗口元信息 (TimeWindow) → SyncMetricsRecord。
 *
 * <p>用 AggregateFunction 增量聚合 + ProcessWindowFunction 拿窗口元信息
 * 是 Flink 推荐组合：状态最小（只存 Acc），同时能拿到 window.getStart()/getEnd()。
 */
public class MetricsWindowFunction
        extends ProcessWindowFunction<MetricsAggregator.Result, SyncMetricsRecord, String, TimeWindow> {

    private final String jobName;

    public MetricsWindowFunction(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void process(String key, Context ctx,
                        Iterable<MetricsAggregator.Result> elements,
                        Collector<SyncMetricsRecord> out) {
        // AggregateFunction + ProcessWindowFunction 组合下 elements 必定只有一个
        MetricsAggregator.Result r = elements.iterator().next();
        TimeWindow w = ctx.window();
        out.collect(new SyncMetricsRecord(
                new Timestamp(w.getStart()),
                new Timestamp(w.getEnd()),
                jobName,
                r.recordsIn, r.recordsOut, r.recordsDlq,
                r.p50LatencyMs, r.p99LatencyMs));
    }
}
