package com.example.gis.sink;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 业务指标聚合：在一个时间窗口内累积入流/出流/DLQ 计数 + 延迟样本，
 * 窗口关闭时输出 SyncMetricsRecord。
 *
 * <p>P50/P99 用 List 直接排序计算。窗口跨度 10s，单作业事件数级别 ≤ 万，
 * 排序成本可忽略，不需要 t-digest 类近似算法。
 */
public class MetricsAggregator
        implements AggregateFunction<MetricEvent, MetricsAggregator.Acc, MetricsAggregator.Result> {

    /** 累积器：内部状态，必须可序列化（Flink 默认 Kryo）。 */
    public static class Acc {
        public long recordsIn = 0;
        public long recordsOut = 0;
        public long recordsDlq = 0;
        public final List<Long> latenciesMs = new ArrayList<>(256);
    }

    /** 输出：聚合后的窗口结果。windowStart/End 由 ProcessWindowFunction 注入，所以这里不带。 */
    public static class Result {
        public long recordsIn;
        public long recordsOut;
        public long recordsDlq;
        public long p50LatencyMs;
        public long p99LatencyMs;

        public Result(long in, long out, long dlq, long p50, long p99) {
            this.recordsIn = in;
            this.recordsOut = out;
            this.recordsDlq = dlq;
            this.p50LatencyMs = p50;
            this.p99LatencyMs = p99;
        }
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(MetricEvent ev, Acc acc) {
        acc.recordsIn++;
        if (ev.isDlq) {
            acc.recordsDlq++;
        } else {
            acc.recordsOut++;
        }
        // DLQ 事件没有可信的 src 时间戳（解析就失败了），跳过延迟统计
        if (!ev.isDlq && ev.latencyMs >= 0) {
            acc.latenciesMs.add(ev.latencyMs);
        }
        return acc;
    }

    @Override
    public Result getResult(Acc acc) {
        return new Result(
                acc.recordsIn,
                acc.recordsOut,
                acc.recordsDlq,
                percentile(acc.latenciesMs, 50),
                percentile(acc.latenciesMs, 99));
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        Acc r = new Acc();
        r.recordsIn = a.recordsIn + b.recordsIn;
        r.recordsOut = a.recordsOut + b.recordsOut;
        r.recordsDlq = a.recordsDlq + b.recordsDlq;
        r.latenciesMs.addAll(a.latenciesMs);
        r.latenciesMs.addAll(b.latenciesMs);
        return r;
    }

    private static long percentile(List<Long> samples, int p) {
        if (samples.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        int idx = (int) Math.ceil((p / 100.0) * sorted.size()) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted.get(idx);
    }
}
