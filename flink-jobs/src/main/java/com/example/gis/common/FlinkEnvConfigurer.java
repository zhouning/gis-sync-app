package com.example.gis.common;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 集中配置 Flink 流环境的生产级参数：checkpoint、重启策略、并行度。
 *
 * <p>Job 类在 main() 起头调用 {@link #configure(StreamExecutionEnvironment)}
 * 即可获得统一的容错语义，避免每个 Job 自己写一套（容易飘）。
 *
 * <p>实际部署时多数参数已在 docker-compose 的 FLINK_PROPERTIES 注入，
 * 这里再次显式配置一次是为了：
 * <ul>
 *   <li>本地 IDE 直跑（mini cluster）时也具备相同的语义</li>
 *   <li>明确表达 Job 期望的容错保证，便于代码 review</li>
 * </ul>
 */
public final class FlinkEnvConfigurer {

    private FlinkEnvConfigurer() {}

    public static void configure(StreamExecutionEnvironment env) {
        // Checkpoint：每分钟一次，EXACTLY_ONCE
        env.enableCheckpointing(60_000);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        cp.setMinPauseBetweenCheckpoints(30_000);
        cp.setCheckpointTimeout(600_000);              // 大快照场景留 10 分钟
        cp.setTolerableCheckpointFailureNumber(3);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        int parallelism = EnvUtils.getInt("FLINK_PARALLELISM", 1);
        env.setParallelism(parallelism);
    }
}
