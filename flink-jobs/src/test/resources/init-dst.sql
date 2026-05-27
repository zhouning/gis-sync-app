-- ==============================================================
-- 目标库：geodb_dst
-- 同步链路的下游，存储 Sedona 转换后的坐标 + 失败死信 + 同步指标
-- ==============================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ============== 1. 同步结果表 ==============
-- 与源表同样的 id 主键，几何字段使用 Web Mercator (EPSG:3857)
-- 目标侧业务方按需查询此表（地图前端通常直接消费 3857）
CREATE TABLE IF NOT EXISTS spatial_data_xfm (
    id              INT PRIMARY KEY,
    name            TEXT,
    geom_4326       GEOMETRY(Point, 4326),
    geom_3857       GEOMETRY(Point, 3857) NOT NULL,
    src_update_time TIMESTAMP(3),
    sync_time       TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_xfm_geom_4326 ON spatial_data_xfm USING GIST (geom_4326);
CREATE INDEX IF NOT EXISTS idx_xfm_geom_3857 ON spatial_data_xfm USING GIST (geom_3857);

-- ============== 2. 死信表（DLQ） ==============
-- Sink Job 发生失败时（无效几何、转换异常、目标库写入冲突等）写入此处
-- 字段 raw_payload 存原始 Kafka 消息 JSON，便于人工排查或重投
CREATE TABLE IF NOT EXISTS cdc_dlq (
    id            BIGSERIAL PRIMARY KEY,
    src_id        INT,                       -- 源记录主键，可为空（解析失败时）
    op            TEXT,                      -- 操作类型：c/u/d
    error_class   TEXT NOT NULL,             -- 异常类全限定名
    error_message TEXT,                      -- 异常 message
    raw_payload   JSONB,                     -- 原始事件
    occurred_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replayed_at   TIMESTAMP(3),              -- 重投时间，NULL 表示未重投
    replay_count  INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dlq_occurred_at ON cdc_dlq (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_unreplayed   ON cdc_dlq (occurred_at DESC) WHERE replayed_at IS NULL;

-- ============== 3. 同步指标表 ==============
-- Flink Job 每 N 秒滚动窗口聚合一次，写入此表供看板查询
CREATE TABLE IF NOT EXISTS sync_metrics (
    window_start  TIMESTAMP(3) NOT NULL,
    window_end    TIMESTAMP(3) NOT NULL,
    job_name      TEXT NOT NULL,
    records_in    BIGINT NOT NULL DEFAULT 0,    -- 窗口内进入的事件数
    records_out   BIGINT NOT NULL DEFAULT 0,    -- 写入目标库成功的数量
    records_dlq   BIGINT NOT NULL DEFAULT 0,    -- 进入 DLQ 的数量
    p50_latency_ms BIGINT,                       -- 端到端延迟（src.update_time → sink_time）
    p99_latency_ms BIGINT,
    PRIMARY KEY (window_start, job_name)
);

CREATE INDEX IF NOT EXISTS idx_metrics_recent ON sync_metrics (window_start DESC);
