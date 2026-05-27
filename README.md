# GIS Real-time Sync App

PostGIS → Flink CDC → Kafka → Flink + Sedona 转换 → PostGIS 的实时空间数据同步管道，附带看板、监控、告警、容错与 CI/CD。

## 全景

```
┌────────────────┐       ┌──────────────────┐       ┌────────────────┐
│  PostGIS_src   │──CDC──▶│  Source Job      │──Avro─▶│  Kafka         │
│  (业务库)       │       │  postgres-cdc    │       │  spatial-data- │
│  geom 4326     │       │  → Avro Confluent│       │  cdc           │
└────────────────┘       └──────────────────┘       └────────┬───────┘
                                                              │
        ┌─────────────────────────────────────────────────────┘
        ▼
┌──────────────────────────┐    ┌────────────────┐
│  Sink Job                │───▶│  PostGIS_dst   │
│  Avro → Sedona transform │    │  geom 3857+GiST│
│  → 主流 / DLQ 分流        │    └────────────────┘
└────┬─────────────┬───────┘    ┌────────────────┐
     │             └────────────│  cdc_dlq 表 +  │
     │                          │  spatial-data- │
     │                          │  dlq topic     │
     │                          └────────────────┘
     │
     ▼
sync_metrics 表（10s 滚动窗口指标）

┌──────────────────────────┐    ┌────────────────┐    ┌────────────────┐
│  Spring Boot Backend     │◀───│   PostGIS_dst  │    │  React 看板    │
│  REST + WebSocket :8090  │    │   + 源库 slot  │◀───│  :8080 / :5173 │
└──────────────────────────┘    └────────────────┘    └────────────────┘

监控：Prometheus + Grafana + AlertManager + 3 个 exporter
容错：双 JM HA（ZK 协调）+ RocksDB 增量 checkpoint + 指数退避重启
```

## 模块结构

```
.
├── flink-jobs/        Flink 作业（Java 11）
│   └── src/main/java/com/example/gis/
│       ├── source/SourceCdcToKafkaJob.java   PG CDC → Kafka
│       ├── sink/KafkaToSinkJob.java          Kafka → Sedona → PG_dst + DLQ
│       └── common/ ...
├── backend/           Spring Boot API（Java 17）
│   └── src/main/java/com/example/gis/backend/
│       ├── api/        7 个 REST + 1 个 WS 端点
│       ├── kafka/      DLQ 重投 + WebSocket 推流
│       └── service/    DB / Flink REST 客户端
├── frontend/          React 18 + Vite + antd 5 + MapLibre + recharts
├── docker/            docker-compose 12+1 容器
├── scripts/smoke.sh   本地 / CI 端到端冒烟
└── .github/workflows/ CI + e2e-smoke
```

## 技术栈

| 层 | 选型 | 版本 |
| :--- | :--- | :--- |
| 流处理 | Apache Flink | **1.19.3** |
| 空间引擎 | Apache Sedona | **1.9.0** |
| CDC | Flink CDC postgres-cdc | **3.5.0** |
| Connector | flink-connector-jdbc / flink-connector-kafka | 3.3.0-1.19 |
| Avro | flink-avro-confluent-registry | 1.19.3 |
| 消息中间件 | Confluent Kafka KRaft + Schema Registry | 7.7.1 |
| 数据库 | PostGIS | 16-3.4 (ARM64 用 imresamu/postgis) |
| 后端 | Spring Boot | 3.3.5（Java 17） |
| 前端 | React + Vite + antd + MapLibre | 18 / 5 / 5 / 4 |
| 监控 | Prometheus / Grafana / AlertManager | v2.55 / 11.3 / v0.27 |
| HA | ZooKeeper | 3.9 |

## 端口

| URL | 服务 |
| :--- | :--- |
| http://localhost:8080 | **看板（生产）** |
| http://localhost:5173 | **看板（dev server）** |
| http://localhost:8090 | Backend API |
| http://localhost:8081 / 8181 | Flink UI（双 JM HA） |
| http://localhost:3000 | Grafana（admin/admin） |
| http://localhost:9090 | Prometheus |
| http://localhost:9093 | AlertManager |
| http://localhost:8082 | Schema Registry |
| localhost:5432 / 5433 | PostGIS src / dst |

## 快速开始

```bash
# 1. 装依赖
make build            # docker 内跑 mvn 构建 flink-jobs + backend
make libs             # 下载 Flink connector / Avro / SR client jar 到 docker/flink-libs/
make fe-install       # 前端 npm install
make fe-build         # 出 frontend/dist/

# 2. 起栈
make up               # 起 12 个容器（不含 prod 前端）
make ps               # 查容器状态

# 3. 提交 Flink 作业
make submit-source    # PG → Kafka
make submit-sink      # Kafka → PG（带 DLQ + 指标聚合）

# 4. 看板
open http://localhost:5173      # dev：要先 make fe-dev 起 vite
# 或
make fe-build && docker compose -f docker/docker-compose.yml --profile prod up -d frontend
open http://localhost:8080      # 生产形态：nginx + dist + 反代

# 5. 测试
make test             # 单元测试
make it               # 集成测试（Testcontainers，~3-5 分钟）
make smoke            # 端到端冒烟（起栈 → 提作业 → 注入数据 → 验证 → 清理）
```

## 常用运维命令

```bash
# 数据
make psql-src                          # 进源库
make psql-dst                          # 进目标库
make kafka-topics                      # 列 Kafka topic
make kafka-consume TOPIC=spatial-data-cdc

# Flink
make job-list                          # RUNNING 作业列表
make ha-leader                         # 看 ZK 里的 leader 选举
make ha-kill-leader                    # 演练 HA 故障转移
make savepoint JOB=<jid>               # 触发 savepoint

# 监控
make prom-targets                      # Prometheus scrape targets
make prom-alerts                       # 当前 firing 告警
make flink-metrics                     # Flink Prometheus reporter 探活

# 后端
make api                               # 一次性测全部 REST 端点
make backend-logs
```

## 运行特性

- **EOS / 容错**：Flink checkpoint 1 分钟一次（EXACTLY_ONCE），失败按指数退避重启，外部化 checkpoint 在 cancel 后保留。
- **JobManager HA**：ZK 协调下双 JM。`make ha-kill-leader` 杀 leader，数据面在 standby 接管期间不中断。
- **State Backend**：RocksDB + 增量 checkpoint。
- **DLQ**：Sink Job 把"业务级失败"（无效几何、SRID 错等）写双副本——`cdc_dlq` 表（看板查询）+ `spatial-data-dlq` topic（程序化重投）。
- **重投**：`POST /api/dlq/{id}/replay` 从 raw_payload 反序列化 → 重新写回 spatial-data-cdc。
- **业务指标**：Sink Job 内部 10 秒滚动窗口聚合 records_in/out/dlq + P50/P99 延迟，写 `sync_metrics` 表 → postgres_exporter 暴露给 Prometheus。

## 关键告警（10 条规则）

PostgreSQL 复制槽（业界 #1 事故源）、Flink job 状态、Kafka consumer lag、业务级 DLQ 速率与端到端延迟。详见 `docker/prometheus/rules/alerts.yml`。

## CI/CD

- **`.github/workflows/ci.yml`**：每次 PR + main push 跑 build-flink、build-backend、build-frontend、integration-tests（Testcontainers）。
- **`.github/workflows/e2e-smoke.yml`**：仅 main push / 手动触发跑完整 docker compose smoke。
- **`.github/dependabot.yml`**：每周扫描 maven / npm / GitHub Actions 依赖。

## 许可

MIT
