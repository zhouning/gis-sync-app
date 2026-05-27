# P5 加固阶段路线（待办）

P0–P4 已经完成项目从单 Job demo 到全栈观测看板的演进。P5 是从"演示生产级"走向"真正生产可上线"的那一段，任何一项都可以单独成立一个迭代。

---

## 优先级矩阵

按"提升程度 ÷ 工作量"排，越靠上越值得先做：

| # | 任务 | 解决什么问题 | 工作量 | 优先级 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | **目标库 PG 加只读副本** | 看板查询不冲击同步主写 | S | 高 |
| 2 | **Kafka 多 broker** | 单 broker 是单点；副本因子 = 1 一旦磁盘损坏数据全失 | M | 高 |
| 3 | **后端加 Spring Security + JWT** | 现在 API 公开，任何人都能 `POST /dlq/{id}/replay` | M | 高 |
| 4 | **AlertManager 接入真实通知通道** | 占位 webhook 没用，得 Slack/钉钉/企业微信/邮件 | S | 高 |
| 5 | **链路追踪（OTel + Jaeger）** | 跨 backend/Flink/PG 的延迟归因，看板上能点击单条 DLQ 跳到对应链路 | L | 中 |
| 6 | **K8s Helm Chart** | 从 docker-compose 升到 K8s，支持滚动升级、HPA、PDB | L | 中 |
| 7 | **Flink 升级到 1.20 / 2.x** | Sedona 跟进后再升，目前钉死 1.19 | L | 中 |
| 8 | **地理热力图** | 看板加 H3 网格密度图层，比单点 marker 信息密度高 | M | 中 |
| 9 | **Schema Registry 同步给下游消费者** | 现在只有 Source/Sink Job 在用，外部消费者不存在 | S | 低 |
| 10 | **限流 / 熔断（Resilience4j）** | 重投 API 防滥用 | S | 低 |
| 11 | **CI 加 OWASP Dependency-Check** | 自动扫 CVE | S | 低 |
| 12 | **数据回填工具** | 历史数据修正后批量重转换写入目标库 | M | 低 |

---

## 详细方案速查

### 1. 目标库 PG 只读副本（streaming replication）

```
docker-compose 加：
  postgis-dst-replica:
    image: imresamu/postgis:16-3.4
    command: 启动 standby + walreceiver
    depends_on: postgis-dst (primary)
```

backend 的 `BeansConfig`：
- `dataSource`（@Primary）  → 写主（Sink Job 用）
- `readOnlyDataSource`（标 `@ReadOnly`） → 走 replica
- `DashboardService` 改用 `@Transactional(readOnly=true)` + `@RouteToReplica` 拦截器

成本：1 个新容器 + ~50 行 Spring 配置。

### 2. Kafka 多 broker

```
docker-compose:
  kafka-1, kafka-2, kafka-3 三个 broker（KRaft 都是 controller+broker）
  topic 重建：副本因子 3，min.insync.replicas=2
```

注意：本地 16GB 内存可能扛不住 3 broker × 1GB heap + 现有 12 容器，建议**仅在 K8s/staging 环境上**做这一项。

### 3. JWT Auth

```
backend/pom.xml +:
  spring-boot-starter-security
  spring-boot-starter-oauth2-resource-server
SecurityConfig:
  - 所有 /api/** 需 JWT Bearer token
  - JWT 由独立 IdP 签发（自建简易 /api/auth/login 或对接 Keycloak）
  - WebSocket /ws/** 用 JWT 握手参数
frontend:
  - login 页 + 把 token 存 localStorage
  - 所有 fetch 加 Authorization header
  - WS 连接时拼 ?token=xxx
```

### 4. AlertManager 实接入

替换 `docker/alertmanager/alertmanager.yml` 里的占位 webhook：
- 钉钉：`https://oapi.dingtalk.com/robot/send?access_token=...`（需中间转换 prom → 钉钉 markdown 格式的 webhook 适配器，比如 `prom-dingtalk-webhook`）
- 企业微信：类似适配器
- Email：`smtp_smarthost` 配置 + receiver

### 5. OpenTelemetry 链路追踪

```
docker-compose 加：jaeger 1.62 (all-in-one)
backend +: opentelemetry-spring-boot-starter
flink jobs +: -javaagent:opentelemetry-javaagent.jar  (在 docker-compose env 里)
```

trace 关联点：
- HTTP 入口（backend Controller）
- Kafka producer/consumer span
- JDBC 调用 span
- Flink operator span（需 1.18+ 自带支持）

看板加 trace ID 点击跳转到 jaeger UI 的 deep link。

### 6. Helm Chart

模块拆分：
```
charts/gis-sync/
  templates/
    postgis-src/postgis-dst (StatefulSet + headless Service)
    kafka (KRaft StatefulSet 3 副本)
    schema-registry
    flink-jobmanager (Deployment, replicas=2)
    flink-taskmanager (Deployment + HPA)
    backend (Deployment + Service + Ingress)
    frontend (Deployment with nginx + Service + Ingress)
    prometheus, grafana, alertmanager
  values.yaml: 资源 limit / 副本数 / 镜像 / TLS / 域名
```

注意：Sedona shaded jar 巨大（~94MB），考虑 ConfigMap mount 还是放镜像里。

### 7. Flink 升级

迁移路径（按时间顺序）：
1. 等 Sedona 升到 1.10+ 时，pom 里 `flink.version` 默认升到 1.20
2. 同步升级 flink-connector-kafka / jdbc / postgres-cdc 后缀到 -1.20
3. 跑全量回归 IT
4. 一次 commit 升完，单独 PR

**不要**让 dependabot 自动升 Flink（已经在 dependabot.yml 锁死了）。

### 8. 地理热力图

前端加 Layer：
- 接收 WS 推流时累计 H3 网格（@uber/h3-js, resolution 7-9）
- recharts 不擅长地图叠加，改用 deck.gl 或 maplibre native heatmap layer
- 后端可选地加 `/api/heatmap?bbox&res` 端点做服务端聚合（数据量大时）

### 9-12 其他

- **9** Schema Registry：把 schema 暴露给非 Flink 的下游（Spark/Trino），需要 SR 实例对外可访问 + ACL
- **10** Resilience4j：`/api/dlq/{id}/replay` 加 RateLimiter（每 IP 每分钟 5 次）
- **11** OWASP DC：GitHub Action `dependency-check-action`，每周扫一次
- **12** 回填工具：写个 CLI（CommandLineRunner），从指定 LSN 起读 WAL 重新走 Sink Job 路径

---

## 我建议的下一步

如果你后面想继续做，**最值的两件**：

1. **第 4 项（AlertManager 真接入）** — 半天工作量，立即把"演示告警"变成"出事会通知到人"，性价比最高。

2. **第 1 项（PG 只读副本）** — 看板查询冲击同步写是个真实风险，replica 配上 + backend 改动小。

完成这两个后，可以宣称"这套真的能上生产"。

后续想做更重的（K8s / OTel / JWT），任何一个单拎出来都是一个独立 Phase。
