# GIS Real-time Sync App

基于 **Apache Flink** + **Apache Sedona** 的 GIS 实时数据同步与坐标转换应用。

## 功能特性

- **实时坐标转换**：WGS84 (EPSG:4326) → Web Mercator (EPSG:3857) 实时投影转换
- **坐标校验**：自动过滤超范围、零值等无效坐标
- **地理围栏判断**：基于 `ST_Contains` + TUMBLE 窗口的实时点面关系检测与区域密度统计
- **Flink CDC 直连**：内嵌 Debezium 直连 PostGIS，无需独立部署 Kafka/Debezium
- **高性能序列化**：为 JTS Geometry 注册 Kryo 序列化器，减少约 2/3 序列化开销
- **生产就绪**：支持 Exactly-Once 语义、Checkpoint 容错、JDBC Sink 输出

## 技术栈

| 组件 | 版本 | 说明 |
| :--- | :--- | :--- |
| Apache Flink | 1.19.0 | 流处理引擎 |
| Apache Sedona | 1.8.1 | 分布式空间 SQL |
| GeoTools | 30.2 | CRS 管理与投影转换 |
| Flink CDC | 3.0.1 | PostGIS 变更捕获 |
| Java | 11+ | 开发语言 |

## 快速开始

### 环境要求

- JDK 11+（推荐 JDK 17）
- Maven 3.8+

### 构建

```bash
git clone https://github.com/zhouning/gis-sync-app.git
cd gis-sync-app
mvn clean package
```

### 运行

**本地运行（IDE）**

直接在 IDE 中运行 `GisStreamingJob.main()`，使用内嵌 Flink 环境。

**集群部署**

```bash
./bin/flink run -c com.example.gis.GisStreamingJob target/gis-sync-app-1.0-SNAPSHOT.jar
```

### 测试

```bash
mvn test
```

## 作业说明

| 作业类 | 说明 |
| :--- | :--- |
| `GisStreamingJob` | 核心坐标转换作业：datagen source → WGS84 to Mercator → print/JDBC |
| `GisStreamingJobCdc` | Flink CDC 直连 PostGIS，无需独立 Kafka/Debezium |
| `GeofenceStreamingJob` | 地理围栏空间 Join：ST_Contains + TUMBLE 窗口密度统计 |

## 架构概览

```
PostGIS ──→ Flink CDC ──→ Flink + Sedona SQL ──→ 目标数据库
                              │
                    ┌─────────┴─────────┐
                    │  坐标校验          │
                    │  ST_Point 构建     │
                    │  ST_SetSRID(4326)  │
                    │  ST_Transform(3857)│
                    │  ST_AsText 输出    │
                    └───────────────────┘
```

## 配置示例

默认使用 `datagen` 数据源演示。接入 Kafka 数据源示例：

```sql
CREATE TABLE source_geodata (
    id INT,
    lon DOUBLE,
    lat DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'gis-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
```

## 常见问题

- **`NoClassDefFoundError: org/geotools/...`**：确保使用 shaded JAR 部署
- **Windows 编码问题**：终端执行 `chcp 65001` 切换 UTF-8

## 设计文档

- [DESIGN.md](DESIGN.md)（English）
- [DESIGN_CN.md](DESIGN_CN.md)（中文）

## License

MIT
