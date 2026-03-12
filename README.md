# GIS Real-time Synchronization App

This project provides a robust, streaming-based solution for real-time GIS data synchronization and transformation. It leverages **Apache Flink** for stream processing and **Apache Sedona** for high-performance spatial SQL operations.

## 🚀 Features

*   **Real-time Coordinate Transformation**: Converts WGS84 (GPS) coordinates to Web Mercator (EPSG:3857) on the fly.
*   **Coordinate Validation**: Filters out invalid coordinates (out-of-range, zero values) before transformation.
*   **Geofence Spatial Join**: Real-time point-in-polygon detection with TUMBLE window aggregation.
*   **Flink CDC Direct Connect**: Captures PostGIS changes without a separate Kafka/Debezium deployment.
*   **High Performance**: Kryo serialization for JTS Geometry types, distributed spatial indexes and native Flink operators.
*   **Production Ready**: Enhanced Checkpointing (Exactly-Once, tolerant failures, min pause), JDBC Sink reference.
*   **Heterogeneous Support**: Designed to bridge PostGIS, Oracle Spatial, and other spatial data sources.

## 🛠️ Prerequisites

*   **Java**: JDK 11 or higher (Tested on JDK 17)
*   **Maven**: 3.8.x or higher
*   **Flink Cluster**: (Optional for local testing) Flink 1.19+

## 📥 Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd gis-sync-app
    ```

2.  **Build the project**:
    This will create a lightweight JAR for development and a shaded JAR for production deployment.
    ```bash
    mvn clean package
    ```

## 🏃‍♂️ Usage

### Local Execution (IDE)
You can run the `GisStreamingJob` directly in your IDE (IntelliJ/Eclipse). It uses an embedded Flink environment.

1.  Open `src/main/java/com/example/gis/GisStreamingJob.java`.
2.  Run the `main()` method.
3.  Observe the console output for transformed spatial data.

### Available Jobs

| Job Class | Description |
| :--- | :--- |
| `GisStreamingJob` | 核心坐标转换 Job（datagen source → WGS84 to Mercator → print/JDBC） |
| `GisStreamingJobCdc` | Flink CDC 直连 PostGIS 示例（无需 Kafka/Debezium 独立部署） |
| `GeofenceStreamingJob` | 地理围栏空间 Join 示例（ST_Contains + TUMBLE 窗口密度统计） |

### Cluster Deployment
To submit the job to a running Flink cluster:

```bash
./bin/flink run -c com.example.gis.GisStreamingJob target/gis-sync-app-1.0-SNAPSHOT.jar
```

## ⚙️ Configuration

The current version uses a `datagen` source for demonstration. To connect to real data sources, modify the `CREATE TABLE` SQL in `GisStreamingJob.java`:

**Example: Reading from Kafka**
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

## 🧪 Testing

Run the included unit tests to verify spatial logic:

```bash
mvn test
```

## ⚠️ Troubleshooting

*   **`NoClassDefFoundError: org/geotools/...`**: Ensure you are using the `shaded` JAR (`gis-sync-app-1.0-SNAPSHOT.jar`) which bundles necessary GeoTools dependencies.
*   **Encoding Issues**: If you see "Illegal character" errors on Windows, ensure your terminal uses UTF-8 (`chcp 65001`) or rely on the `mvn package` command which handles encoding correctly.

## 📚 Technical Design

For a deep dive into the architecture, CDC patterns, and trade-off analysis, please refer to:
*   [DESIGN.md](DESIGN.md) (English)
*   [DESIGN_CN.md](DESIGN_CN.md) (Chinese)
