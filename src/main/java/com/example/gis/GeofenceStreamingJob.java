package com.example.gis;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.sedona.flink.SedonaFlinkRegistrator;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * 空间 Join + 地理围栏（Geofence）实时判断示例 Job。
 *
 * <p>核心原理：
 * <ul>
 *   <li>空间 Join：将车辆实时坐标与预定义的地理围栏多边形做 ST_Contains 判断，
 *       当车辆坐标点落在某个围栏多边形内部时，即认为该车辆进入了对应区域。</li>
 *   <li>TUMBLE 窗口：将无界流按固定时间间隔（1 分钟）切分为不重叠的窗口，
 *       在每个窗口内对各区域的车辆数做聚合统计。</li>
 * </ul>
 *
 * <p>生产环境注意事项：
 * <ul>
 *   <li>可使用 R-tree 空间索引加速 ST_Contains 计算，避免逐条遍历围栏多边形。</li>
 *   <li>geofence 围栏表在生产中应从数据库或配置中心动态加载，而非硬编码 VALUES。</li>
 * </ul>
 */
public class GeofenceStreamingJob {
    public static void main(String[] args) throws Exception {
        // ========== 1. 配置 Flink 流处理环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 配置：60s 间隔，EXACTLY_ONCE 语义，容忍 3 次失败，最小间隔 30s
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // ========== 2. 注册 Sedona 空间函数与 Kryo 序列化器 ==========
        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);

        // Kryo 序列化器注册
        env.getConfig().registerKryoType(Point.class);
        env.getConfig().registerKryoType(LineString.class);
        env.getConfig().registerKryoType(Polygon.class);
        env.getConfig().registerKryoType(MultiPoint.class);
        env.getConfig().registerKryoType(MultiLineString.class);
        env.getConfig().registerKryoType(MultiPolygon.class);
        env.getConfig().registerKryoType(GeometryCollection.class);

        System.out.println("Sedona Functions Registered Successfully.");

        // ========== 3. 创建车辆实时位置 datagen source 表 ==========
        // 模拟北京区域内的车辆 GPS 坐标流（经度 116.2~116.6，纬度 39.7~40.1）
        String vehicleDdl =
                "CREATE TEMPORARY TABLE vehicle_positions ( " +
                "    vehicle_id STRING, " +
                "    lon DOUBLE, " +
                "    lat DOUBLE, " +
                "    event_time AS PROCTIME() " +
                ") WITH ( " +
                "    'connector' = 'datagen', " +
                "    'rows-per-second' = '10', " +
                "    'fields.vehicle_id.kind' = 'sequence', " +
                "    'fields.vehicle_id.start' = '1', " +
                "    'fields.vehicle_id.end' = '1000', " +
                "    'fields.lon.min' = '116.2', " +
                "    'fields.lon.max' = '116.6', " +
                "    'fields.lat.min' = '39.7', " +
                "    'fields.lat.max' = '40.1' " +
                ")";
        tableEnv.executeSql(vehicleDdl);

        // ========== 4. 创建静态地理围栏区域表 ==========
        // 使用 VALUES 子句定义北京典型区域的多边形 WKT
        // 生产环境中应从数据库或配置中心加载围栏数据
        String geofenceDdl =
                "CREATE TEMPORARY VIEW geofence_areas AS " +
                "SELECT * FROM (VALUES " +
                "    ('GF001', '天安门广场', " +
                "     'POLYGON((116.386 39.903, 116.392 39.903, 116.392 39.907, 116.386 39.907, 116.386 39.903))'), " +
                "    ('GF002', '中关村', " +
                "     'POLYGON((116.298 39.957, 116.330 39.957, 116.330 39.985, 116.298 39.985, 116.298 39.957))'), " +
                "    ('GF003', '望京', " +
                "     'POLYGON((116.460 39.980, 116.500 39.980, 116.500 40.010, 116.460 40.010, 116.460 39.980))'), " +
                "    ('GF004', 'CBD', " +
                "     'POLYGON((116.440 39.900, 116.480 39.900, 116.480 39.930, 116.440 39.930, 116.440 39.900))') " +
                ") AS t(area_id, area_name, wkt_polygon)";
        tableEnv.executeSql(geofenceDdl);

        // ========== 5. 空间 Join + TUMBLE 窗口聚合查询 ==========
        // 空间 Join 原理：ST_Contains 判断围栏多边形是否包含车辆坐标点
        // TUMBLE 窗口：按 1 分钟滚动窗口聚合，统计各区域内不重复车辆数
        // 生产环境中可通过 R-tree 索引加速 ST_Contains 的空间匹配计算
        String query =
                "SELECT " +
                "    g.area_id, " +
                "    g.area_name, " +
                "    TUMBLE_START(v.event_time, INTERVAL '1' MINUTE) AS window_start, " +
                "    TUMBLE_END(v.event_time, INTERVAL '1' MINUTE) AS window_end, " +
                "    COUNT(DISTINCT v.vehicle_id) AS vehicle_count " +
                "FROM vehicle_positions v " +
                "CROSS JOIN geofence_areas g " +
                "WHERE ST_Contains( " +
                "    ST_GeomFromWKT(g.wkt_polygon), " +
                "    ST_Point(v.lon, v.lat) " +
                ") " +
                "GROUP BY " +
                "    g.area_id, " +
                "    g.area_name, " +
                "    TUMBLE(v.event_time, INTERVAL '1' MINUTE)";

        Table resultTable = tableEnv.sqlQuery(query);

        // ========== 6. 输出结果 ==========
        resultTable.execute().print();
    }
}
