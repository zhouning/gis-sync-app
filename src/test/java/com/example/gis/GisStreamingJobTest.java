package com.example.gis;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.sedona.flink.SedonaFlinkRegistrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GisStreamingJobTest {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kryo 序列化器注册
        env.getConfig().registerKryoType(Point.class);
        env.getConfig().registerKryoType(LineString.class);
        env.getConfig().registerKryoType(Polygon.class);
        env.getConfig().registerKryoType(MultiPoint.class);
        env.getConfig().registerKryoType(MultiLineString.class);
        env.getConfig().registerKryoType(MultiPolygon.class);
        env.getConfig().registerKryoType(GeometryCollection.class);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(env, settings);

        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);
    }

    @Test
    public void testCoordinateTransformation() throws Exception {
        String ddl = "CREATE TEMPORARY TABLE source_input ( " +
                     "    id INT, " +
                     "    lon DOUBLE, " +
                     "    lat DOUBLE " +
                     ") WITH ( " +
                     "    'connector' = 'datagen', " +
                     "    'number-of-rows' = '1', " +
                     "    'fields.id.kind' = 'sequence', " +
                     "    'fields.id.start' = '1', " +
                     "    'fields.id.end' = '1', " +
                     "    'fields.lon.kind' = 'random', " +
                     "    'fields.lon.min' = '116.4070', " +
                     "    'fields.lon.max' = '116.4080', " +
                     "    'fields.lat.kind' = 'random', " +
                     "    'fields.lat.min' = '39.9040', " +
                     "    'fields.lat.max' = '39.9050' " +
                     ")";

        tableEnv.executeSql(ddl);

        String query = "SELECT " +
                       "    ST_AsText( " +
                       "        ST_Transform( " +
                       "            ST_SetSRID(ST_Point(lon, lat), 4326), " +
                       "            'EPSG:4326', " +
                       "            'EPSG:3857' " +
                       "        ) " +
                       "    ) as wkt_result " +
                       "FROM source_input";

        Table resultTable = tableEnv.sqlQuery(query);

        List<String> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = resultTable.execute().collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                results.add(row.getFieldAs("wkt_result"));
            }
        }

        assertThat(results).hasSize(1);
        String wkt = results.get(0);

        assertThat(wkt).startsWith("POINT (");

        String[] parts = wkt.replace("POINT (", "").replace(")", "").split(" ");
        double x = Double.parseDouble(parts[0]);
        double y = Double.parseDouble(parts[1]);

        assertThat(x).isBetween(12950000.0, 12970000.0);
        assertThat(y).isBetween(4850000.0, 4860000.0);

        System.out.println("Test Passed! Transformed WKT: " + wkt);
    }

    @Test
    public void testCoordinateValidationFiltersZeroCoords() throws Exception {
        // 验证 (0,0) 坐标被过滤
        String ddl = "CREATE TEMPORARY TABLE zero_coords ( " +
                     "    id INT, " +
                     "    lon DOUBLE, " +
                     "    lat DOUBLE " +
                     ") WITH ( " +
                     "    'connector' = 'datagen', " +
                     "    'number-of-rows' = '1', " +
                     "    'fields.id.kind' = 'sequence', " +
                     "    'fields.id.start' = '1', " +
                     "    'fields.id.end' = '1', " +
                     "    'fields.lon.min' = '0.0', " +
                     "    'fields.lon.max' = '0.0', " +
                     "    'fields.lat.min' = '0.0', " +
                     "    'fields.lat.max' = '0.0' " +
                     ")";

        tableEnv.executeSql(ddl);

        String query = "SELECT id, lon, lat " +
                       "FROM zero_coords " +
                       "WHERE lon BETWEEN -180 AND 180 " +
                       "  AND lat BETWEEN -90 AND 90 " +
                       "  AND NOT (lon = 0.0 AND lat = 0.0)";

        Table resultTable = tableEnv.sqlQuery(query);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = resultTable.execute().collect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        assertThat(results).isEmpty();
        System.out.println("Test Passed! Zero coordinates correctly filtered out.");
    }

    @Test
    public void testGeofenceSpatialContains() throws Exception {
        // 验证 ST_Contains 空间判断：北京天安门坐标应落在天安门围栏内
        String query = "SELECT ST_Contains( " +
                       "    ST_GeomFromWKT('POLYGON((116.386 39.903, 116.392 39.903, 116.392 39.907, 116.386 39.907, 116.386 39.903))'), " +
                       "    ST_Point(116.389, 39.905) " +
                       ") AS is_inside";

        Table resultTable = tableEnv.sqlQuery(query);

        List<Boolean> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = resultTable.execute().collect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next().getFieldAs("is_inside"));
            }
        }

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isTrue();
        System.out.println("Test Passed! Point correctly detected inside geofence.");
    }
}