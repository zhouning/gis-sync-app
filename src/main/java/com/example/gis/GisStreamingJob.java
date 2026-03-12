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

public class GisStreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kryo Serialization: Register JTS Geometry types for efficient serialization
        env.getConfig().registerKryoType(Point.class);
        env.getConfig().registerKryoType(LineString.class);
        env.getConfig().registerKryoType(Polygon.class);
        env.getConfig().registerKryoType(MultiPoint.class);
        env.getConfig().registerKryoType(MultiLineString.class);
        env.getConfig().registerKryoType(MultiPolygon.class);
        env.getConfig().registerKryoType(GeometryCollection.class);

        // Production Configuration: Enable Checkpointing (1 minute interval)
        env.enableCheckpointing(60000);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        cpConfig.setMinPauseBetweenCheckpoints(30000);
        cpConfig.setCheckpointTimeout(120000);
        cpConfig.setTolerableCheckpointFailureNumber(3);
        cpConfig.setMaxConcurrentCheckpoints(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);

        System.out.println("Sedona Functions Registered Successfully.");

        String ddl = "CREATE TEMPORARY TABLE source_geodata ( " +
                     "    id INT, " +
                     "    lon DOUBLE, " +
                     "    lat DOUBLE, " +
                     "    row_time AS PROCTIME() " +
                     ") WITH ( " +
                     "    'connector' = 'datagen', " +
                     "    'rows-per-second' = '2', " +
                     "    'fields.id.kind' = 'sequence', " +
                     "    'fields.id.start' = '1', " +
                     "    'fields.id.end' = '100', " +
                     "    'fields.lon.min' = '73.0', " +
                     "    'fields.lon.max' = '135.0', " +
                     "    'fields.lat.min' = '18.0', " +
                     "    'fields.lat.max' = '53.0' " +
                     ")";

        tableEnv.executeSql(ddl);

        // Robust SQL: Validate coordinates and Geometry before Transform
        String query = "SELECT " +
                       "    id, " +
                       "    lon, " +
                       "    lat, " +
                       "    ST_AsText(ST_SetSRID(ST_Point(lon, lat), 4326)) as original_wgs84, " +
                       "    CASE " +
                       "      WHEN ST_IsValid(ST_SetSRID(ST_Point(lon, lat), 4326)) = true THEN " +
                       "        ST_AsText( " +
                       "            ST_Transform( " +
                       "                ST_SetSRID(ST_Point(lon, lat), 4326), " +
                       "                'EPSG:4326', " +
                       "                'EPSG:3857' " +
                       "            ) " +
                       "        ) " +
                       "      ELSE 'INVALID_GEOMETRY' " +
                       "    END as transformed_mercator " +
                       "FROM source_geodata " +
                       "WHERE lon BETWEEN -180 AND 180 " +
                       "  AND lat BETWEEN -90 AND 90 " +
                       "  AND NOT (lon = 0.0 AND lat = 0.0)";

        Table transformedTable = tableEnv.sqlQuery(query);

        // ---------------------------------------------------------------
        // JDBC Sink Example (Production Reference)
        // ---------------------------------------------------------------
        // String jdbcSinkDdl =
        //     "CREATE TEMPORARY TABLE sink_geodata ( " +
        //     "    id INT, " +
        //     "    lon DOUBLE, " +
        //     "    lat DOUBLE, " +
        //     "    original_wgs84 STRING, " +
        //     "    transformed_mercator STRING, " +
        //     "    PRIMARY KEY (id) NOT ENFORCED " +
        //     ") WITH ( " +
        //     "    'connector' = 'jdbc', " +
        //     "    'url' = 'jdbc:postgresql://localhost:5432/gis_db', " +
        //     "    'table-name' = 'geo_transformed', " +
        //     "    'username' = '${JDBC_USER}', " +
        //     "    'password' = '${JDBC_PASSWORD}', " +
        //     "    'sink.buffer-flush.max-rows' = '500', " +
        //     "    'sink.buffer-flush.interval' = '5s' " +
        //     ")";
        // tableEnv.executeSql(jdbcSinkDdl);
        // transformedTable.executeInsert("sink_geodata");
        // ---------------------------------------------------------------

        transformedTable.execute().print();
    }
}
