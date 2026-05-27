package com.example.gis.it;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * 共享的集成测试环境：拉起 PG_src、PG_dst、Kafka、Schema Registry。
 *
 * <p>这是 testcontainers 的"启动 + 注入连接信息 + 关闭"的样板代码集中地，
 * 各个 IT 用 {@link #startContainers()} / {@link #stopContainers()} 在 BeforeAll/AfterAll 里统一调用。
 *
 * <p>容器都用同一 Docker network，方便 SR 用 service 名连 Kafka。
 */
public final class TestEnvironment {

    public static final String PG_USER = "postgres";
    public static final String PG_PASS = "postgres";
    public static final String SRC_DB = "geodb_src";
    public static final String DST_DB = "geodb_dst";
    public static final String SLOT_NAME = "gis_sync_slot";
    public static final String PUBLICATION = "gis_pub";
    public static final String CDC_TOPIC = "spatial-data-cdc";
    public static final String DLQ_TOPIC = "spatial-data-dlq";

    private static Network network;
    private static PostgreSQLContainer<?> srcPg;
    private static PostgreSQLContainer<?> dstPg;
    private static ConfluentKafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    private TestEnvironment() {}

    public static void startContainers() {
        network = Network.newNetwork();

        srcPg = new PostgreSQLContainer<>(DockerImageName.parse("imresamu/postgis:16-3.4")
                    .asCompatibleSubstituteFor("postgres"))
                .withNetwork(network)
                .withNetworkAliases("postgis-src")
                .withDatabaseName(SRC_DB)
                .withUsername(PG_USER)
                .withPassword(PG_PASS)
                .withCommand("postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_wal_senders=10",
                        "-c", "max_replication_slots=10")
                .withCopyFileToContainer(MountableFile.forClasspathResource("init-src.sql"),
                        "/docker-entrypoint-initdb.d/01-init.sql")
                .withStartupTimeout(Duration.ofMinutes(2));

        dstPg = new PostgreSQLContainer<>(DockerImageName.parse("imresamu/postgis:16-3.4")
                    .asCompatibleSubstituteFor("postgres"))
                .withNetwork(network)
                .withNetworkAliases("postgis-dst")
                .withDatabaseName(DST_DB)
                .withUsername(PG_USER)
                .withPassword(PG_PASS)
                .withCopyFileToContainer(MountableFile.forClasspathResource("init-dst.sql"),
                        "/docker-entrypoint-initdb.d/01-init.sql")
                .withStartupTimeout(Duration.ofMinutes(2));

        kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
                .withNetwork(network)
                .withNetworkAliases("kafka");

        srcPg.start();
        dstPg.start();
        kafka.start();

        // SR 必须能连到 Kafka（用 KAFKA broker network alias）
        schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
                .waitingFor(Wait.forHttp("/subjects").forPort(8081).withStartupTimeout(Duration.ofMinutes(2)));
        schemaRegistry.start();

        createTopics();
    }

    public static void stopContainers() {
        if (schemaRegistry != null) schemaRegistry.stop();
        if (kafka != null) kafka.stop();
        if (dstPg != null) dstPg.stop();
        if (srcPg != null) srcPg.stop();
        if (network != null) network.close();
    }

    private static void createTopics() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient ac = AdminClient.create(p)) {
            ac.createTopics(List.of(
                new NewTopic(CDC_TOPIC, 3, (short) 1),
                new NewTopic(DLQ_TOPIC, 1, (short) 1),
                new NewTopic("spatial-sync-metrics", 1, (short) 1)
            )).all().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("createTopics failed", e);
        }
    }

    public static String srcJdbcUrl()    { return srcPg.getJdbcUrl(); }
    public static String dstJdbcUrl()    { return dstPg.getJdbcUrl(); }
    public static String srcHost()       { return "postgis-src"; }      // 内部网络别名
    public static String srcInternalPort() { return "5432"; }
    public static String dstHost()       { return "postgis-dst"; }
    public static String dstInternalPort() { return "5432"; }
    /** Kafka host:port（host 网络访问，给本测试 JVM 用 KafkaProducer/Consumer）。 */
    public static String kafkaBootstrap() { return kafka.getBootstrapServers(); }
    /** Kafka 容器内别名（给 Flink mini-cluster 跑的 Job 在容器网络里访问，但 mini-cluster 在 host 跑，所以两者其实都用 host 端口）。 */
    public static String kafkaInternalBootstrap() { return "kafka:9093"; }
    public static String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }

    public static Connection connectSrc() throws Exception {
        return DriverManager.getConnection(srcJdbcUrl(), PG_USER, PG_PASS);
    }
    public static Connection connectDst() throws Exception {
        return DriverManager.getConnection(dstJdbcUrl(), PG_USER, PG_PASS);
    }

    public static void truncateAll() throws Exception {
        try (Connection c = connectSrc(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE spatial_data");
        }
        try (Connection c = connectDst(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE spatial_data_xfm, cdc_dlq, sync_metrics");
        }
    }
}
