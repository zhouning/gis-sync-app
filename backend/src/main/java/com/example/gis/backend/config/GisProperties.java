package com.example.gis.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 业务配置：从 application.yml 注入。
 */
@ConfigurationProperties(prefix = "gis")
public class GisProperties {

    private SourceDb sourceDb = new SourceDb();
    private Flink flink = new Flink();
    private Kafka kafka = new Kafka();

    public static class SourceDb {
        private String url;
        private String username;
        private String password;
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }

    public static class Flink {
        private String restUrl;
        public String getRestUrl() { return restUrl; }
        public void setRestUrl(String restUrl) { this.restUrl = restUrl; }
    }

    public static class Kafka {
        private String cdcTopic;
        private String dlqTopic;
        private String schemaRegistryUrl;
        private String wsConsumerGroup;
        public String getCdcTopic() { return cdcTopic; }
        public void setCdcTopic(String cdcTopic) { this.cdcTopic = cdcTopic; }
        public String getDlqTopic() { return dlqTopic; }
        public void setDlqTopic(String dlqTopic) { this.dlqTopic = dlqTopic; }
        public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
        public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }
        public String getWsConsumerGroup() { return wsConsumerGroup; }
        public void setWsConsumerGroup(String wsConsumerGroup) { this.wsConsumerGroup = wsConsumerGroup; }
    }

    public SourceDb getSourceDb() { return sourceDb; }
    public Flink getFlink() { return flink; }
    public Kafka getKafka() { return kafka; }
}
