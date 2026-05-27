package com.example.gis.backend.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消费配置：用 Confluent KafkaAvroDeserializer 反序列化 spatial-data-cdc。
 * specific.avro.reader=false 让 deserializer 返回 GenericRecord（无需预生成 Avro 类）。
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<Object, GenericRecord> cdcConsumerFactory(
            GisProperties props,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, props.getKafka().getWsConsumerGroup());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // key 是 avro-confluent 编码（Source Job 那边 'key.format'='avro-confluent'）
        // 所以这里 key 也得用 KafkaAvroDeserializer，不能 IntegerDeserializer
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        p.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getKafka().getSchemaRegistryUrl());
        p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(p);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, GenericRecord> cdcKafkaListenerFactory(
            ConsumerFactory<Object, GenericRecord> cdcConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, GenericRecord> f =
            new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(cdcConsumerFactory);
        f.setConcurrency(1);
        return f;
    }
}
