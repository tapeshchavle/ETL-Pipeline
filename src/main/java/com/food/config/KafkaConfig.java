package com.food.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Kafka producer configuration for the Data Engineering Pipeline.
 * Publishes business events (order, cart, user) to Kafka topics
 * consumed by the data pipeline for analytics and ML.
 *
 * SAFETY: If Kafka is unreachable the Spring Boot app continues to work normally.
 * All publishes are wrapped in try-catch inside KafkaPublishingService.
 */
@Configuration
public class KafkaConfig {

    // Defaults to localhost:9094 (external listener) so host machine can reach
    // the Kafka container. Pipeline containers use kafka:9092 internally.
    @Value("${spring.kafka.bootstrap-servers:localhost:9094}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Don't add type headers — keeps messages clean for cross-language consumers (Python)
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        // Async retries for reliability
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
