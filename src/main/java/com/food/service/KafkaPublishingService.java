package com.food.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Safety wrapper around KafkaTemplate.
 *
 * Guarantees that:
 * 1. If Kafka is not running → no exception, app works normally
 * 2. If Kafka send fails    → logs a warning, app continues
 * 3. Publishing is fire-and-forget (async, non-blocking)
 *
 * Inject this service instead of KafkaTemplate directly in all service classes.
 */
@Service
public class KafkaPublishingService {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublishingService.class);

    // required=false: if Kafka is not configured the bean is null and we skip publishing
    @Autowired(required = false)
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes an event to the given Kafka topic.
     * Silently skips if Kafka is unavailable — existing API behaviour is never impacted.
     *
     * @param topic Kafka topic name
     * @param key   message key (usually userId or orderId for partitioning)
     * @param event the event POJO (serialized to JSON)
     */
    public void publish(String topic, String key, Object event) {
        if (kafkaTemplate == null) {
            log.debug("Kafka not configured — skipping event publish to topic: {}", topic);
            return;
        }
        try {
            kafkaTemplate.send(topic, key, event);
            log.debug("Published event to Kafka topic [{}] key [{}]", topic, key);
        } catch (Exception e) {
            // Never let Kafka failure break the main application flow
            log.warn("Failed to publish event to Kafka topic [{}]: {}", topic, e.getMessage());
        }
    }

    public void publish(String topic, Object event) {
        publish(topic, null, event);
    }

}
