package com.example.kafkapatterns.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * The queue-worker and fanout listeners use Spring Boot's auto-configured
 * {@code kafkaListenerContainerFactory} (default ack-mode from
 * application.yml). The order-events listener opts into a dedicated
 * MANUAL ack-mode factory instead, so it only commits an offset after it has
 * successfully applied the event to {@code OrderStateStore} -- illustrating
 * the kind of at-least-once, offset-aware processing that distinguishes
 * "consuming a log" from "consuming a queue".
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> manualAckKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
