package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.config.KafkaTopics;
import com.example.kafkapatterns.dto.BroadcastMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ-style PUB/SUB FANOUT.
 *
 * These two listeners use DIFFERENT {@code groupId}s on the same {@code
 * broadcast-topic}. Kafka tracks offsets per consumer group independently,
 * so every message is delivered to BOTH groups in full -- each behaves as
 * if it had its own private queue bound to a RabbitMQ fanout exchange.
 */
@Component
public class FanoutListener {

    private static final Logger log = LoggerFactory.getLogger(FanoutListener.class);

    @KafkaListener(
            id = "analytics-listener",
            topics = KafkaTopics.BROADCAST_TOPIC,
            groupId = KafkaTopics.GROUP_ANALYTICS)
    public void onAnalyticsEvent(ConsumerRecord<String, BroadcastMessage> record) {
        BroadcastMessage message = record.value();
        log.info("[{}] received event={} content={}",
                KafkaTopics.GROUP_ANALYTICS, message.eventId(), message.content());
    }

    @KafkaListener(
            id = "notifications-listener",
            topics = KafkaTopics.BROADCAST_TOPIC,
            groupId = KafkaTopics.GROUP_NOTIFICATIONS)
    public void onNotificationsEvent(ConsumerRecord<String, BroadcastMessage> record) {
        BroadcastMessage message = record.value();
        log.info("[{}] received event={} content={}",
                KafkaTopics.GROUP_NOTIFICATIONS, message.eventId(), message.content());
    }
}
