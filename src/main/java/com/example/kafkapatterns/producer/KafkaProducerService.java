package com.example.kafkapatterns.producer;

import com.example.kafkapatterns.config.KafkaTopics;
import com.example.kafkapatterns.dto.BroadcastMessage;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.dto.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Single producer service backing all three messaging patterns. The
 * difference between "RabbitMQ-style" and "Kafka-native" here isn't the
 * producer API -- it's whether a partition key is supplied.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * No partition key: Kafka's sticky/round-robin partitioner spreads tasks
     * across {@code task-queue-topic}'s partitions, so the two competing
     * {@code worker-group} instances split the work -- like two consumers
     * pulling from the same RabbitMQ queue.
     */
    public void sendTaskToQueue(TaskMessage message) {
        kafkaTemplate.send(KafkaTopics.TASK_QUEUE_TOPIC, message);
        log.info("-> [task-queue-topic] queued task {}", message.taskId());
    }

    /**
     * No partition key needed: fanout semantics come from having multiple
     * consumer GROUPS on {@code broadcast-topic}, not from partitioning.
     */
    public void sendBroadcast(BroadcastMessage message) {
        kafkaTemplate.send(KafkaTopics.BROADCAST_TOPIC, message);
        log.info("-> [broadcast-topic] broadcast event {}", message.eventId());
    }

    /**
     * Keyed by {@code orderId}: guarantees every event for a given order is
     * appended to the same partition in publish order, which is what makes
     * offset-0 replay able to deterministically rebuild per-order state.
     */
    public void sendOrderEvent(String orderId, OrderEvent event) {
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_TOPIC, orderId, event);
        log.info("-> [order-events-topic] {} for order {}", event.eventType(), orderId);
    }
}
