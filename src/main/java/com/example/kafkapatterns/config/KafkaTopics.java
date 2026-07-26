package com.example.kafkapatterns.config;

/**
 * Central registry of topic names and consumer-group ids so the naming stays
 * consistent across producers, listeners, and topic provisioning.
 */
public final class KafkaTopics {

    // --- RabbitMQ-style: point-to-point work queue -------------------------
    public static final String TASK_QUEUE_TOPIC = "task-queue-topic";
    public static final String WORKER_GROUP = "worker-group";

    // --- RabbitMQ-style: pub/sub fanout --------------------------------------
    public static final String BROADCAST_TOPIC = "broadcast-topic";
    public static final String GROUP_ANALYTICS = "group-analytics";
    public static final String GROUP_NOTIFICATIONS = "group-notifications";

    // --- Kafka-native: append-only event log with replay ---------------------
    public static final String ORDER_EVENTS_TOPIC = "order-events-topic";
    public static final String ORDER_EVENTS_LIVE_GROUP = "order-events-live-group";

    private KafkaTopics() {
    }
}
