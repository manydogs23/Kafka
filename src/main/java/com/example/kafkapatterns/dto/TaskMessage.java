package com.example.kafkapatterns.dto;

import java.time.Instant;

/**
 * A unit of work published to {@code task-queue-topic}. Sent WITHOUT a
 * partition key so Kafka spreads tasks across partitions, letting the two
 * competing {@code worker-group} listener instances share the load like
 * consumers pulling from the same RabbitMQ queue.
 */
public record TaskMessage(String taskId, String payload, Instant createdAt) {
}
