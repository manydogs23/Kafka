package com.example.kafkapatterns.dto;

import java.time.Instant;

/**
 * An event published to {@code broadcast-topic}. Every consumer group
 * (analytics, notifications, ...) receives its own full copy of every
 * message, mirroring a RabbitMQ fanout exchange.
 */
public record BroadcastMessage(String eventId, String content, Instant createdAt) {
}
