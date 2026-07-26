package com.example.kafkapatterns.dto;

import java.time.Instant;

/**
 * An immutable fact appended to {@code order-events-topic}. Published WITH
 * {@code orderId} as the partition key so every event for a given order lands
 * on the same partition, preserving per-order ordering when the log is
 * replayed from offset 0 to rebuild state.
 */
public record OrderEvent(String orderId, String eventType, String details, Instant timestamp) {
}
