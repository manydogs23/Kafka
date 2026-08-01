package com.example.kafkapatterns.dto;

import java.time.Instant;

/**
 * An immutable fact appended to {@code user-footprint-topic}. Published WITH
 * {@code userId} as the partition key so every action by a given user lands
 * on the same partition, preserving per-user ordering when the log is
 * replayed from offset 0 to rebuild their footprint.
 */
public record UserFootprintEvent(String userId, String action, String details, Instant timestamp) {
}
