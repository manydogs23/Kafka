package com.example.kafkapatterns.dto;

/**
 * User F (queue producer) → task-queue-topic. {@code key} is optional: when
 * blank/absent, the task is round-robined across partitions (no key); when
 * present, it's sent keyed so every task sharing that key lands on the same
 * partition, and therefore the same worker, every time.
 */
public record TaskPayloadRequest(String payload, String key) {
}
