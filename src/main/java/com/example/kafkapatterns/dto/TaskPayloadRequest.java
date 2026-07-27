package com.example.kafkapatterns.dto;

/** User F (queue producer) → task-queue-topic. */
public record TaskPayloadRequest(String payload) {
}
