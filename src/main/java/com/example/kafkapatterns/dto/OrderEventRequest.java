package com.example.kafkapatterns.dto;

/** Inbound payload for {@code POST /api/v1/kafka/stream}. */
public record OrderEventRequest(String orderId, String eventType, String details) {
}
