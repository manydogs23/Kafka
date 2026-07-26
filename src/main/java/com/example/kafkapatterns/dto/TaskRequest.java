package com.example.kafkapatterns.dto;

/** Inbound payload for {@code POST /api/v1/rabbitmq/queue}. */
public record TaskRequest(String payload) {
}
