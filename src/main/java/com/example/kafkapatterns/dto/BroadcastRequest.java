package com.example.kafkapatterns.dto;

/** Inbound payload for {@code POST /api/v1/rabbitmq/fanout}. */
public record BroadcastRequest(String content) {
}
