package com.example.kafkapatterns.dto;

/** User A → RabbitMQ-style fanout. */
public record WordsRequest(String words) {
}
