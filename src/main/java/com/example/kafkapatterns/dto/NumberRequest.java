package com.example.kafkapatterns.dto;

/** User C → Kafka-style event stream. */
public record NumberRequest(String number) {
}
