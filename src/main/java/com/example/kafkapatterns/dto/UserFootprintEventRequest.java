package com.example.kafkapatterns.dto;

/** Inbound payload for {@code POST /api/v1/footprint}. */
public record UserFootprintEventRequest(String userId, String action, String details) {
}
