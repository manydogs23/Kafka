package com.example.kafkapatterns.controller;

import com.example.kafkapatterns.consumer.OrderEventReplayService;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.dto.OrderEventRequest;
import com.example.kafkapatterns.producer.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Triggers the Kafka-native event-log pattern: appending events to {@code
 * order-events-topic}, and separately, rebuilding application state by
 * replaying that log from offset 0.
 */
@RestController
@RequestMapping("/api/v1/kafka/stream")
public class KafkaStreamController {

    private final KafkaProducerService producerService;
    private final OrderEventReplayService replayService;

    public KafkaStreamController(KafkaProducerService producerService, OrderEventReplayService replayService) {
        this.producerService = producerService;
        this.replayService = replayService;
    }

    /** Appends a new immutable fact to the order-events log, keyed by orderId. */
    @PostMapping
    public ResponseEntity<OrderEvent> publishOrderEvent(@RequestBody OrderEventRequest request) {
        OrderEvent event = new OrderEvent(request.orderId(), request.eventType(), request.details(), Instant.now());
        producerService.sendOrderEvent(request.orderId(), event);
        return ResponseEntity.accepted().body(event);
    }

    /** Rewinds a throwaway consumer to offset 0 and rebuilds per-order state from scratch. */
    @GetMapping("/replay")
    public ResponseEntity<Map<String, List<OrderEvent>>> replay() {
        return ResponseEntity.ok(replayService.replayFromBeginning());
    }
}
