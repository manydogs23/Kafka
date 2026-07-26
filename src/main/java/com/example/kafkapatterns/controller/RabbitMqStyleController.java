package com.example.kafkapatterns.controller;

import com.example.kafkapatterns.dto.BroadcastMessage;
import com.example.kafkapatterns.dto.BroadcastRequest;
import com.example.kafkapatterns.dto.TaskMessage;
import com.example.kafkapatterns.dto.TaskRequest;
import com.example.kafkapatterns.producer.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Triggers the two RabbitMQ-style patterns: a competing-consumer work queue
 * and a pub/sub fanout, both implemented on top of Kafka.
 */
@RestController
@RequestMapping("/api/v1/rabbitmq")
public class RabbitMqStyleController {

    private final KafkaProducerService producerService;

    public RabbitMqStyleController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    /** Work queue: exactly one of the two worker-group listeners will process this. */
    @PostMapping("/queue")
    public ResponseEntity<TaskMessage> publishToQueue(@RequestBody TaskRequest request) {
        TaskMessage message = new TaskMessage(UUID.randomUUID().toString(), request.payload(), Instant.now());
        producerService.sendTaskToQueue(message);
        return ResponseEntity.accepted().body(message);
    }

    /** Fanout: both group-analytics and group-notifications will each receive this. */
    @PostMapping("/fanout")
    public ResponseEntity<BroadcastMessage> publishFanout(@RequestBody BroadcastRequest request) {
        BroadcastMessage message = new BroadcastMessage(UUID.randomUUID().toString(), request.content(), Instant.now());
        producerService.sendBroadcast(message);
        return ResponseEntity.accepted().body(message);
    }
}
