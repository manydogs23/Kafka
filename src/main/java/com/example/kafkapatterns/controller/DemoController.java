package com.example.kafkapatterns.controller;

import com.example.kafkapatterns.config.MessagingRulesProperties;
import com.example.kafkapatterns.dto.BroadcastMessage;
import com.example.kafkapatterns.dto.NumberRequest;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.dto.TaskMessage;
import com.example.kafkapatterns.dto.TaskPayloadRequest;
import com.example.kafkapatterns.dto.WordsRequest;
import com.example.kafkapatterns.live.KafkaPullBuffer;
import com.example.kafkapatterns.live.QueuePullBuffer;
import com.example.kafkapatterns.live.QueuePushHub;
import com.example.kafkapatterns.live.RabbitPushHub;
import com.example.kafkapatterns.producer.KafkaProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Browser demo APIs:
 * <ul>
 *   <li>Fanout push: A → B / B1 / B2 (SSE)</li>
 *   <li>Event-log pull: C → D / D1 / D2 (separate Kafka groups)</li>
 *   <li>Work queue: F → E1 (push) / E2 (pull), competing workers</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/demo")
public class DemoController {

    private final KafkaProducerService producerService;
    private final RabbitPushHub rabbitPushHub;
    private final KafkaPullBuffer kafkaPullBuffer;
    private final QueuePushHub queuePushHub;
    private final QueuePullBuffer queuePullBuffer;
    private final MessagingRulesProperties rules;

    public DemoController(KafkaProducerService producerService,
                          RabbitPushHub rabbitPushHub,
                          KafkaPullBuffer kafkaPullBuffer,
                          QueuePushHub queuePushHub,
                          QueuePullBuffer queuePullBuffer,
                          MessagingRulesProperties rules) {
        this.producerService = producerService;
        this.rabbitPushHub = rabbitPushHub;
        this.kafkaPullBuffer = kafkaPullBuffer;
        this.queuePushHub = queuePushHub;
        this.queuePullBuffer = queuePullBuffer;
        this.rules = rules;
    }

    // --- broadcast-topic: User A → B / B1 / B2 --------------------------------

    @PostMapping("/rabbit/words")
    public ResponseEntity<BroadcastMessage> publishWords(@RequestBody WordsRequest request) {
        String words = request.words() == null ? "" : request.words().trim();
        if (words.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        BroadcastMessage message = new BroadcastMessage(UUID.randomUUID().toString(), words, Instant.now());
        producerService.sendBroadcast(message);
        return ResponseEntity.accepted().body(message);
    }

    /** Shared SSE stream for Users B, B1, B2 (all open tabs get every fanout). */
    @GetMapping(path = "/rabbit/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter liveWords() {
        return rabbitPushHub.subscribe();
    }

    // --- order-events-topic: User C → D / D1 / D2 -----------------------------

    @PostMapping("/kafka/numbers")
    public ResponseEntity<OrderEvent> publishNumber(@RequestBody NumberRequest request) {
        String number = request.number() == null ? "" : request.number().trim();
        if (number.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        String orderId = "numbers";
        OrderEvent event = new OrderEvent(orderId, "NUMBER", number, Instant.now());
        producerService.sendOrderEvent(orderId, event);
        return ResponseEntity.accepted().body(event);
    }

    /**
     * Pull for a named inbox. Inbox ids come from {@code messaging.groups.order-pull}
     * keys in rules.yml (each has its own Kafka groupId).
     */
    @GetMapping("/kafka/pull/{user}")
    public ResponseEntity<Map<String, Object>> pullNumbers(@PathVariable String user) {
        String inbox = kafkaPullBuffer.normalize(user);
        if (!kafkaPullBuffer.isValid(inbox)) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Unknown pull user/inbox. Valid: " + kafkaPullBuffer.inboxes());
        }
        List<Map<String, Object>> messages = kafkaPullBuffer.drain(inbox);
        return ResponseEntity.ok(Map.of(
                "user", inbox,
                "count", messages.size(),
                "messages", messages
        ));
    }

    /** Convenience: pull the default inbox (first key in order-pull rules). */
    @GetMapping("/kafka/pull")
    public ResponseEntity<Map<String, Object>> pullNumbersDefault() {
        return pullNumbers(kafkaPullBuffer.defaultInbox());
    }

    /** Inspect loaded rules (topics, groups, users) from rules.yml. */
    @GetMapping("/rules")
    public ResponseEntity<MessagingRulesProperties> rules() {
        return ResponseEntity.ok(rules);
    }

    // --- task-queue-topic: User F → E1 (push) / E2 (pull) ---------------------

    @PostMapping("/queue/tasks")
    public ResponseEntity<TaskMessage> publishTask(@RequestBody TaskPayloadRequest request) {
        String payload = request.payload() == null ? "" : request.payload().trim();
        if (payload.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        TaskMessage message = new TaskMessage(UUID.randomUUID().toString(), payload, Instant.now());
        producerService.sendTaskToQueue(message);
        return ResponseEntity.accepted().body(message);
    }

    /** User E1: SSE — only tasks handled by worker-instance-1. */
    @GetMapping(path = "/queue/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter liveTasks() {
        return queuePushHub.subscribe();
    }

    /** User E2: pull — only tasks handled by worker-instance-2. */
    @GetMapping("/queue/pull")
    public ResponseEntity<Map<String, Object>> pullTasks() {
        List<Map<String, Object>> messages = queuePullBuffer.drain();
        return ResponseEntity.ok(Map.of(
                "count", messages.size(),
                "messages", messages
        ));
    }
}
