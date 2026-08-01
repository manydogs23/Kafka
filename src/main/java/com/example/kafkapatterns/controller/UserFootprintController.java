package com.example.kafkapatterns.controller;

import com.example.kafkapatterns.consumer.UserFootprintReplayService;
import com.example.kafkapatterns.dto.UserFootprintEvent;
import com.example.kafkapatterns.dto.UserFootprintEventRequest;
import com.example.kafkapatterns.producer.KafkaProducerService;
import com.example.kafkapatterns.state.UserFootprintStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Triggers the Kafka-native event-log pattern for tracking a user's
 * operating footprint: appending actions to {@code user-footprint-topic},
 * reading the live per-user projection, and separately, rebuilding it from
 * scratch by replaying the log from offset 0.
 */
@RestController
@RequestMapping("/api/v1/footprint")
public class UserFootprintController {

    private final KafkaProducerService producerService;
    private final UserFootprintReplayService replayService;
    private final UserFootprintStore footprintStore;

    public UserFootprintController(KafkaProducerService producerService,
                                    UserFootprintReplayService replayService,
                                    UserFootprintStore footprintStore) {
        this.producerService = producerService;
        this.replayService = replayService;
        this.footprintStore = footprintStore;
    }

    /** Appends a new immutable fact to the user-footprint log, keyed by userId. */
    @PostMapping
    public ResponseEntity<UserFootprintEvent> publishUserFootprintEvent(@RequestBody UserFootprintEventRequest request) {
        UserFootprintEvent event = new UserFootprintEvent(request.userId(), request.action(), request.details(), Instant.now());
        producerService.sendUserFootprintEvent(request.userId(), event);
        return ResponseEntity.accepted().body(event);
    }

    /** Live projection for every user seen so far, built by the always-running listener. */
    @GetMapping
    public ResponseEntity<Map<String, List<UserFootprintEvent>>> allFootprints() {
        return ResponseEntity.ok(footprintStore.snapshot());
    }

    /** Live projection for a single user, built by the always-running listener. */
    @GetMapping("/{userId}")
    public ResponseEntity<List<UserFootprintEvent>> footprintForUser(@PathVariable String userId) {
        return ResponseEntity.ok(footprintStore.forUser(userId));
    }

    /** Rewinds a throwaway consumer to offset 0 and rebuilds every user's footprint from scratch. */
    @GetMapping("/replay")
    public ResponseEntity<Map<String, List<UserFootprintEvent>>> replay() {
        return ResponseEntity.ok(replayService.replayFromBeginning());
    }
}
