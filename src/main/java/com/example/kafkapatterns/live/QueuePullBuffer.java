package com.example.kafkapatterns.live;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pull buffer for User E2 — tasks processed by worker-instance-2 only.
 */
@Component
public class QueuePullBuffer {

    private final ConcurrentLinkedQueue<Map<String, Object>> pending = new ConcurrentLinkedQueue<>();

    public void offer(Map<String, Object> message) {
        pending.offer(message);
    }

    public List<Map<String, Object>> drain() {
        List<Map<String, Object>> batch = new ArrayList<>();
        Map<String, Object> next;
        while ((next = pending.poll()) != null) {
            batch.add(next);
        }
        return batch;
    }
}
