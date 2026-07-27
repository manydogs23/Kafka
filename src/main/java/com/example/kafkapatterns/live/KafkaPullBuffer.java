package com.example.kafkapatterns.live;

import com.example.kafkapatterns.config.MessagingRulesProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Named pull inboxes for event-log users (D/D1/D2). Inbox names come from
 * {@code messaging.groups.order-pull} keys in rules.yml.
 */
@Component
public class KafkaPullBuffer {

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Map<String, Object>>> inboxes =
            new ConcurrentHashMap<>();
    private final String defaultInbox;

    public KafkaPullBuffer(MessagingRulesProperties rules) {
        Map<String, String> orderPull = rules.getGroups().getOrderPull();
        if (orderPull == null || orderPull.isEmpty()) {
            throw new IllegalStateException("messaging.groups.order-pull must define at least one inbox in rules.yml");
        }
        orderPull.keySet().forEach(id -> inboxes.put(id.toLowerCase(), new ConcurrentLinkedQueue<>()));
        this.defaultInbox = orderPull.keySet().iterator().next().toLowerCase();
    }

    public void offer(String inbox, Map<String, Object> message) {
        queue(inbox).offer(message);
    }

    public List<Map<String, Object>> drain(String inbox) {
        ConcurrentLinkedQueue<Map<String, Object>> q = queue(inbox);
        List<Map<String, Object>> batch = new ArrayList<>();
        Map<String, Object> next;
        while ((next = q.poll()) != null) {
            batch.add(next);
        }
        return batch;
    }

    public Set<String> inboxes() {
        return Set.copyOf(inboxes.keySet());
    }

    public String defaultInbox() {
        return defaultInbox;
    }

    public boolean isValid(String inbox) {
        return inboxes.containsKey(normalize(inbox));
    }

    public String normalize(String inbox) {
        if (inbox == null || inbox.isBlank()) {
            return defaultInbox;
        }
        return inbox.trim().toLowerCase();
    }

    private ConcurrentLinkedQueue<Map<String, Object>> queue(String inbox) {
        String key = normalize(inbox);
        ConcurrentLinkedQueue<Map<String, Object>> q = inboxes.get(key);
        if (q == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown pull inbox: " + inbox);
        }
        return q;
    }
}
