package com.example.kafkapatterns.state;

import com.example.kafkapatterns.dto.UserFootprintEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory projection of {@code user-footprint-topic}, keyed by userId. This
 * is intentionally rebuildable from nothing: both the live listener and the
 * offset-0 replay path funnel through {@link #apply(UserFootprintEvent)}, so
 * the same logic produces the same state whether events arrive in real time
 * or are replayed from the beginning of the log.
 */
@Component
public class UserFootprintStore {

    private final Map<String, List<UserFootprintEvent>> eventsByUser = new ConcurrentHashMap<>();

    public void apply(UserFootprintEvent event) {
        eventsByUser
                .computeIfAbsent(event.userId(), id -> new ArrayList<>())
                .add(event);
    }

    public List<UserFootprintEvent> forUser(String userId) {
        return List.copyOf(eventsByUser.getOrDefault(userId, List.of()));
    }

    public Map<String, List<UserFootprintEvent>> snapshot() {
        return new LinkedHashMap<>(eventsByUser);
    }

    public void reset() {
        eventsByUser.clear();
    }
}
