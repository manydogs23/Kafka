package com.example.kafkapatterns.state;

import com.example.kafkapatterns.dto.OrderEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory projection of {@code order-events-topic}, keyed by orderId. This
 * is intentionally rebuildable from nothing: both the live listener and the
 * offset-0 replay path funnel through {@link #apply(OrderEvent)}, so the
 * same logic produces the same state whether events arrive in real time or
 * are replayed from the beginning of the log.
 */
@Component
public class OrderStateStore {

    private final Map<String, List<OrderEvent>> eventsByOrder = new ConcurrentHashMap<>();

    public void apply(OrderEvent event) {
        eventsByOrder
                .computeIfAbsent(event.orderId(), id -> new ArrayList<>())
                .add(event);
    }

    public Map<String, List<OrderEvent>> snapshot() {
        return new LinkedHashMap<>(eventsByOrder);
    }

    public void reset() {
        eventsByOrder.clear();
    }
}
