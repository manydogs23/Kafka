package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.live.KafkaPullBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * D / D1 / D2 pull inboxes — each {@code groupId} comes from
 * {@code messaging.groups.order-pull.*} in rules.yml.
 */
@Component
public class DemoOrderPullListener {

    private static final Logger log = LoggerFactory.getLogger(DemoOrderPullListener.class);

    private final KafkaPullBuffer kafkaPullBuffer;

    public DemoOrderPullListener(KafkaPullBuffer kafkaPullBuffer) {
        this.kafkaPullBuffer = kafkaPullBuffer;
    }

    @KafkaListener(
            id = "demo-order-pull-d",
            topics = "${messaging.topics.orderEvents}",
            groupId = "${messaging.groups.orderPull.d}")
    public void onForD(ConsumerRecord<String, OrderEvent> record) {
        offerIfNumber("d", record);
    }

    @KafkaListener(
            id = "demo-order-pull-d1",
            topics = "${messaging.topics.orderEvents}",
            groupId = "${messaging.groups.orderPull.d1}")
    public void onForD1(ConsumerRecord<String, OrderEvent> record) {
        offerIfNumber("d1", record);
    }

    @KafkaListener(
            id = "demo-order-pull-d2",
            topics = "${messaging.topics.orderEvents}",
            groupId = "${messaging.groups.orderPull.d2}")
    public void onForD2(ConsumerRecord<String, OrderEvent> record) {
        offerIfNumber("d2", record);
    }

    private void offerIfNumber(String inbox, ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        if (event == null || !"NUMBER".equals(event.eventType())) {
            return;
        }
        log.info("[demo-pull:{}] offset={} number={}", inbox, record.offset(), event.details());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderId", event.orderId());
        payload.put("number", event.details());
        payload.put("occurredAt", event.timestamp().toString());
        payload.put("partition", record.partition());
        payload.put("offset", record.offset());
        payload.put("inbox", inbox);
        kafkaPullBuffer.offer(inbox, payload);
    }
}
