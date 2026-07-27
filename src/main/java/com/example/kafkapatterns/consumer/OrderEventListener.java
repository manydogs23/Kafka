package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.state.OrderStateStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Live event-log consumer — group from {@code messaging.groups.order-live}.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderStateStore orderStateStore;

    public OrderEventListener(OrderStateStore orderStateStore) {
        this.orderStateStore = orderStateStore;
    }

    @KafkaListener(
            id = "order-events-live",
            topics = "${messaging.topics.orderEvents}",
            groupId = "${messaging.groups.orderLive}",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent event = record.value();
        log.info("[order-events-live] partition={} offset={} orderId={} type={}",
                record.partition(), record.offset(), record.key(), event.eventType());
        orderStateStore.apply(event);
        ack.acknowledge();
    }
}
