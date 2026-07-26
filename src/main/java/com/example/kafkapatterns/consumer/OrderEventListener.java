package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.config.KafkaTopics;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.state.OrderStateStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka-native EVENT LOG consumption.
 *
 * Unlike the queue/fanout listeners (which auto-commit), this listener runs
 * on the {@code manualAckKafkaListenerContainerFactory} and only commits an
 * offset once the event has been durably applied to {@link OrderStateStore}.
 * This is the log-processing mindset: the offset is a bookmark into an
 * immutable, replayable history, not a "delete after read" pointer the way
 * a queue message ack would be.
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
            topics = KafkaTopics.ORDER_EVENTS_TOPIC,
            groupId = KafkaTopics.ORDER_EVENTS_LIVE_GROUP,
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent event = record.value();
        log.info("[order-events-live] partition={} offset={} orderId={} type={}",
                record.partition(), record.offset(), record.key(), event.eventType());

        orderStateStore.apply(event);

        // Manual commit: only advance the offset once state has been applied.
        ack.acknowledge();
    }
}
