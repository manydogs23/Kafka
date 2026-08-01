package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.dto.UserFootprintEvent;
import com.example.kafkapatterns.state.UserFootprintStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Live event-log consumer — group from {@code messaging.groups.userFootprintLive}.
 */
@Component
public class UserFootprintListener {

    private static final Logger log = LoggerFactory.getLogger(UserFootprintListener.class);

    private final UserFootprintStore userFootprintStore;

    public UserFootprintListener(UserFootprintStore userFootprintStore) {
        this.userFootprintStore = userFootprintStore;
    }

    @KafkaListener(
            id = "user-footprint-live",
            topics = "${messaging.topics.userFootprint}",
            groupId = "${messaging.groups.userFootprintLive}",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void onUserFootprintEvent(ConsumerRecord<String, UserFootprintEvent> record, Acknowledgment ack) {
        UserFootprintEvent event = record.value();
        log.info("[user-footprint-live] partition={} offset={} userId={} action={}",
                record.partition(), record.offset(), record.key(), event.action());
        userFootprintStore.apply(event);
        ack.acknowledge();
    }
}
