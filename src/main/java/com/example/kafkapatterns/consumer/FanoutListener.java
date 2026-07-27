package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.dto.BroadcastMessage;
import com.example.kafkapatterns.live.RabbitPushHub;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fanout — group ids from rules.yml ({@code messaging.groups.analytics/notifications}).
 */
@Component
public class FanoutListener {

    private static final Logger log = LoggerFactory.getLogger(FanoutListener.class);

    private final RabbitPushHub rabbitPushHub;
    private final String analyticsGroup;
    private final String notificationsGroup;

    public FanoutListener(RabbitPushHub rabbitPushHub,
                          @Value("${messaging.groups.analytics}") String analyticsGroup,
                          @Value("${messaging.groups.notifications}") String notificationsGroup) {
        this.rabbitPushHub = rabbitPushHub;
        this.analyticsGroup = analyticsGroup;
        this.notificationsGroup = notificationsGroup;
    }

    @KafkaListener(
            id = "analytics-listener",
            topics = "${messaging.topics.broadcast}",
            groupId = "${messaging.groups.analytics}")
    public void onAnalyticsEvent(ConsumerRecord<String, BroadcastMessage> record) {
        BroadcastMessage message = record.value();
        log.info("[{}] received event={} content={}",
                analyticsGroup, message.eventId(), message.content());
    }

    @KafkaListener(
            id = "notifications-listener",
            topics = "${messaging.topics.broadcast}",
            groupId = "${messaging.groups.notifications}")
    public void onNotificationsEvent(ConsumerRecord<String, BroadcastMessage> record) {
        BroadcastMessage message = record.value();
        log.info("[{}] received event={} content={}",
                notificationsGroup, message.eventId(), message.content());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventId", message.eventId());
        payload.put("words", message.content());
        payload.put("occurredAt", message.createdAt().toString());
        rabbitPushHub.push(payload);
    }
}
