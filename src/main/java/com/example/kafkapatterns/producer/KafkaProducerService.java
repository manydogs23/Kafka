package com.example.kafkapatterns.producer;

import com.example.kafkapatterns.config.MessagingRulesProperties;
import com.example.kafkapatterns.dto.BroadcastMessage;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.dto.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single producer service backing all three messaging patterns.
 * Topic names and partition counts come from {@code rules.yml}.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessagingRulesProperties rules;
    private final AtomicInteger taskPartitionCounter = new AtomicInteger();

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate, MessagingRulesProperties rules) {
        this.kafkaTemplate = kafkaTemplate;
        this.rules = rules;
    }

    /**
     * Work-queue produce: rotate across partitions so competing consumers
     * (E1/E2) both receive work in demos (avoids sticky partitioner bias).
     */
    public void sendTaskToQueue(TaskMessage message) {
        int partitions = rules.getPartitions().getTaskQueue();
        int partition = Math.floorMod(taskPartitionCounter.getAndIncrement(), partitions);
        String topic = rules.getTopics().getTaskQueue();
        kafkaTemplate.send(topic, partition, null, message);
        log.info("-> [{}] queued task {} partition={}", topic, message.taskId(), partition);
    }

    public void sendBroadcast(BroadcastMessage message) {
        String topic = rules.getTopics().getBroadcast();
        kafkaTemplate.send(topic, message);
        log.info("-> [{}] broadcast event {}", topic, message.eventId());
    }

    public void sendOrderEvent(String orderId, OrderEvent event) {
        String topic = rules.getTopics().getOrderEvents();
        kafkaTemplate.send(topic, orderId, event);
        log.info("-> [{}] {} for order {}", topic, event.eventType(), orderId);
    }
}
