package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.config.KafkaTopics;
import com.example.kafkapatterns.dto.OrderEvent;
import com.example.kafkapatterns.state.OrderStateStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka-native REPLAYABILITY demo.
 *
 * Spins up a throwaway consumer with a brand-new, never-seen-before group
 * id, assigns it every partition of {@code order-events-topic} directly
 * (bypassing group-coordinated subscription), and seeks to offset 0 on each
 * one. Because the topic has infinite retention (see {@link
 * com.example.kafkapatterns.config.KafkaTopicConfig}), the entire history is
 * still there to read -- this is the capability a RabbitMQ queue fundamentally
 * does not have once a message is acked and deleted.
 */
@Service
public class OrderEventReplayService {

    private static final Logger log = LoggerFactory.getLogger(OrderEventReplayService.class);

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final OrderStateStore orderStateStore;

    public OrderEventReplayService(ConsumerFactory<Object, Object> consumerFactory, OrderStateStore orderStateStore) {
        this.consumerFactory = consumerFactory;
        this.orderStateStore = orderStateStore;
    }

    public Map<String, List<OrderEvent>> replayFromBeginning() {
        orderStateStore.reset();

        Properties overrides = new Properties();
        // Unique, disposable group id: this replay must never share -- or
        // disturb -- the committed offsets of the live "order-events-live" group.
        overrides.put(ConsumerConfig.GROUP_ID_CONFIG, "order-events-replay-" + UUID.randomUUID());
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<Object, Object> consumer = consumerFactory.createConsumer(null, null, null, overrides)) {
            List<TopicPartition> partitions = consumer.partitionsFor(KafkaTopics.ORDER_EVENTS_TOPIC).stream()
                    .map(PartitionInfo::partition)
                    .map(partition -> new TopicPartition(KafkaTopics.ORDER_EVENTS_TOPIC, partition))
                    .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions); // rewind every partition to offset 0

            int replayedCount = 0;
            boolean keepPolling = true;
            while (keepPolling) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    keepPolling = false;
                    continue;
                }
                for (ConsumerRecord<Object, Object> record : records) {
                    orderStateStore.apply((OrderEvent) record.value());
                    replayedCount++;
                }
            }

            log.info("[replay] rebuilt state from offset 0: {} events across {} partitions",
                    replayedCount, partitions.size());
            return orderStateStore.snapshot();
        }
    }
}
