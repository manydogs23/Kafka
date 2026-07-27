package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.config.MessagingRulesProperties;
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
 * Replay from offset 0 using a disposable group id
 * ({@code messaging.groups.order-replay-prefix} + UUID).
 */
@Service
public class OrderEventReplayService {

    private static final Logger log = LoggerFactory.getLogger(OrderEventReplayService.class);

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final OrderStateStore orderStateStore;
    private final MessagingRulesProperties rules;

    public OrderEventReplayService(ConsumerFactory<Object, Object> consumerFactory,
                                   OrderStateStore orderStateStore,
                                   MessagingRulesProperties rules) {
        this.consumerFactory = consumerFactory;
        this.orderStateStore = orderStateStore;
        this.rules = rules;
    }

    public Map<String, List<OrderEvent>> replayFromBeginning() {
        orderStateStore.reset();

        String topic = rules.getTopics().getOrderEvents();
        String groupId = rules.getGroups().getOrderReplayPrefix() + UUID.randomUUID();

        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<Object, Object> consumer = consumerFactory.createConsumer(null, null, null, overrides)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(PartitionInfo::partition)
                    .map(partition -> new TopicPartition(topic, partition))
                    .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

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

            log.info("[replay] group={} rebuilt state from offset 0: {} events across {} partitions",
                    groupId, replayedCount, partitions.size());
            return orderStateStore.snapshot();
        }
    }
}
