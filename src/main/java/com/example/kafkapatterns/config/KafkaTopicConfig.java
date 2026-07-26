package com.example.kafkapatterns.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the three demo topics on startup via Spring Kafka's KafkaAdmin
 * auto-configuration. Partition counts are chosen to make each pattern
 * observable:
 *
 * <ul>
 *   <li>{@code task-queue-topic}: multiple partitions so the two
 *       {@code worker-group} listener instances actually compete for work
 *       instead of one sitting idle.</li>
 *   <li>{@code broadcast-topic}: partition count is irrelevant to fanout
 *       semantics -- every consumer GROUP gets a full copy of every
 *       partition's data regardless of how many partitions exist.</li>
 *   <li>{@code order-events-topic}: multiple partitions for throughput, with
 *       infinite retention so the log can always be replayed from offset 0
 *       to rebuild state.</li>
 * </ul>
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic taskQueueTopic() {
        return TopicBuilder.name(KafkaTopics.TASK_QUEUE_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic broadcastTopic() {
        return TopicBuilder.name(KafkaTopics.BROADCAST_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                // Infinite retention: this topic is the source of truth, not a
                // transient queue, so it must remain replayable from offset 0.
                .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
                .build();
    }
}
