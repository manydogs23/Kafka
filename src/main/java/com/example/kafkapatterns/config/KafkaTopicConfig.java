package com.example.kafkapatterns.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares demo topics from {@link MessagingRulesProperties} (rules.yml).
 */
@Configuration
public class KafkaTopicConfig {

    private final MessagingRulesProperties rules;

    public KafkaTopicConfig(MessagingRulesProperties rules) {
        this.rules = rules;
    }

    @Bean
    public NewTopic taskQueueTopic() {
        return TopicBuilder.name(rules.getTopics().getTaskQueue())
                .partitions(rules.getPartitions().getTaskQueue())
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic broadcastTopic() {
        return TopicBuilder.name(rules.getTopics().getBroadcast())
                .partitions(rules.getPartitions().getBroadcast())
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(rules.getTopics().getOrderEvents())
                .partitions(rules.getPartitions().getOrderEvents())
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
                .build();
    }

    @Bean
    public NewTopic userFootprintTopic() {
        return TopicBuilder.name(rules.getTopics().getUserFootprint())
                .partitions(rules.getPartitions().getUserFootprint())
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
                .build();
    }
}
