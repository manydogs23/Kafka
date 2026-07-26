package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.config.KafkaTopics;
import com.example.kafkapatterns.dto.TaskMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ-style WORK QUEUE (point-to-point).
 *
 * Both listeners below share the SAME {@code groupId} ({@value
 * KafkaTopics#WORKER_GROUP}). Kafka assigns each partition of {@code
 * task-queue-topic} to exactly one member of the group, so a given message
 * is delivered to only ONE of these two methods -- never both -- just like
 * two competing consumers draining a single RabbitMQ queue. Kill one
 * instance and the broker rebalances its partitions onto the survivor.
 */
@Component
public class QueueWorkerListener {

    private static final Logger log = LoggerFactory.getLogger(QueueWorkerListener.class);

    @KafkaListener(
            id = "worker-instance-1",
            topics = KafkaTopics.TASK_QUEUE_TOPIC,
            groupId = KafkaTopics.WORKER_GROUP)
    public void workerOne(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage message = record.value();
        log.info("[worker-instance-1 | {}] partition={} offset={} task={} payload={}",
                KafkaTopics.WORKER_GROUP, record.partition(), record.offset(),
                message.taskId(), message.payload());
    }

    @KafkaListener(
            id = "worker-instance-2",
            topics = KafkaTopics.TASK_QUEUE_TOPIC,
            groupId = KafkaTopics.WORKER_GROUP)
    public void workerTwo(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage message = record.value();
        log.info("[worker-instance-2 | {}] partition={} offset={} task={} payload={}",
                KafkaTopics.WORKER_GROUP, record.partition(), record.offset(),
                message.taskId(), message.payload());
    }
}
