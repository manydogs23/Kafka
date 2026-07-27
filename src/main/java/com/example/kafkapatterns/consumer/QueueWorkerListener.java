package com.example.kafkapatterns.consumer;

import com.example.kafkapatterns.dto.TaskMessage;
import com.example.kafkapatterns.live.QueuePullBuffer;
import com.example.kafkapatterns.live.QueuePushHub;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Work queue — both listeners share {@code messaging.groups.worker} from rules.yml.
 */
@Component
public class QueueWorkerListener {

    private static final Logger log = LoggerFactory.getLogger(QueueWorkerListener.class);

    private final QueuePushHub queuePushHub;
    private final QueuePullBuffer queuePullBuffer;
    private final String workerGroup;

    public QueueWorkerListener(QueuePushHub queuePushHub,
                               QueuePullBuffer queuePullBuffer,
                               @Value("${messaging.groups.worker}") String workerGroup) {
        this.queuePushHub = queuePushHub;
        this.queuePullBuffer = queuePullBuffer;
        this.workerGroup = workerGroup;
    }

    @KafkaListener(
            id = "worker-instance-1",
            topics = "${messaging.topics.taskQueue}",
            groupId = "${messaging.groups.worker}")
    public void workerOne(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage message = record.value();
        log.info("[worker-instance-1 | {}] partition={} offset={} task={} payload={}",
                workerGroup, record.partition(), record.offset(),
                message.taskId(), message.payload());
        queuePushHub.push(taskPayload(message, record, "worker-instance-1"));
    }

    @KafkaListener(
            id = "worker-instance-2",
            topics = "${messaging.topics.taskQueue}",
            groupId = "${messaging.groups.worker}")
    public void workerTwo(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage message = record.value();
        log.info("[worker-instance-2 | {}] partition={} offset={} task={} payload={}",
                workerGroup, record.partition(), record.offset(),
                message.taskId(), message.payload());
        queuePullBuffer.offer(taskPayload(message, record, "worker-instance-2"));
    }

    private static Map<String, Object> taskPayload(TaskMessage message,
                                                   ConsumerRecord<String, TaskMessage> record,
                                                   String worker) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", message.taskId());
        payload.put("payload", message.payload());
        payload.put("worker", worker);
        payload.put("partition", record.partition());
        payload.put("offset", record.offset());
        payload.put("occurredAt", message.createdAt().toString());
        return payload;
    }
}
