# Project Structure

```
Kafka/
├── pom.xml                                    Maven build (Spring Boot 4.0.0, Java 21)
├── docker-compose.yml                         Kafka (KRaft, no Zookeeper) + Kafka UI
│
├── src/main/java/com/example/kafkapatterns/
│   ├── KafkaPatternsApplication.java          Boot entry point
│   │
│   ├── config/
│   │   ├── KafkaTopics.java                   Topic names + consumer-group ids (single source of truth)
│   │   ├── KafkaTopicConfig.java               NewTopic beans: partitions, replication, retention
│   │   └── KafkaConsumerConfig.java            manualAckKafkaListenerContainerFactory bean
│   │
│   ├── dto/                                    Java 21 records (immutable wire types)
│   │   ├── TaskMessage.java / TaskRequest.java             task-queue-topic payload / REST body
│   │   ├── BroadcastMessage.java / BroadcastRequest.java   broadcast-topic payload / REST body
│   │   └── OrderEvent.java / OrderEventRequest.java        order-events-topic payload / REST body
│   │
│   ├── producer/
│   │   └── KafkaProducerService.java           sendTaskToQueue / sendBroadcast / sendOrderEvent
│   │
│   ├── consumer/
│   │   ├── QueueWorkerListener.java            2 listeners, SAME groupId=worker-group   → work queue
│   │   ├── FanoutListener.java                 2 listeners, DIFFERENT groupIds          → pub/sub fanout
│   │   ├── OrderEventListener.java             manual-ack live listener                 → event log
│   │   └── OrderEventReplayService.java        disposable consumer, seekToBeginning     → replay
│   │
│   ├── state/
│   │   └── OrderStateStore.java                In-memory projection keyed by orderId, shared by
│   │                                            both the live listener and the replay path
│   │
│   └── controller/
│       ├── RabbitMqStyleController.java        POST /api/v1/rabbitmq/queue, /fanout
│       └── KafkaStreamController.java          POST /api/v1/kafka/stream, GET /stream/replay
│
├── src/main/resources/
│   └── application.yml                         bootstrap-servers, (de)serializers, ack-mode,
│                                                annotated pattern-to-config cheat sheet
│
└── node-client/                                External, out-of-JVM producer/consumer
    ├── package.json                            kafkajs dependency + npm run scripts per mode
    └── index.js                                Direct kafkajs producers/consumers +
                                                 HTTP-trigger alternative, mode-selected via argv
```

## File responsibilities at a glance

| Layer | File | Responsibility |
|---|---|---|
| Entry point | `KafkaPatternsApplication.java` | `@SpringBootApplication` bootstrap |
| Naming | `KafkaTopics.java` | Constants shared by config, producer, and every listener — no magic strings |
| Provisioning | `KafkaTopicConfig.java` | Declares partition counts and retention per topic on startup |
| Container tuning | `KafkaConsumerConfig.java` | Adds a MANUAL ack-mode factory for the log-style listener, distinct from Boot's auto-configured default |
| Wire types | `dto/*.java` | Java records: immutable, no boilerplate getters/equals/hashCode |
| Outbound | `KafkaProducerService.java` | Encodes the *only* real difference between the two families: whether a partition key is set |
| Inbound (queue) | `QueueWorkerListener.java` | Proves competing-consumer delivery via shared `groupId` |
| Inbound (fanout) | `FanoutListener.java` | Proves broadcast delivery via independent `groupId`s |
| Inbound (log) | `OrderEventListener.java` | Commit-after-apply processing (manual ack) instead of auto-commit |
| Replay | `OrderEventReplayService.java` | Rebuilds state on demand by rewinding to offset 0 |
| Projection | `OrderStateStore.java` | The thing both live consumption and replay converge on — proof that replay reproduces the same state |
| API | `controller/*.java` | REST triggers for all three patterns plus the replay endpoint |
| Infra | `docker-compose.yml` | Single-node KRaft broker + optional Kafka UI on `:8090` |
| External client | `node-client/` | Same patterns exercised from outside the JVM, joining the identical consumer groups |
