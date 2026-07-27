# Project Structure

```
Kafka/
├── pom.xml                                    Maven build (Spring Boot 4.0.0, Java 21)
├── docker-compose.yml                         Kafka (KRaft) + Kafka UI (:8090) — local/dev
├── docker-compose-mq.yml                      Kafka + RabbitMQ (reserve) + Kafka UI — VM-oriented
│
├── docs/
│   ├── ARCHITECTURE.md                        Solution + sequence diagrams
│   ├── PROJECT_STRUCTURE.md                   This file
│   └── DEMO_DATAFLOWS.md                      Users A/B/C/D push vs pull dataflows
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
│   │   ├── OrderEvent.java / OrderEventRequest.java        order-events-topic payload / REST body
│   │   ├── WordsRequest.java                               User A demo body (words)
│   │   └── NumberRequest.java                              User C demo body (number)
│   │
│   ├── producer/
│   │   └── KafkaProducerService.java           sendTaskToQueue / sendBroadcast / sendOrderEvent
│   │
│   ├── consumer/
│   │   ├── QueueWorkerListener.java            2 listeners, SAME groupId=worker-group   → work queue
│   │   ├── FanoutListener.java                 2 listeners, DIFFERENT groupIds          → pub/sub fanout
│   │   │                                        (+ SSE push from group-notifications)
│   │   ├── OrderEventListener.java             manual-ack live listener                 → event log
│   │   │                                        (+ offer NUMBER events to pull buffer)
│   │   └── OrderEventReplayService.java        disposable consumer, seekToBeginning     → replay
│   │
│   ├── live/
│   │   ├── RabbitPushHub.java                  SSE hub: fanout → User B (auto push)
│   │   └── KafkaPullBuffer.java                In-memory queue: NUMBER events → User D pull
│   │
│   ├── state/
│   │   └── OrderStateStore.java                In-memory projection keyed by orderId, shared by
│   │                                            both the live listener and the replay path
│   │
│   └── controller/
│       ├── DemoController.java                 /api/v1/demo/*  (Users A/B/C/D)
│       ├── RabbitMqStyleController.java        POST /api/v1/rabbitmq/queue, /fanout
│       └── KafkaStreamController.java          POST /api/v1/kafka/stream, GET /stream/replay
│
├── src/main/resources/
│   ├── application.yml                         bootstrap-servers, (de)serializers, ack-mode,
│   │                                            annotated pattern-to-config cheat sheet
│   └── static/                                 Browser demo (served at :8080)
│       ├── index.html                          Hub linking Users A–D
│       ├── rabbit-a.html                       User A — type words (producer)
│       ├── rabbit-b.html                       User B — live SSE inbox (push)
│       ├── kafka-c.html                        User C — type numbers (producer)
│       ├── kafka-d.html                        User D — Fetch / Pull inbox
│       └── css/demo.css                        Shared demo styles
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
| Inbound (fanout) | `FanoutListener.java` | Proves broadcast delivery via independent `groupId`s; notifications group feeds SSE |
| Inbound (log) | `OrderEventListener.java` | Commit-after-apply processing (manual ack); buffers `NUMBER` events for pull |
| Replay | `OrderEventReplayService.java` | Rebuilds state on demand by rewinding to offset 0 |
| Projection | `OrderStateStore.java` | The thing both live consumption and replay converge on — proof that replay reproduces the same state |
| Live push | `RabbitPushHub.java` | Holds SSE emitters; pushes fanout payloads to User B |
| Live pull | `KafkaPullBuffer.java` | Concurrent queue drained by `GET /api/v1/demo/kafka/pull` |
| Demo API | `DemoController.java` | Words produce, SSE subscribe, numbers produce, pull drain |
| Pattern APIs | `RabbitMqStyleController` / `KafkaStreamController` | Original REST triggers for queue, fanout, stream, replay |
| Static UI | `static/*.html` | Four-user producer/consumer teaching pages |
| Docs | `docs/DEMO_DATAFLOWS.md` | Sequence diagrams for A→B push and C→D pull |
| Infra | `docker-compose.yml` | Single-node KRaft broker + Kafka UI on `:8090` |
| Infra (VM) | `docker-compose-mq.yml` | Kafka + reserved RabbitMQ + Kafka UI (VM / remote broker) |
| External client | `node-client/` | Same patterns exercised from outside the JVM, joining the identical consumer groups |

## Demo API cheat sheet

| User | Page | Endpoint | Role |
|---|---|---|---|
| A | `/rabbit-a.html` | `POST /api/v1/demo/rabbit/words` | Produce words → fanout |
| B | `/rabbit-b.html` | `GET /api/v1/demo/rabbit/live` (SSE) | Auto receive |
| C | `/kafka-c.html` | `POST /api/v1/demo/kafka/numbers` | Produce number → event log |
| D | `/kafka-d.html` | `GET /api/v1/demo/kafka/pull` | Explicit pull |
