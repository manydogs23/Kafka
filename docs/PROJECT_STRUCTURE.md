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
│   └── DEMO_DATAFLOWS.md                      Demo flows for fanout, event-log, and queue pages
│
├── src/main/java/com/example/kafkapatterns/
│   ├── KafkaPatternsApplication.java          Boot entry point
│   │
│   ├── config/
│   │   ├── MessagingRulesProperties.java      Binds `rules.yml` into typed Java config
│   │   ├── KafkaTopicConfig.java              NewTopic beans from rules.yml
│   │   └── KafkaConsumerConfig.java           manualAckKafkaListenerContainerFactory bean
│   │
│   ├── dto/                                    Java 21 records (immutable wire types)
│   │   ├── TaskMessage.java / TaskRequest.java             task-queue-topic payload / REST body
│   │   ├── TaskPayloadRequest.java                         User F demo body (task payload)
│   │   ├── BroadcastMessage.java / BroadcastRequest.java   broadcast-topic payload / REST body
│   │   ├── OrderEvent.java / OrderEventRequest.java        order-events-topic payload / REST body
│   │   ├── UserFootprintEvent.java / UserFootprintEventRequest.java  user-footprint-topic payload / REST body
│   │   ├── WordsRequest.java                               User A demo body (words)
│   │   └── NumberRequest.java                              User C demo body (number)
│   │
│   ├── producer/
│   │   └── KafkaProducerService.java          sendTaskToQueue / sendBroadcast / sendOrderEvent
│   │                                            using topic names from rules.yml
│   │
│   ├── consumer/
│   │   ├── QueueWorkerListener.java           2 listeners, SAME rules.yml group         → work queue
│   │   │                                        worker-1 pushes to E1, worker-2 buffers E2
│   │   ├── FanoutListener.java                2 listeners, DIFFERENT rules.yml groups   → fanout
│   │   │                                        notifications group feeds SSE to B/B1/B2
│   │   ├── OrderEventListener.java            manual-ack live listener                  → event log
│   │   ├── DemoOrderPullListener.java         extra pull groups for D / D1 / D2
│   │   ├── OrderEventReplayService.java       disposable replay consumer                → replay
│   │   ├── UserFootprintListener.java         manual-ack live listener                  → user footprint log
│   │   └── UserFootprintReplayService.java    disposable replay consumer                → footprint replay
│   │
│   ├── live/
│   │   ├── RabbitPushHub.java                 SSE hub: fanout → B / B1 / B2
│   │   ├── QueuePushHub.java                  SSE hub: worker-1 → E1
│   │   ├── QueuePullBuffer.java               Pull buffer: worker-2 → E2
│   │   └── KafkaPullBuffer.java               Named pull inboxes: D / D1 / D2
│   │
│   ├── state/
│   │   ├── OrderStateStore.java               In-memory projection keyed by orderId, shared by
│   │   │                                       live consume and replay
│   │   └── UserFootprintStore.java            In-memory projection keyed by userId, shared by
│   │                                           live consume and replay
│   │
│   └── controller/
│       ├── DemoController.java                /api/v1/demo/*  (A/B/B1/B2/C/D/D1/D2/E1/E2/F)
│       │                                        each action also logs a footprint event (see G)
│       ├── RabbitMqStyleController.java       POST /api/v1/rabbitmq/queue, /fanout
│       ├── KafkaStreamController.java         POST /api/v1/kafka/stream, GET /stream/replay
│       └── UserFootprintController.java       POST /api/v1/footprint, GET /footprint (all), /footprint/{userId}, /footprint/replay
│
├── src/main/resources/
│   ├── application.yml                        bootstrap-servers, serializers, ack-mode,
│   │                                           imports `rules.yml`
│   ├── rules.yml                              Source of truth for topics, partitions, groups, users
│   └── static/                                Browser demo (served at :8080)
│       ├── index.html                         Home / entry page
│       ├── rabbit-a.html                      User A — fanout producer
│       ├── rabbit-b.html                      User B — fanout SSE consumer
│       ├── rabbit-b1.html                     User B1 — same SSE stream as B
│       ├── rabbit-b2.html                     User B2 — same SSE stream as B
│       ├── kafka-c.html                       User C — event-log producer
│       ├── kafka-d.html                       User D — pull inbox `d`
│       ├── kafka-d1.html                      User D1 — pull inbox `d1`
│       ├── kafka-d2.html                      User D2 — pull inbox `d2`
│       ├── queue-f.html                       User F — work-queue producer
│       ├── queue-e1.html                      User E1 — push inbox from worker-1
│       ├── queue-e2.html                      User E2 — pull inbox from worker-2
│       ├── footprint-g.html                   User G — footprint producer + live/replay viewer
│       └── css/demo.css                       Shared demo styles
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
| Rules | `rules.yml` | Editable source of truth for topics, groups, partitions, and browser-user mappings |
| Rules binding | `MessagingRulesProperties.java` | Loads `rules.yml` into typed Spring config |
| Provisioning | `KafkaTopicConfig.java` | Declares topic names and partition counts from rules.yml |
| Container tuning | `KafkaConsumerConfig.java` | Adds a MANUAL ack-mode factory for the log-style listener, distinct from Boot's auto-configured default |
| Wire types | `dto/*.java` | Java records: immutable, no boilerplate getters/equals/hashCode |
| Outbound | `KafkaProducerService.java` | Sends queue/fanout/log records using topic names from rules.yml; manually round-robins queue partitions |
| Inbound (queue) | `QueueWorkerListener.java` | Competing consumers in the shared worker group; routes worker-1 to E1 and worker-2 to E2 |
| Inbound (fanout) | `FanoutListener.java` | Independent fanout groups; notifications group feeds SSE to B/B1/B2 |
| Inbound (log, live) | `OrderEventListener.java` | Commit-after-apply processing for the shared `OrderStateStore` |
| Inbound (log, demo pull) | `DemoOrderPullListener.java` | Three additional Kafka groups for D / D1 / D2 inboxes |
| Replay | `OrderEventReplayService.java` | Rebuilds state from offset 0 with a disposable group prefix from rules.yml |
| Projection | `OrderStateStore.java` | Shared state rebuilt by both live consume and replay |
| Inbound (footprint, live) | `UserFootprintListener.java` | Commit-after-apply processing for the shared `UserFootprintStore` |
| Replay (footprint) | `UserFootprintReplayService.java` | Rebuilds per-user footprint from offset 0 with a disposable group prefix from rules.yml |
| Projection (footprint) | `UserFootprintStore.java` | Shared per-user state rebuilt by both live consume and replay |
| Live push (fanout) | `RabbitPushHub.java` | SSE broadcaster for B / B1 / B2 |
| Live push (queue) | `QueuePushHub.java` | SSE broadcaster for E1 |
| Live pull (queue) | `QueuePullBuffer.java` | In-memory queue drained by E2 |
| Live pull (event log) | `KafkaPullBuffer.java` | Named inboxes drained by D / D1 / D2 |
| Demo API | `DemoController.java` | Fanout produce/SSE, event-log produce/pull, queue produce/push/pull, `/rules` inspection, and (via `logFootprint()`) a footprint event per action, keyed by persona |
| Pattern APIs | `RabbitMqStyleController` / `KafkaStreamController` | Original REST triggers for queue, fanout, stream, replay |
| Footprint API | `UserFootprintController.java` | `POST /api/v1/footprint`, `GET /footprint` (all, live), `GET /footprint/{userId}` (one, live), `GET /footprint/replay` (all, rebuilt from offset 0) |
| Static UI | `static/*.html` | 12-user teaching pages: A/B/B1/B2, C/D/D1/D2, F/E1/E2, G |
| Docs | `docs/DEMO_DATAFLOWS.md` | Detailed sequence diagrams for current browser demo |
| Infra | `docker-compose.yml` | Single-node KRaft broker + Kafka UI on `:8090` |
| Infra (VM) | `docker-compose-mq.yml` | Kafka + reserved RabbitMQ + Kafka UI (VM / remote broker) |
| External client | `node-client/` | Same patterns exercised from outside the JVM, joining the identical consumer groups |

## Demo API cheat sheet

| User | Page | Endpoint | Role |
|---|---|---|---|
| A | `/rabbit-a.html` | `POST /api/v1/demo/rabbit/words` | Produce words → fanout |
| B / B1 / B2 | `/rabbit-b*.html` | `GET /api/v1/demo/rabbit/live` | Receive the same SSE fanout stream |
| C | `/kafka-c.html` | `POST /api/v1/demo/kafka/numbers` | Produce number → event log |
| D | `/kafka-d.html` | `GET /api/v1/demo/kafka/pull/d` | Pull inbox `d` |
| D1 | `/kafka-d1.html` | `GET /api/v1/demo/kafka/pull/d1` | Pull inbox `d1` |
| D2 | `/kafka-d2.html` | `GET /api/v1/demo/kafka/pull/d2` | Pull inbox `d2` |
| F | `/queue-f.html` | `POST /api/v1/demo/queue/tasks` | Produce work-queue tasks; optional `key` demonstrates keyed vs round-robin partitioning |
| E1 | `/queue-e1.html` | `GET /api/v1/demo/queue/live` | Receive tasks handled by worker-1 |
| E2 | `/queue-e2.html` | `GET /api/v1/demo/queue/pull` | Pull tasks handled by worker-2 |
| G | `/footprint-g.html` | `POST /api/v1/footprint`, `GET /footprint` / `/footprint/{userId}`, `GET /footprint/replay` | Log a userId-keyed action; read one user's live footprint, or all of them if the User ID field is left blank; replay everyone from offset 0 |

## Notes

- Browser users are demo personas, not direct Kafka clients.
- Kafka group membership is defined by listener config placeholders backed by `rules.yml`.
- `GET /api/v1/demo/rules` returns the currently loaded rules at runtime.
