# Architecture

## 1. Solution Architecture (SA)

The service is a single Spring Boot process that talks to one Kafka broker. What changes between the three patterns is never the transport — it's **partition key presence** and **consumer group topology**.

A browser demo layer sits on top of two of those patterns:

- **RabbitMQ-style fanout → push to User B** (SSE via `RabbitPushHub`)
- **Kafka-native event log → pull by User D** (in-memory `KafkaPullBuffer`)

See also [DEMO_DATAFLOWS.md](DEMO_DATAFLOWS.md) for the A/B/C/D page sequences.

```mermaid
flowchart TB
    subgraph Clients["Callers"]
        Browser["Browser demo<br/>Users A/B/C/D"]
        REST["REST client<br/>(curl / Postman)"]
        NodeProd["Node.js client<br/>(kafkajs producer)"]
    end

    subgraph App["Spring Boot 4 app :8080"]
        DemoCtrl["DemoController<br/>/api/v1/demo/*"]
        RabbitCtrl["RabbitMqStyleController<br/>/api/v1/rabbitmq/*"]
        StreamCtrl["KafkaStreamController<br/>/api/v1/kafka/stream*"]
        Producer["KafkaProducerService"]

        Hub["RabbitPushHub<br/>(SSE)"]
        Buf["KafkaPullBuffer<br/>(drain-on-pull)"]

        W1["QueueWorkerListener<br/>workerOne()"]
        W2["QueueWorkerListener<br/>workerTwo()"]
        FA["FanoutListener<br/>onAnalyticsEvent()"]
        FN["FanoutListener<br/>onNotificationsEvent()"]
        OE["OrderEventListener<br/>(manual ack)"]
        Replay["OrderEventReplayService<br/>(disposable consumer)"]
        Store["OrderStateStore<br/>(in-memory projection)"]
    end

    subgraph Kafka["Kafka broker :9092 (KRaft)"]
        T1["task-queue-topic<br/>3 partitions"]
        T2["broadcast-topic<br/>3 partitions"]
        T3["order-events-topic<br/>3 partitions, retention=-1"]
    end

    subgraph NodeCons["Node.js client (optional extra consumer)"]
        NC["consume-worker / consume-analytics /<br/>consume-notifications"]
    end

    Browser --> DemoCtrl
    REST --> RabbitCtrl
    REST --> StreamCtrl
    REST --> DemoCtrl
    NodeProd -. "direct kafkajs produce\n(+ __TypeId__ header)" .-> T1
    NodeProd -.-> T2
    NodeProd -.-> T3

    DemoCtrl --> Producer
    DemoCtrl --> Hub
    DemoCtrl --> Buf
    RabbitCtrl --> Producer
    StreamCtrl --> Producer
    Producer -- "no key, round-robin" --> T1
    Producer -- "no key, round-robin" --> T2
    Producer -- "key = orderId" --> T3

    T1 -- "groupId=worker-group" --> W1
    T1 -- "groupId=worker-group" --> W2
    T1 -. "same groupId, extra competitor" .-> NC

    T2 -- "groupId=group-analytics" --> FA
    T2 -- "groupId=group-notifications" --> FN
    T2 -. "own groupId, own full copy" .-> NC

    T3 -- "groupId=order-events-live-group" --> OE
    T3 -- "disposable groupId, seekToBeginning" --> Replay
    OE --> Store
    OE --> Buf
    FN --> Hub
    Hub -. "SSE push" .-> Browser
    Replay --> Store

    StreamCtrl -. "GET /stream/replay" .-> Replay
```

**Key design decisions:**

| Decision | Where | Why |
|---|---|---|
| No partition key on `task-queue-topic` / `broadcast-topic` | `KafkaProducerService` | Lets Kafka's partitioner spread load — required for the queue pattern to actually distribute work |
| Partition key = `orderId` on `order-events-topic` | `KafkaProducerService` | Guarantees ordering *per order*, which replay depends on |
| Same `groupId` on two listeners | `QueueWorkerListener` | Forces Kafka to split partitions between them → point-to-point |
| Different `groupId`s on two listeners | `FanoutListener` | Forces Kafka to give each its own offsets → pub/sub |
| `retention.ms = -1` | `KafkaTopicConfig` | Without infinite retention, replay-from-0 would eventually hit deleted segments |
| Separate MANUAL-ack container factory | `KafkaConsumerConfig` | Only the log-style listener commits after applying state; queue/fanout use Boot's default auto-ack |
| Disposable, UUID-suffixed group id for replay | `OrderEventReplayService` | Must never collide with, or perturb, the live listener's committed offsets |
| SSE push only from `group-notifications` | `FanoutListener` → `RabbitPushHub` | One browser delivery per fanout message (analytics still logs its own copy) |
| Pull buffer for `NUMBER` events | `OrderEventListener` → `KafkaPullBuffer` | Lets User D explicitly pull; demo of poll/fetch vs auto push |

---

## 2. Sequence Diagrams (SD)

### 2.1 RabbitMQ-style work queue (`task-queue-topic`, point-to-point)

Two listener instances share `groupId=worker-group`. Kafka's group coordinator assigns each a disjoint set of partitions, so a given message is delivered to exactly **one** of them — never both.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as RabbitMqStyleController
    participant P as KafkaProducerService
    participant K as task-queue-topic
    participant W1 as worker-instance-1
    participant W2 as worker-instance-2

    C->>Ctrl: POST /api/v1/rabbitmq/queue {payload}
    Ctrl->>Ctrl: build TaskMessage(taskId, payload, now())
    Ctrl->>P: sendTaskToQueue(message)
    P->>K: send(topic, message)  // no key
    Ctrl-->>C: 202 Accepted (TaskMessage)

    Note over K,W2: Group coordinator assigned<br/>partitions 0-2 across W1 and W2
    K->>W1: deliver record (partition it owns)
    activate W1
    W1->>W1: log "[worker-instance-1] processed task"
    deactivate W1

    Note over W2: Never receives the same record —<br/>it owns a different partition
```

### 2.2 RabbitMQ-style pub/sub fanout (`broadcast-topic`)

Two listeners use *different* `groupId`s. Kafka tracks committed offsets independently per group, so both receive a full, independent copy of every message. The notifications listener also pushes to User B over SSE.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as RabbitMqStyleController / DemoController
    participant P as KafkaProducerService
    participant K as broadcast-topic
    participant GA as group-analytics
    participant GN as group-notifications
    participant Hub as RabbitPushHub
    participant B as User B (SSE)

    C->>Ctrl: POST /api/v1/rabbitmq/fanout {content}<br/>or POST /api/v1/demo/rabbit/words {words}
    Ctrl->>Ctrl: build BroadcastMessage(eventId, content, now())
    Ctrl->>P: sendBroadcast(message)
    P->>K: send(topic, message)  // no key
    Ctrl-->>C: 202 Accepted (BroadcastMessage)

    par delivered to every group independently
        K->>GA: deliver record
        GA->>GA: log "[group-analytics] received"
    and
        K->>GN: deliver record
        GN->>GN: log "[group-notifications] received"
        GN->>Hub: push(payload)
        Hub-->>B: SSE event "message"
    end

    Note over GA,GN: Same offset, same message,<br/>two independent read positions
```

### 2.3 Kafka-native event log: append + live consumption (`order-events-topic`)

Events are keyed by `orderId`. The live listener uses **manual** acknowledgment, committing only after state has been applied — not before, and not automatically on a timer. Demo `NUMBER` events are also offered to `KafkaPullBuffer` for User D.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as KafkaStreamController / DemoController
    participant P as KafkaProducerService
    participant K as order-events-topic
    participant L as OrderEventListener
    participant S as OrderStateStore
    participant Buf as KafkaPullBuffer

    C->>Ctrl: POST /api/v1/kafka/stream {orderId, eventType, details}<br/>or POST /api/v1/demo/kafka/numbers {number}
    Ctrl->>Ctrl: build OrderEvent(...)
    Ctrl->>P: sendOrderEvent(orderId, event)
    P->>K: send(topic, key=orderId, event)
    Ctrl-->>C: 202 Accepted (OrderEvent)

    K->>L: deliver record (partition determined by hash(orderId))
    activate L
    L->>S: apply(event)
    S->>S: eventsByOrder[orderId].add(event)
    opt eventType == NUMBER
        L->>Buf: offer({ number, offset, … })
    end
    L->>K: ack.acknowledge()  // manual commit, AFTER apply
    deactivate L
```

### 2.4 Kafka-native replay: rebuilding state from offset 0

Triggered on demand. A brand-new, disposable consumer group reads the *entire* history — proof that the log, unlike a RabbitMQ queue, still has the data after it's been "consumed" once by the live listener.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as KafkaStreamController
    participant R as OrderEventReplayService
    participant S as OrderStateStore
    participant K as order-events-topic

    C->>Ctrl: GET /api/v1/kafka/stream/replay
    Ctrl->>R: replayFromBeginning()
    R->>S: reset()

    R->>K: createConsumer(groupId="order-events-replay-<uuid>")
    R->>K: assign(all partitions)
    R->>K: seekToBeginning(all partitions)

    loop until poll() returns empty
        K-->>R: ConsumerRecords batch
        R->>S: apply(event) for each record
    end

    R-->>Ctrl: OrderStateStore.snapshot()
    Ctrl-->>C: 200 OK { orderId: [events...], ... }

    Note over R,K: Disposable group id never touches<br/>order-events-live-group's committed offsets
```

### 2.5 Browser demo: push vs pull

Teaching UX on top of §§2.2 and 2.3. Full diagrams live in [DEMO_DATAFLOWS.md](DEMO_DATAFLOWS.md).

| Demo | Producer | Broker path | Consumer UX |
|---|---|---|---|
| RabbitMQ-style push | User A → `POST /api/v1/demo/rabbit/words` | `broadcast-topic` → `FanoutListener` → `RabbitPushHub` | User B: SSE auto-update |
| Kafka-style pull | User C → `POST /api/v1/demo/kafka/numbers` | `order-events-topic` → `OrderEventListener` → `KafkaPullBuffer` | User D: `GET /api/v1/demo/kafka/pull` |
