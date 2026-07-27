# Demo dataflows (Users A/B/C/D)

Two browser flows on top of the existing Kafka patterns:

1. **RabbitMQ-style (push)** — User A types words; User B receives them automatically (SSE).
2. **Kafka-style (pull)** — User C types numbers; User D fetches them with an explicit pull.

Pages: `http://localhost:8080/` → `rabbit-a.html`, `rabbit-b.html`, `kafka-c.html`, `kafka-d.html`.

---

## 1. RabbitMQ-style (push) — User A → User B

```mermaid
sequenceDiagram
    participant A as User A<br/>(rabbit-a.html)
    participant API as DemoController
    participant P as KafkaProducerService
    participant K as broadcast-topic
    participant F as FanoutListener<br/>(group-notifications)
    participant Hub as RabbitPushHub<br/>(SSE)
    participant B as User B<br/>(rabbit-b.html)

    A->>API: POST /api/v1/demo/rabbit/words<br/>{ words }
    API->>P: sendBroadcast(message)
    P->>K: publish (no key, fanout topic)
    API-->>A: 202 Accepted

    K->>F: deliver copy to notifications group
    F->>Hub: push({ words, eventId, … })
    Hub-->>B: SSE event "message"
    Note over B: Appears automatically<br/>no Fetch button
```

**Idea:** A publishes once; B is already subscribed over SSE, so the page updates when the fanout listener receives the Kafka message.

| Step | Component | Action |
|------|-----------|--------|
| 1 | User A | `POST /api/v1/demo/rabbit/words` |
| 2 | `KafkaProducerService` | Write to `broadcast-topic` |
| 3 | `FanoutListener` (`group-notifications`) | Consume fanout copy |
| 4 | `RabbitPushHub` | Push to open SSE subscribers |
| 5 | User B | Message appears with no button click |

---

## 2. Kafka-style (pull) — User C → User D

```mermaid
sequenceDiagram
    participant C as User C<br/>(kafka-c.html)
    participant API as DemoController
    participant P as KafkaProducerService
    participant K as order-events-topic
    participant L as OrderEventListener
    participant Buf as KafkaPullBuffer
    participant D as User D<br/>(kafka-d.html)

    C->>API: POST /api/v1/demo/kafka/numbers<br/>{ number }
    API->>P: sendOrderEvent("numbers", event)
    P->>K: publish (key=numbers)
    API-->>C: 202 Accepted

    K->>L: deliver event
    L->>Buf: offer({ number, offset, … })
    Note over Buf: Stored in memory<br/>D still sees nothing

    D->>API: GET /api/v1/demo/kafka/pull
    API->>Buf: drain()
    Buf-->>API: list of pending messages
    API-->>D: { count, messages }
    Note over D: Only shows after<br/>Fetch / Pull
```

**Idea:** C appends to the Kafka log; the live listener buffers events. D only sees them when they **pull** (drain the buffer).

| Step | Component | Action |
|------|-----------|--------|
| 1 | User C | `POST /api/v1/demo/kafka/numbers` |
| 2 | `KafkaProducerService` | Append to `order-events-topic` (key=`numbers`) |
| 3 | `OrderEventListener` | Apply state + `KafkaPullBuffer.offer` |
| 4 | User D | `GET /api/v1/demo/kafka/pull` |
| 5 | `KafkaPullBuffer` | `drain()` → return pending batch |

---

## Side-by-side

| | RabbitMQ-style (A→B) | Kafka-style (C→D) |
|--|----------------------|-------------------|
| Topic | `broadcast-topic` | `order-events-topic` |
| Producer page | User A (words) | User C (numbers) |
| Consumer page | User B | User D |
| Delivery to browser | **Push** (SSE) | **Pull** (HTTP GET) |
| User action on receive | None | Click **Fetch / Pull** |
| API produce | `POST /api/v1/demo/rabbit/words` | `POST /api/v1/demo/kafka/numbers` |
| API consume | `GET /api/v1/demo/rabbit/live` (SSE) | `GET /api/v1/demo/kafka/pull` |

> Note: both flows still use **Kafka** as the broker. “RabbitMQ-style” / “Kafka-style” here describe the **browser delivery pattern** (push vs pull) on top of the project’s fanout and event-log topics.
