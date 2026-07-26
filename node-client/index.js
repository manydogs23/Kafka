// Node.js client demonstrating two things against the kafka-messaging-patterns
// Spring Boot service:
//
//   1. Publishing directly to Kafka with kafkajs (bypassing the REST layer
//      entirely), including the __TypeId__ header the Java side's
//      JsonDeserializer needs to resolve the correct DTO class.
//   2. Joining the SAME consumer groups as the Java listeners
//      (worker-group, group-analytics, group-notifications) to prove that
//      group membership -- not the language/runtime -- is what determines
//      whether a message is delivered once (queue) or to everyone (fanout).
//
// Run with: node index.js <mode>   (see package.json scripts for the list)

import { Kafka } from "kafkajs";

const BROKER = "localhost:9092";
const API_BASE = "http://localhost:8080";

const TOPICS = {
  TASK_QUEUE: "task-queue-topic",
  BROADCAST: "broadcast-topic",
  ORDER_EVENTS: "order-events-topic",
};

const GROUPS = {
  WORKER: "worker-group",
  ANALYTICS: "group-analytics",
  NOTIFICATIONS: "group-notifications",
};

// Fully-qualified Java class names. Spring's JsonSerializer stamps this into
// the __TypeId__ record header; without it the Java-side JsonDeserializer
// can't tell which DTO to instantiate for messages produced from outside the
// JVM.
const JAVA_TYPES = {
  TASK_MESSAGE: "com.example.kafkapatterns.dto.TaskMessage",
  BROADCAST_MESSAGE: "com.example.kafkapatterns.dto.BroadcastMessage",
  ORDER_EVENT: "com.example.kafkapatterns.dto.OrderEvent",
};

const kafka = new Kafka({ clientId: "node-messaging-client", brokers: [BROKER] });

function jsonMessage(javaTypeId, payload) {
  return {
    value: JSON.stringify(payload),
    headers: { __TypeId__: javaTypeId },
  };
}

// --- Direct kafkajs producers ------------------------------------------------

async function produceQueueTask() {
  const producer = kafka.producer();
  await producer.connect();
  const taskId = `node-task-${Date.now()}`;
  // No partition key: lets Kafka spread this across task-queue-topic's
  // partitions, same as the Java producer, so it can land on either
  // worker-instance-1, worker-instance-2, or this script's own
  // consume-worker process (whichever owns that partition).
  await producer.send({
    topic: TOPICS.TASK_QUEUE,
    messages: [
      jsonMessage(JAVA_TYPES.TASK_MESSAGE, {
        taskId,
        payload: "task published directly from Node.js",
        createdAt: new Date().toISOString(),
      }),
    ],
  });
  console.log(`[produce-queue] sent ${taskId} to ${TOPICS.TASK_QUEUE}`);
  await producer.disconnect();
}

async function produceFanoutEvent() {
  const producer = kafka.producer();
  await producer.connect();
  const eventId = `node-event-${Date.now()}`;
  await producer.send({
    topic: TOPICS.BROADCAST,
    messages: [
      jsonMessage(JAVA_TYPES.BROADCAST_MESSAGE, {
        eventId,
        content: "broadcast published directly from Node.js",
        createdAt: new Date().toISOString(),
      }),
    ],
  });
  console.log(`[produce-fanout] sent ${eventId} to ${TOPICS.BROADCAST}`);
  await producer.disconnect();
}

async function produceOrderEvent() {
  const producer = kafka.producer();
  await producer.connect();
  const orderId = `node-order-${Date.now()}`;
  await producer.send({
    topic: TOPICS.ORDER_EVENTS,
    messages: [
      {
        key: orderId, // keyed: all events for this order stay in one partition
        ...jsonMessage(JAVA_TYPES.ORDER_EVENT, {
          orderId,
          eventType: "ORDER_CREATED",
          details: "order event published directly from Node.js",
          timestamp: new Date().toISOString(),
        }),
      },
    ],
  });
  console.log(`[produce-order] sent event for ${orderId} to ${TOPICS.ORDER_EVENTS}`);
  await producer.disconnect();
}

// --- Consumers joining the SAME groups as the Java listeners -----------------

async function consumeAsWorker() {
  // Joins worker-group: Kafka now rebalances task-queue-topic's partitions
  // across THREE competing members (2 Java + this one), so this process
  // will only ever see the slice it's assigned -- never a duplicate of what
  // a Java worker already consumed.
  const consumer = kafka.consumer({ groupId: GROUPS.WORKER });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.TASK_QUEUE, fromBeginning: false });
  console.log(`[consume-worker] joined ${GROUPS.WORKER}, competing with the Java worker instances...`);
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const task = JSON.parse(message.value.toString());
      console.log(
        `[consume-worker | ${GROUPS.WORKER}] partition=${partition} offset=${message.offset} task=${task.taskId} payload=${task.payload}`
      );
    },
  });
}

async function consumeFanout(groupId) {
  // Joins its own consumer group, independent of group-analytics /
  // group-notifications in the Java app -- so it receives a FULL copy of
  // every broadcast, regardless of what those groups have already read.
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.BROADCAST, fromBeginning: false });
  console.log(`[consume-fanout] joined ${groupId} on ${TOPICS.BROADCAST}...`);
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[consume-fanout | ${groupId}] partition=${partition} offset=${message.offset} content=${event.content}`);
    },
  });
}

// --- Alternative path: trigger the patterns over HTTP instead of Kafka directly

async function postJson(path, body) {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  console.log(`[http] POST ${path} -> ${response.status}`);
  console.log(await response.json());
}

async function httpTriggerQueue() {
  await postJson("/api/v1/rabbitmq/queue", { payload: "task published via REST from Node.js" });
}

async function httpTriggerFanout() {
  await postJson("/api/v1/rabbitmq/fanout", { content: "broadcast published via REST from Node.js" });
}

async function httpTriggerOrderEvent() {
  await postJson("/api/v1/kafka/stream", {
    orderId: `http-order-${Date.now()}`,
    eventType: "ORDER_CREATED",
    details: "order event published via REST from Node.js",
  });
}

async function httpTriggerReplay() {
  const response = await fetch(`${API_BASE}/api/v1/kafka/stream/replay`);
  console.log(`[http] GET /api/v1/kafka/stream/replay -> ${response.status}`);
  console.log(JSON.stringify(await response.json(), null, 2));
}

// --- Entry point --------------------------------------------------------------

const mode = process.argv[2];

const modes = {
  "produce-queue": produceQueueTask,
  "produce-fanout": produceFanoutEvent,
  "produce-order": produceOrderEvent,
  "consume-worker": consumeAsWorker,
  "consume-analytics": () => consumeFanout(GROUPS.ANALYTICS),
  "consume-notifications": () => consumeFanout(GROUPS.NOTIFICATIONS),
  "http-queue": httpTriggerQueue,
  "http-fanout": httpTriggerFanout,
  "http-order": httpTriggerOrderEvent,
  "http-replay": httpTriggerReplay,
};

const run = modes[mode];
if (!run) {
  console.error(`Unknown mode "${mode}". Available modes: ${Object.keys(modes).join(", ")}`);
  process.exit(1);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
