const { Kafka } = require("kafkajs");

const kafka = new Kafka({ brokers: ["localhost:9092"] });

async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        "order.created", "order.confirmed", "order.cancelled",
        "inventory.reserved", "inventory.failed", "inventory.released",
        "payment.completed", "payment.failed",
      ].map((topic) => ({ topic })),
    });
  } catch (e) {
    console.warn("createTopics:", e.message);
  } finally {
    await admin.disconnect();
  }
}

module.exports = { kafka, ensureTopics };