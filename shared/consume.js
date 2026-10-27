const { kafka } = require("./kafka");
const { withSpan } = require("./trace");

async function consume(pool, groupId, topics, handle) {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topics });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const eventId = message.headers.eventId.toString();
      const data = JSON.parse(message.value.toString());
      await withSpan(`consume ${topic}`, data._tp, async () => {
        const c = await pool.connect();
        try {
          await c.query("BEGIN");
          const r = await c.query(
            "INSERT INTO processed_events(event_id) VALUES ($1) ON CONFLICT DO NOTHING",
            [eventId]
          );
          if (r.rowCount) await handle(c, topic, data);
          await c.query("COMMIT");
        } catch (e) {
          await c.query("ROLLBACK");
          throw e;
        } finally {
          c.release();
        }
      }, { "messaging.group": groupId });
    },
  });
}

module.exports = { consume };