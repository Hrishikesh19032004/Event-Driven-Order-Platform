const { inject } = require("./trace");

async function enqueue(c, topic, key, payload) {
  await c.query(
    "INSERT INTO outbox(id, topic, key, payload) VALUES (gen_random_uuid(), $1, $2, $3)",
    [topic, key, { ...payload, _tp: inject() }]
  );
}

async function runRelay(pool, producer) {
  for (;;) {
    let n = 0;
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      const { rows } = await c.query(
        "SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 200 FOR UPDATE SKIP LOCKED"
      );
      n = rows.length;
      if (n) {
        if (process.env.CRASH_BEFORE_PUBLISH) process.exit(1);
        const byTopic = {};
        for (const r of rows)
          (byTopic[r.topic] ||= []).push({ key: r.key, value: JSON.stringify(r.payload), headers: { eventId: r.id } });
        await producer.sendBatch({
          topicMessages: Object.entries(byTopic).map(([topic, messages]) => ({ topic, messages })),
        });
        await c.query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", [rows.map((r) => r.id)]);
      }
      await c.query("COMMIT");
    } catch (e) {
      await c.query("ROLLBACK");
      console.error("relay error", e);
    } finally {
      c.release();
    }
    if (!n) await new Promise((r) => setTimeout(r, 200));
  }
}

module.exports = { enqueue, runRelay };