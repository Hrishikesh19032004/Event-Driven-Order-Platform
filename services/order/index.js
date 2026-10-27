const express = require("express");
const { Pool } = require("pg");
const { readFileSync } = require("fs");
const { randomUUID } = require("crypto");
const { kafka, ensureTopics } = require("../../shared/kafka");
const { enqueue, runRelay } = require("../../shared/outbox");
const { consume } = require("../../shared/consume");
require("../../shared/tracing")("order-service"); 
const { withSpan } = require("../../shared/trace");

const pool = new Pool({ connectionString: "postgres://postgres:postgres@localhost:5433/orders" });
const TRANSITIONS = {
  "inventory.reserved": ["RESERVED", ["PENDING"]],
  "inventory.failed": ["CANCELLED", ["PENDING"]],
  "payment.completed": ["CONFIRMED", ["PENDING", "RESERVED"]],
  "payment.failed": ["CANCELLED", ["PENDING", "RESERVED"]],
};
async function main() {
  await pool.query(readFileSync(__dirname + "/schema.sql", "utf8"));
  await ensureTopics();
  const producer = kafka.producer();
  await producer.connect();
  runRelay(pool, producer);

  await consume(pool, "order-svc", Object.keys(TRANSITIONS), async (c, topic, d) => {
    const [to, from] = TRANSITIONS[topic];
    const r = await c.query("UPDATE orders SET status = $1 WHERE id = $2 AND status = ANY($3)", [to, d.orderId, from]);
    if (r.rowCount && (to === "CONFIRMED" || to === "CANCELLED"))
      await enqueue(c, `order.${to.toLowerCase()}`, d.orderId, { orderId: d.orderId, status: to, reason: d.reason });
  });

  const app = express();
  app.use(express.json());

  app.post("/orders", (req, res) => withSpan("POST /orders", null, async () => {
    const { sku, qty } = req.body;
    if (typeof sku !== "string" || !Number.isInteger(qty) || qty <= 0)
      return res.status(400).json({ error: "invalid input" });
    const id = randomUUID();
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      await c.query("INSERT INTO orders(id, sku, qty) VALUES ($1, $2, $3)", [id, sku, qty]);
      await enqueue(c, "order.created", id, { orderId: id, sku, qty });
      await c.query("COMMIT");
      res.status(201).json({ id, status: "PENDING" });
    } catch (e) {
      await c.query("ROLLBACK");
      res.status(500).json({ error: "failed" });
    } finally {
      c.release();
    }
  }));

  app.get("/orders/:id", async (req, res) => {
    const { rows } = await pool.query("SELECT id, sku, qty, status FROM orders WHERE id = $1", [req.params.id]);
    rows[0] ? res.json(rows[0]) : res.sendStatus(404);
  });

  app.listen(3001, () => console.log("order service :3001"));
}
main();