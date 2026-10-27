const { Pool } = require("pg");
const { readFileSync } = require("fs");
const { kafka, ensureTopics } = require("../../shared/kafka");
const { enqueue, runRelay } = require("../../shared/outbox");
const { consume } = require("../../shared/consume");
require("../../shared/tracing")("inventory-service");    
const pool = new Pool({ connectionString: "postgres://postgres:postgres@localhost:5434/inventory" });

async function main() {
  await pool.query(readFileSync(__dirname + "/schema.sql", "utf8"));
  await ensureTopics();
  const producer = kafka.producer();
  await producer.connect();
  runRelay(pool, producer);

  await consume(pool, "inventory-svc", ["order.created", "payment.failed"], async (c, topic, d) => {
    if (topic === "payment.failed") {
      const r = await c.query("DELETE FROM reservations WHERE order_id = $1 RETURNING sku, qty", [d.orderId]);
      if (r.rowCount) {
        await c.query("UPDATE inventory SET stock = stock + $2 WHERE sku = $1", [r.rows[0].sku, r.rows[0].qty]);
        await enqueue(c, "inventory.released", d.orderId, { orderId: d.orderId });
      }
      return;
    }
    const r = await c.query(
      "UPDATE inventory SET stock = stock - $2 WHERE sku = $1 AND stock >= $2",
      [d.sku, d.qty]
    );
    if (r.rowCount) {
      await c.query("INSERT INTO reservations(order_id, sku, qty) VALUES ($1, $2, $3)", [d.orderId, d.sku, d.qty]);
      await enqueue(c, "inventory.reserved", d.orderId, { orderId: d.orderId, sku: d.sku, qty: d.qty });
    } else {
      await enqueue(c, "inventory.failed", d.orderId, { orderId: d.orderId, reason: "OUT_OF_STOCK" });
    }
  });
  console.log("inventory service running");
}
main();