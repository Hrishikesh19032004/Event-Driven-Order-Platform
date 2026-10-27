const { Pool } = require("pg");
const { readFileSync } = require("fs");
const { kafka, ensureTopics } = require("../../shared/kafka");
const { enqueue, runRelay } = require("../../shared/outbox");
const { consume } = require("../../shared/consume");
require("../../shared/tracing")("payment-service"); 

const pool = new Pool({ connectionString: "postgres://postgres:postgres@localhost:5435/payments" });
const FAIL_RATE = Number(process.env.FAIL_RATE || 0);

async function main() {
  await pool.query(readFileSync(__dirname + "/schema.sql", "utf8"));
  await ensureTopics();
  const producer = kafka.producer();
  await producer.connect();
  runRelay(pool, producer);

  await consume(pool, "payment-svc", ["inventory.reserved"], async (c, _topic, d) => {
    const amount = d.qty * 100;
    const ok = d.qty !== 99 && Math.random() >= FAIL_RATE;
    await c.query(
      "INSERT INTO payments(order_id, amount, status) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
      [d.orderId, amount, ok ? "CHARGED" : "FAILED"]
    );
    if (ok) await enqueue(c, "payment.completed", d.orderId, { orderId: d.orderId, amount });
    else await enqueue(c, "payment.failed", d.orderId, { orderId: d.orderId, reason: "CARD_DECLINED" });
  });
  console.log("payment service running");
}
main();