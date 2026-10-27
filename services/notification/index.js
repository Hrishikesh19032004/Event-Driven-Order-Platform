const { Pool } = require("pg");
const { readFileSync } = require("fs");
const { ensureTopics } = require("../../shared/kafka");
const { consume } = require("../../shared/consume");
require("../../shared/tracing")("notification-service");
const pool = new Pool({ connectionString: "postgres://postgres:postgres@localhost:5436/notifications" });

async function main() {
  await pool.query(readFileSync(__dirname + "/schema.sql", "utf8"));
  await ensureTopics();
  await consume(pool, "notification-svc", ["order.confirmed", "order.cancelled"], async (c, topic, d) => {
    const message = topic === "order.confirmed"
      ? `Order ${d.orderId} confirmed`
      : `Order ${d.orderId} cancelled: ${d.reason || "unknown"}`;
    await c.query("INSERT INTO notifications(order_id, type, message) VALUES ($1, $2, $3)", [d.orderId, topic, message]);
  });
  console.log("notification service running");
}
main();