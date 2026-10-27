const { Pool } = require("pg");
const { kafka } = require("../shared/kafka");

const url = (port, db) => `postgres://postgres:postgres@localhost:${port}/${db}`;
const pools = {
  order: new Pool({ connectionString: url(5433, "orders") }),
  inventory: new Pool({ connectionString: url(5434, "inventory") }),
  payment: new Pool({ connectionString: url(5435, "payments") }),
  notification: new Pool({ connectionString: url(5436, "notifications") }),
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, timeout = 20000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await sleep(300);
  }
  throw new Error("waitFor timed out");
}

async function createOrder(sku, qty) {
  const res = await fetch("http://localhost:3001/orders", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sku, qty }),
  });
  return res.json();
}

const getOrder = async (id) => (await fetch(`http://localhost:3001/orders/${id}`)).json();
const stock = async () => (await pools.inventory.query("SELECT stock FROM inventory WHERE sku='SKU-1'")).rows[0].stock;
const count = async (pool, sql, params) => Number((await pool.query(sql, params)).rows[0].n);

const producer = kafka.producer();

async function closeAll() {
  await producer.disconnect().catch(() => {});
  await Promise.all(Object.values(pools).map((p) => p.end()));
}

module.exports = { pools, sleep, waitFor, createOrder, getOrder, stock, count, producer, closeAll };