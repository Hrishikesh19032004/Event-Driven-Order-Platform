const { randomUUID } = require("crypto");
const { pools, sleep, waitFor, createOrder, getOrder, stock, count, producer, closeAll } = require("./helpers");

jest.setTimeout(40000);
beforeAll(() => producer.connect());
afterAll(closeAll);

const statusIs = (id, s) => async () => (await getOrder(id)).status === s;

test("happy path: order confirmed, payment charged, notification sent", async () => {
  const { id } = await createOrder("SKU-1", 2);
  await waitFor(statusIs(id, "CONFIRMED"));
  const p = await pools.payment.query("SELECT status FROM payments WHERE order_id=$1", [id]);
  expect(p.rows[0].status).toBe("CHARGED");
  await waitFor(() => count(pools.notification, "SELECT count(*) n FROM notifications WHERE order_id=$1 AND type='order.confirmed'", [id]));
});

test("payment failure: order cancelled and stock released", async () => {
  const before = await stock();
  const { id } = await createOrder("SKU-1", 99);
  await waitFor(statusIs(id, "CANCELLED"));
  await waitFor(async () => (await stock()) === before);
  expect(await count(pools.inventory, "SELECT count(*) n FROM reservations WHERE order_id=$1", [id])).toBe(0);
  await waitFor(() => count(pools.notification, "SELECT count(*) n FROM notifications WHERE order_id=$1 AND type='order.cancelled'", [id]));
});

test("out of stock: cancelled and payment never involved", async () => {
  const { id } = await createOrder("SKU-1", 100000);
  await waitFor(statusIs(id, "CANCELLED"));
  expect(await count(pools.payment, "SELECT count(*) n FROM payments WHERE order_id=$1", [id])).toBe(0);
});

test("same event replayed 5 times has exactly one effect", async () => {
  const before = await stock();
  const orderId = randomUUID();
  const eventId = randomUUID();
  for (let i = 0; i < 5; i++) {
    await producer.send({
      topic: "order.created",
      messages: [{ key: orderId, value: JSON.stringify({ orderId, sku: "SKU-1", qty: 3 }), headers: { eventId } }],
    });
  }
  await waitFor(() => count(pools.inventory, "SELECT count(*) n FROM reservations WHERE order_id=$1", [orderId]));
  await sleep(3000);
  expect(await count(pools.inventory, "SELECT count(*) n FROM reservations WHERE order_id=$1", [orderId])).toBe(1);
  expect(await count(pools.inventory, "SELECT count(*) n FROM processed_events WHERE event_id=$1", [eventId])).toBe(1);
  expect(await stock()).toBe(before - 3);
});

test("crash recovery: committed-but-unpublished event is delivered later", async () => {
  const id = randomUUID();
  const c = await pools.order.connect();
  try {
    await c.query("BEGIN");
    await c.query("INSERT INTO orders(id, sku, qty) VALUES ($1,'SKU-1',2)", [id]);
    await c.query(
      "INSERT INTO outbox(id, topic, key, payload) VALUES (gen_random_uuid(),'order.created',$1,$2)",
      [id, { orderId: id, sku: "SKU-1", qty: 2 }]
    );
    await c.query("COMMIT");
  } finally {
    c.release();
  }
  await waitFor(statusIs(id, "CONFIRMED"));
});