const { Pool } = require("pg");
const pool = new Pool({ connectionString: "postgres://postgres:postgres@localhost:5433/orders" });

(async () => {
  const t0 = Date.now();
  for (;;) {
    const { rows } = await pool.query(
      "select count(*) filter (where status in ('PENDING','RESERVED')) open, count(*) total from orders"
    );
    const open = Number(rows[0].open);
    console.log(`${((Date.now() - t0) / 1000).toFixed(1)}s  open=${open}  total=${rows[0].total}`);
    if (open === 0) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  await pool.end();
})();