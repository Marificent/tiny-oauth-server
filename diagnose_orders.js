// diagnose_orders.js
require("dotenv").config();
const { Client } = require("pg");

(async () => {
  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
  });

  try {
    await client.connect();
    console.log("🔎 Diagnóstico das tabelas-base");

    // 1) Contagens
    const qCounts = `
      SELECT
        (SELECT COUNT(*) FROM public.tiny_orders)      AS orders,
        (SELECT COUNT(*) FROM public.tiny_order_items) AS items,
        (SELECT COUNT(*) FROM public.tiny_products)    AS products
    `;
    const counts = (await client.query(qCounts)).rows[0];
    console.log("📦 Quantidades:", counts);

    // 2) Datas min/max + últimos 10 dias com volume
    const qDates = `
      SELECT
        MIN(order_date)::date AS min_date,
        MAX(order_date)::date AS max_date,
        SUM(total_order)::numeric(18,2) AS sum_total
      FROM public.tiny_orders;
    `;
    const dates = (await client.query(qDates)).rows[0];
    console.log("🗓️ Faixa de datas e total acumulado:", dates);

    const qDaily = `
      SELECT order_date::date AS day, COUNT(*) AS qtd, SUM(total_order)::numeric(18,2) AS total
      FROM public.tiny_orders
      GROUP BY 1
      ORDER BY 1 DESC
      LIMIT 10;
    `;
    const daily = (await client.query(qDaily)).rows;
    console.log("📆 Últimos 10 dias com pedidos (se houver):");
    daily.forEach(r => console.log(`  ${r.day} | pedidos=${r.qtd} | total=${r.total}`));

    // 3) Canais existentes
    const qChannels = `
      SELECT COALESCE(channel,'(null)') AS channel, COUNT(*) AS qtd, SUM(total_order)::numeric(18,2) AS total
      FROM public.tiny_orders
      GROUP BY 1
      ORDER BY total DESC NULLS LAST
      LIMIT 15;
    `;
    const channels = (await client.query(qChannels)).rows;
    console.log("🛒 Canais (top 15 por receita):");
    channels.forEach(r => console.log(`  ${r.channel} | pedidos=${r.qtd} | total=${r.total}`));

    // 4) Amostra de pedidos mais recentes
    const qRecent = `
      SELECT id, order_number, order_date, status, channel, total_order
      FROM public.tiny_orders
      ORDER BY order_date DESC NULLS LAST, id DESC
      LIMIT 5;
    `;
    const recent = (await client.query(qRecent)).rows;
    console.log("🧾 Amostra de pedidos mais recentes:");
    recent.forEach(r => console.log(`  #${r.order_number} | ${r.order_date} | R$ ${r.total_order} | ${r.status} | ${r.channel}`));

    // 5) Amostra de itens mais recentes
    const qItems = `
      SELECT oi.order_id, oi.product_code, oi.product_name, oi.quantity, oi.total_price, o.order_date
      FROM public.tiny_order_items oi
      JOIN public.tiny_orders o ON o.id = oi.order_id
      ORDER BY o.order_date DESC NULLS LAST, oi.order_id DESC
      LIMIT 5;
    `;
    const items = (await client.query(qItems)).rows;
    console.log("📦 Amostra de itens mais recentes:");
    items.forEach(r => console.log(`  ${r.order_id} | ${r.order_date} | ${r.product_code} | ${r.product_name} | qte=${r.quantity} | total=${r.total_price}`));

  } catch (err) {
    console.error("❌ Erro no diagnóstico:", err.message || err);
    process.exit(1);
  } finally {
    process.exit(0);
  }
})();
