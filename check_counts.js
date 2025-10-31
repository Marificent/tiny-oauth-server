// check_counts.js — verifica se há dados nas tabelas-base e datas recentes
require("dotenv").config();
const { Client } = require("pg");

(async () => {
  const client = new Client({
    host: process.env.PGHOST || "localhost",
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE || "iaplumas",
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD,
    application_name: "check_counts.js",
  });

  try {
    await client.connect();

    const q = async (label, sql) => {
      try {
        const { rows } = await client.query(sql);
        console.log(`\n🔎 ${label}`);
        console.table(rows);
      } catch (e) {
        console.log(`\n❌ ${label} (erro)`);
        console.log(e.message);
      }
    };

    // Quantidades gerais
    await q("Quantidades (linhas) nas tabelas-base", `
      SELECT 'tiny_products' AS tabela, COUNT(*)::int AS linhas FROM public.tiny_products
      UNION ALL
      SELECT 'tiny_orders'   AS tabela, COUNT(*)::int AS linhas FROM public.tiny_orders
      UNION ALL
      SELECT 'tiny_order_items' AS tabela, COUNT(*)::int AS linhas FROM public.tiny_order_items
      ORDER BY tabela;
    `);

    // Datas úteis para pedidos
    await q("Intervalo de datas de tiny_orders", `
      SELECT 
        MIN(order_date) AS primeira_data,
        MAX(order_date) AS ultima_data,
        COUNT(*)::int   AS total_pedidos
      FROM public.tiny_orders;
    `);

    // Amostra de pedidos mais recentes
    await q("Top 5 pedidos mais recentes", `
      SELECT id, external_id, order_date, channel, total
      FROM public.tiny_orders
      ORDER BY order_date DESC NULLS LAST
      LIMIT 5;
    `);

    // Amostra de itens recentes
    await q("Top 5 itens mais recentes", `
      SELECT order_id, sku, product_name, qty, price, total
      FROM public.tiny_order_items
      ORDER BY id DESC
      LIMIT 5;
    `);

    // Views (só para ver se estão vazias ou não)
    await q("MV: mv_channels_30d (top 5)", `
      SELECT channel, revenue, orders_count
      FROM public.mv_channels_30d
      ORDER BY revenue DESC NULLS LAST
      LIMIT 5;
    `);

    await q("MV: mv_top_products_90d (top 5)", `
      SELECT rank, sku, product_name, revenue, qty
      FROM public.mv_top_products_90d
      ORDER BY rank
      LIMIT 5;
    `);

    await q("MV: mv_orders_daily (top 5 dias)", `
      SELECT day, orders, items, revenue
      FROM public.mv_orders_daily
      ORDER BY day DESC
      LIMIT 5;
    `);

  } catch (err) {
    console.error("❌ Erro geral:", err.message || err);
    process.exitCode = 1;
  } finally {
    try { await client.end(); } catch {}
  }
})();
