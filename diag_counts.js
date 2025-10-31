// diag_counts.js — diagnóstico das tabelas e colunas Tiny
require('dotenv').config();
const { Client } = require('pg');

(async () => {
  const c = new Client({
    host: process.env.PGHOST,
    port: +process.env.PGPORT || 5432,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
  });
  await c.connect();

  console.log('🔍 Diagnóstico do banco iah_plumas');
  const sess = await c.query(`
    select current_database(), current_user, current_schema, current_setting('search_path') as search_path
  `);
  console.table(sess.rows);

  const tables = await c.query(`
    select table_name, table_type
    from information_schema.tables
    where table_schema = 'public'
    order by table_name
  `);
  console.log('📚 Tabelas públicas:');
  console.table(tables.rows);

  // Contagem geral
  for (const t of ['tiny_products','tiny_orders','tiny_order_items']) {
    const sql = `select count(*)::int as rows from public.${t};`;
    try {
      const r = await c.query(sql);
      console.log(`${t}: ${r.rows[0].rows}`);
    } catch (e) {
      console.log(`❌ ${t}: ${e.message}`);
    }
  }

  // Datas e exemplos
  try {
    const r = await c.query(`
      select min(order_date) as min_date, max(order_date) as max_date, count(*) as total
      from public.tiny_orders
    `);
    console.log('\n📅 Intervalo de datas em tiny_orders:');
    console.table(r.rows);
  } catch (e) {
    console.log('❌ tiny_orders datas:', e.message);
  }

  // Ver primeiras linhas
  for (const [label, sql] of [
    ['Pedidos recentes', 'select id, order_number, order_date, total, status, channel from public.tiny_orders order by order_date desc limit 5'],
    ['Itens recentes', 'select order_id, sku, product_name, qty, price, total from public.tiny_order_items order by order_id desc limit 5'],
    ['Produtos recentes', 'select id, sku, product_name, price, cost_price from public.tiny_products order by id desc limit 5'],
  ]) {
    try {
      const r = await c.query(sql);
      console.log(`\n📦 ${label}:`);
      console.table(r.rows);
    } catch (e) {
      console.log(`❌ ${label}:`, e.message);
    }
  }

  await c.end();
})();
