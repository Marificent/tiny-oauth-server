// introspect_legacy.js — lista colunas e 3 amostras de cada tabela legacy
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

  console.log('🔎 Conectado. Inspecionando tabelas legacy: public.orders e public.order_items');

  // helper: lista colunas
  async function listColumns(table) {
    const sql = `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name=$1
      ORDER BY ordinal_position;
    `;
    const r = await c.query(sql, [table]);
    console.log(`\n📋 Colunas de ${table}:`);
    r.rows.forEach(row => console.log(`  - ${row.column_name} (${row.data_type})`));
  }

  // helper: amostras
  async function sampleRows(table) {
    const r = await c.query(`SELECT * FROM public.${table} ORDER BY 1 DESC LIMIT 3;`);
    console.log(`\n🧪 Amostras de ${table} (máx 3):`);
    if (r.rows.length === 0) {
      console.log('  (sem linhas)');
    } else {
      r.rows.forEach((row, i) => {
        console.log(`  #${i+1}:`, JSON.stringify(row, null, 2));
      });
    }
  }

  await listColumns('orders');
  await sampleRows('orders');

  await listColumns('order_items');
  await sampleRows('order_items');

  await c.end();
  console.log('\n✅ Pronto.');
})();
