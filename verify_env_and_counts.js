// verify_env_and_counts.js
require("dotenv").config();
const { Client } = require("pg");

(async () => {
  const env = ((k) => process.env[k] || "<vazio>");
  console.log("🔧 ENV PG:");
  console.table([
    { key: "PGHOST", value: env("PGHOST") },
    { key: "PGPORT", value: env("PGPORT") },
    { key: "PGDATABASE", value: env("PGDATABASE") },
    { key: "PGUSER", value: env("PGUSER") },
    { key: "PGPASSWORD", value: env("PGPASSWORD") ? "***" : "<vazio>" },
  ]);

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
  });

  try {
    await client.connect();

    const meta = await client.query(`
      SELECT current_user, current_database(), current_schema(),
             (SELECT setting FROM pg_settings WHERE name='search_path') AS search_path
    `);
    console.log("🛰️ Sessão atual:");
    console.table(meta.rows);

    // Listar *todas* as tabelas tiny_* em qualquer schema
    const tabs = await client.query(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_name LIKE 'tiny_%'
      ORDER BY table_schema, table_name
    `);
    console.log("📚 Tabelas tiny_* encontradas:");
    console.table(tabs.rows);

    // Tentar contar em cada tabela encontrada
    for (const r of tabs.rows) {
      const fq = `${r.table_schema}."${r.table_name}"`;
      try {
        const c = await client.query(`SELECT COUNT(*)::bigint AS n FROM ${fq};`);
        console.log(`📦 ${fq}: ${c.rows[0].n}`);
      } catch (e) {
        console.log(`⚠️ Falha ao contar ${fq}: ${e.message}`);
      }
    }

    // Contagens diretas das 3 principais (se existirem)
    const main = [
      'public.tiny_products',
      'public.tiny_orders',
      'public.tiny_order_items',
      'iah_plumas.tiny_products',
      'iah_plumas.tiny_orders',
      'iah_plumas.tiny_order_items'
    ];
    for (const fq of main) {
      try {
        const c = await client.query(`SELECT COUNT(*)::bigint AS n FROM ${fq};`);
        console.log(`🔎 ${fq}: ${c.rows[0].n}`);
      } catch (e) {
        console.log(`— ${fq}: ${e.message}`);
      }
    }
  } catch (err) {
    console.error("❌ Erro:", err.message || err);
    process.exit(1);
  } finally {
    process.exit(0);
  }
})();
