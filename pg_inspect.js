// pg_inspect.js
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

    // Quem sou eu e qual search_path?
    const who = await client.query(`
      SELECT current_user, current_database(), current_schema(), setting AS search_path
      FROM pg_settings WHERE name = 'search_path'
    `);
    console.log("🔐 Conexão:");
    console.table(who.rows);

    // Tabelas que contenham "tiny" no nome (em qualquer schema)
    const tables = await client.query(`
      SELECT table_schema, table_name, table_type
      FROM information_schema.tables
      WHERE table_name ILIKE '%tiny%'
      ORDER BY table_schema, table_name
    `);
    console.log("📚 Tabelas encontradas com 'tiny' no nome:");
    console.table(tables.rows);

    // Colunas das tabelas tiny_orders e tiny_order_items (onde quer que estejam)
    const columns = await client.query(`
      SELECT c.table_schema, c.table_name, c.column_name, c.data_type
      FROM information_schema.columns c
      WHERE (c.table_name IN ('tiny_orders','tiny_order_items','tiny_products'))
      ORDER BY c.table_schema, c.table_name, c.ordinal_position
    `);
    console.log("🧩 Colunas das tabelas principais:");
    console.table(columns.rows);

    // Contagens por schema para tiny_orders/tiny_order_items/tiny_products
    // (monta dinamicamente e executa só para tabelas existentes)
    const tableList = [...new Set(columns.rows.map(r => `${r.table_schema}.${r.table_name}`))];
    for (const fq of tableList) {
      const q = `SELECT '${fq}' AS table, COUNT(*)::bigint AS rows FROM ${fq};`;
      try {
        const r = await client.query(q);
        console.log(`📦 Linhas em ${fq}:`, r.rows[0]);
      } catch (e) {
        console.log(`⚠️  Falha ao contar ${fq}:`, e.message);
      }
    }

  } catch (err) {
    console.error("❌ Erro no pg_inspect:", err.message || err);
    process.exit(1);
  } finally {
    process.exit(0);
  }
})();
