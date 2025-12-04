// list_tables.js
require("dotenv").config();
const db = require("./db");

async function main() {
  try {
    const sql = `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name;
    `;

    const result = await db.query(sql);
    console.log("Tabelas encontradas:\n");
    for (const row of result.rows) {
      console.log(`${row.table_schema}.${row.table_name}`);
    }
  } catch (err) {
    console.error("Erro ao listar tabelas:", err);
  } finally {
    process.exit(0);
  }
}

main();
