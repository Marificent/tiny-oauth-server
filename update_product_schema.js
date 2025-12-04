require("dotenv").config();
const { Client } = require("pg");

async function main() {
  const client = new Client({
    host: process.env.PGHOST || "localhost",
    port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  const sql = `
    ALTER TABLE staging.stg_products
      ADD COLUMN IF NOT EXISTS tags TEXT;

    ALTER TABLE dw.dim_product
      ADD COLUMN IF NOT EXISTS tags TEXT;
  `;

  try {
    console.log("🔌 Conectando ao banco...");
    await client.connect();
    console.log("🧱 Atualizando schema de produtos (staging.stg_products, dw.dim_product)...");
    await client.query(sql);
    console.log("🎉 Colunas 'tags' garantidas com sucesso!");
  } catch (err) {
    console.error("❌ Erro ao atualizar schema:", err);
  } finally {
    await client.end();
    console.log("👋 Conexão encerrada.");
  }
}

main();
