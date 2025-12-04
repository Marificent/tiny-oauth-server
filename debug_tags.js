require("dotenv").config();
const { Client } = require("pg");

async function main() {
  const client = new Client({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: { rejectUnauthorized: false },
  });

  console.log("🔌 Conectando ao banco...");
  await client.connect();

  console.log("\n1) Quantidade total de produtos em dw.dim_product:");
  const totalDim = await client.query(
    "SELECT COUNT(*) AS total FROM dw.dim_product;"
  );
  console.table(totalDim.rows);

  console.log(
    "\n2) Quantos produtos têm tags NÃO nulas e NÃO vazias em dw.dim_product:"
  );
  const totalComTag = await client.query(
    "SELECT COUNT(*) AS com_tags FROM dw.dim_product WHERE tags IS NOT NULL AND tags <> '';"
  );
  console.table(totalComTag.rows);

  console.log("\n3) Amostra de até 10 produtos com tags em dw.dim_product:");
  const sample = await client.query(
    "SELECT codigo, nome, tags FROM dw.dim_product WHERE tags IS NOT NULL AND tags <> '' LIMIT 10;"
  );
  console.table(sample.rows);

  console.log("\n4) Quantidade de linhas na view analytics.vw_tiny_sales_enriched:");
  try {
    const cntView = await client.query(
      "SELECT COUNT(*) AS total FROM analytics.vw_tiny_sales_enriched;"
    );
    console.table(cntView.rows);
  } catch (e) {
    console.warn("⚠️ Não consegui consultar a view analytics.vw_tiny_sales_enriched:", e.message);
  }

  await client.end();
  console.log("\n👋 Debug concluído.");
}

main().catch((e) => {
  console.error("❌ Erro no debug:", e.message);
  process.exit(1);
});
