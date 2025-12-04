require("dotenv").config();
const { Client } = require("pg");

const sql = `
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.vw_tiny_sales_enriched AS
WITH base AS (
  SELECT
    o.order_date::date                                 AS data,
    COALESCE(oi.product_name, '(sem nome)')            AS produto,
    LOWER(COALESCE(oi.product_name, ''))               AS nome_lower,
    COALESCE(oi.qty, 0)::numeric                       AS quantidade,
    COALESCE(oi.total, oi.qty * oi.price, 0)::numeric  AS valor_total,
    dp.tags                                            AS tags
  FROM public.tiny_order_items oi
  JOIN public.tiny_orders o
    ON o.id = oi.order_id
  JOIN dw.dim_product dp
    ON dp.codigo = oi.sku
   AND dp.situacao = 'A'   -- 🔴 só produtos ativos entram na view
)
SELECT
  data,
  produto,
  NULLIF(TRIM(BOTH ', ' FROM COALESCE(tags, '')), '') AS tags,
  quantidade,
  valor_total
FROM base;
`;

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

  try {
    console.log("🔌 Conectando ao banco...");
    await client.connect();
    console.log("📐 Criando view analytics.vw_tiny_sales_enriched...");
    await client.query(sql);
    console.log("🎉 View criada/atualizada com sucesso!");
  } catch (err) {
    console.error("❌ Erro ao criar a view:", err);
  } finally {
    await client.end();
    console.log("👋 Conexão encerrada.");
  }
}

main();
