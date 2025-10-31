// db_setup_all.js — cria/recria as tabelas-base e materialized views
require("dotenv").config();
const { Client } = require("pg");

const sql = `
-- ===== TABELAS-BASE =====
CREATE TABLE IF NOT EXISTS public.tiny_products (
  id                BIGINT PRIMARY KEY,
  sku               TEXT,
  product_name      TEXT,
  price             NUMERIC,
  cost_price        NUMERIC,
  gtin              TEXT,
  situation         TEXT,
  created_at_tiny   TIMESTAMP NULL,
  raw_json          JSONB
);

CREATE TABLE IF NOT EXISTS public.tiny_orders (
  id                BIGSERIAL PRIMARY KEY,
  external_id       TEXT,          -- id do pedido no Tiny (quando houver)
  order_number      TEXT,
  order_date        DATE,
  channel           TEXT,          -- canal (ex.: olist, ml, etc.) quando disponível
  customer_name     TEXT,
  total             NUMERIC,
  status            TEXT,
  raw_json          JSONB
);
-- acelera filtros por data
CREATE INDEX IF NOT EXISTS idx_tiny_orders_date ON public.tiny_orders(order_date);

CREATE TABLE IF NOT EXISTS public.tiny_order_items (
  id                BIGSERIAL PRIMARY KEY,
  order_id          BIGINT REFERENCES public.tiny_orders(id) ON DELETE CASCADE,
  sku               TEXT,
  product_name      TEXT,
  qty               NUMERIC,
  price             NUMERIC,
  total             NUMERIC,
  raw_json          JSONB
);
-- acelera joins
CREATE INDEX IF NOT EXISTS idx_tiny_order_items_order_id ON public.tiny_order_items(order_id);

-- ===== MATERIALIZED VIEWS =====
-- 30 dias por canal
DROP MATERIALIZED VIEW IF EXISTS public.mv_channels_30d;
CREATE MATERIALIZED VIEW public.mv_channels_30d AS
SELECT
  COALESCE(NULLIF(TRIM(channel), ''), '(sem canal)') AS channel,
  COUNT(*)::INT                                      AS orders_count,
  COALESCE(SUM(total), 0)::NUMERIC                   AS revenue
FROM public.tiny_orders
WHERE order_date >= (CURRENT_DATE - INTERVAL '30 days')
GROUP BY 1
ORDER BY revenue DESC NULLS LAST;

-- top produtos 90 dias (por receita)
DROP MATERIALIZED VIEW IF EXISTS public.mv_top_products_90d;
CREATE MATERIALIZED VIEW public.mv_top_products_90d AS
WITH base AS (
  SELECT
    COALESCE(NULLIF(TRIM(i.sku), ''), '(sem sku)')       AS sku,
    COALESCE(NULLIF(TRIM(i.product_name), ''), '(sem nome)') AS product_name,
    COALESCE(SUM(i.total), 0)::NUMERIC                   AS revenue,
    COALESCE(SUM(i.qty), 0)::NUMERIC                     AS qty
  FROM public.tiny_order_items i
  JOIN public.tiny_orders o
    ON o.id = i.order_id
  WHERE o.order_date >= (CURRENT_DATE - INTERVAL '90 days')
  GROUP BY 1,2
)
SELECT
  ROW_NUMBER() OVER (ORDER BY revenue DESC NULLS LAST) AS rank,
  sku, product_name, revenue, qty
FROM base
ORDER BY rank;

-- série diária (90 dias)
DROP MATERIALIZED VIEW IF EXISTS public.mv_orders_daily;
CREATE MATERIALIZED VIEW public.mv_orders_daily AS
SELECT
  o.order_date                          AS day,
  COUNT(*)::INT                         AS orders,
  COALESCE(SUM(o.total), 0)::NUMERIC    AS revenue,
  COALESCE(SUM(i.qty), 0)::NUMERIC      AS items
FROM public.tiny_orders o
LEFT JOIN LATERAL (
  SELECT COALESCE(SUM(qty),0) AS qty
  FROM public.tiny_order_items i
  WHERE i.order_id = o.id
) i ON TRUE
WHERE o.order_date >= (CURRENT_DATE - INTERVAL '90 days')
GROUP BY o.order_date
ORDER BY o.order_date DESC;

-- ===== ÍNDICES ÚNICOS PARA REFRESH CONCURRENTLY =====
-- (índices chave para permitir REFRESH CONCURRENTLY nas MVs)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ux_mv_channels_30d_channel'
  ) THEN
    CREATE UNIQUE INDEX ux_mv_channels_30d_channel
      ON public.mv_channels_30d(channel);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ux_mv_top_products_90d_rank'
  ) THEN
    CREATE UNIQUE INDEX ux_mv_top_products_90d_rank
      ON public.mv_top_products_90d(rank);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ux_mv_orders_daily_day'
  ) THEN
    CREATE UNIQUE INDEX ux_mv_orders_daily_day
      ON public.mv_orders_daily(day);
  END IF;
END$$;

-- ===== PERMISSÕES BÁSICAS (opcional) =====
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;
`;

(async () => {
  const client = new Client({
    host: process.env.PGHOST || "localhost",
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE || "iaplumas",
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD,
    application_name: "db_setup_all.js",
  });

  try {
    await client.connect();
    console.log("🏗️  Criando/recriando schema na base:", client.database);
    await client.query(sql);
    console.log("✅ Schema e MVs prontos.");
  } catch (e) {
    console.error("❌ Erro na criação:", e.message || e);
    process.exitCode = 1;
  } finally {
    try { await client.end(); } catch {}
  }
})();
