-- ===== RESET =====
DROP MATERIALIZED VIEW IF EXISTS public.mv_orders_daily;
DROP MATERIALIZED VIEW IF EXISTS public.mv_channels_30d;
DROP MATERIALIZED VIEW IF EXISTS public.mv_top_products_90d;

-- ===== MV 1: pedidos por dia (com receita calculada dos itens) =====
CREATE MATERIALIZED VIEW public.mv_orders_daily AS
WITH items AS (
  SELECT
    oi.order_id,
    COALESCE(NULLIF(oi.qty::text,''),'0')::numeric       AS qty,
    COALESCE(NULLIF(oi.price::text,''),'0')::numeric     AS price,
    NULLIF(oi.total::text,'')::numeric                   AS line_total
  FROM public.tiny_order_items oi
),
orders_norm AS (
  SELECT
    o.id,
    (o.order_date AT TIME ZONE 'UTC')::date              AS day,
    COALESCE(NULLIF(TRIM(o.channel),''), 'Desconhecido') AS channel
  FROM public.tiny_orders o
)
SELECT
  onrm.day,
  COUNT(DISTINCT onrm.id)                                AS orders_count,
  SUM(i.qty)                                             AS items_count,
  SUM(COALESCE(i.line_total, i.qty * i.price))           AS revenue
FROM orders_norm onrm
LEFT JOIN items i ON i.order_id = onrm.id
GROUP BY onrm.day
ORDER BY onrm.day;

-- ===== MV 2: canais dos últimos 30 dias =====
CREATE MATERIALIZED VIEW public.mv_channels_30d AS
WITH lines AS (
  SELECT
    (o.order_date AT TIME ZONE 'UTC')::date              AS day,
    COALESCE(NULLIF(TRIM(o.channel),''), 'Desconhecido') AS channel,
    COALESCE(NULLIF(oi.total::text,''),NULL)::numeric    AS line_total,
    COALESCE(NULLIF(oi.qty::text,''),'0')::numeric       AS qty,
    COALESCE(NULLIF(oi.price::text,''),'0')::numeric     AS price
  FROM public.tiny_orders o
  LEFT JOIN public.tiny_order_items oi ON oi.order_id = o.id
  WHERE (o.order_date AT TIME ZONE 'UTC')::date >= (CURRENT_DATE - INTERVAL '30 days')::date
)
SELECT
  channel,
  SUM(COALESCE(line_total, qty*price)) AS revenue,
  COUNT(DISTINCT day)                  AS active_days
FROM lines
GROUP BY channel
ORDER BY revenue DESC;

-- ===== MV 3: top produtos dos últimos 90 dias =====
CREATE MATERIALIZED VIEW public.mv_top_products_90d AS
WITH base AS (
  SELECT
    (o.order_date AT TIME ZONE 'UTC')::date              AS day,
    oi.sku                                              AS sku,
    oi.product_name                                     AS product_name,
    COALESCE(NULLIF(oi.qty::text,''),'0')::numeric      AS qty,
    COALESCE(NULLIF(oi.price::text,''),'0')::numeric    AS price,
    NULLIF(oi.total::text,'')::numeric                  AS line_total
  FROM public.tiny_order_items oi
  JOIN public.tiny_orders o ON o.id = oi.order_id
  WHERE (o.order_date AT TIME ZONE 'UTC')::date >= (CURRENT_DATE - INTERVAL '90 days')::date
)
SELECT
  sku,
  COALESCE(MAX(product_name),'')                        AS product_name,
  SUM(qty)                                             AS qty,
  SUM(COALESCE(line_total, qty*price))                 AS revenue
FROM base
GROUP BY sku
ORDER BY revenue DESC, qty DESC;

-- ===== Índices únicos (obrigatórios para REFRESH CONCURRENTLY) =====
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_orders_daily_day
  ON public.mv_orders_daily(day);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_channels_30d_channel
  ON public.mv_channels_30d(channel);

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_top_products_90d_sku
  ON public.mv_top_products_90d(sku);
