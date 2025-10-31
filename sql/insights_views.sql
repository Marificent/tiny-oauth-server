-- =========================
-- Insights Views (IA Plumas) — VERSÃO AJUSTADA
-- =========================

-- 1) Crescimento de receita por canal (últimos 90 dias)
CREATE OR REPLACE VIEW vw_insights_channels_growth AS
SELECT
    o.channel,
    DATE_TRUNC('week', o.order_date) AS semana,
    SUM(o.total) AS receita,
    COUNT(DISTINCT o.id) AS pedidos
FROM tiny_orders o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY o.channel, DATE_TRUNC('week', o.order_date)
ORDER BY semana DESC;

-- 2) Variação WoW por canal
CREATE OR REPLACE VIEW vw_insights_channels_var AS
SELECT
    g.channel,
    g.semana,
    g.receita,
    LAG(g.receita) OVER (PARTITION BY g.channel ORDER BY g.semana) AS receita_anterior,
    ROUND(
        CASE 
            WHEN LAG(g.receita) OVER (PARTITION BY g.channel ORDER BY g.semana) > 0 
            THEN ( (g.receita - LAG(g.receita) OVER (PARTITION BY g.channel ORDER BY g.semana))
                   / LAG(g.receita) OVER (PARTITION BY g.channel ORDER BY g.semana) * 100)
            ELSE NULL 
        END, 2
    ) AS variacao_pct
FROM vw_insights_channels_growth g;

-- 3) Produtos com QUEDA (30d vs 30d anteriores)
CREATE OR REPLACE VIEW vw_insights_drop_produtos AS
WITH receita_periodos AS (
    SELECT
        i.sku,
        i.product_name,
        SUM(i.total) FILTER (WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days') AS receita_30d,
        SUM(i.total) FILTER (
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '60 days' 
              AND o.order_date <  CURRENT_DATE - INTERVAL '30 days'
        ) AS receita_ant_30d
    FROM tiny_order_items i
    JOIN tiny_orders o ON o.id = i.order_id
    GROUP BY i.sku, i.product_name
)
SELECT
    sku,
    product_name,
    receita_30d,
    receita_ant_30d,
    (receita_30d - receita_ant_30d) AS delta,
    ROUND(
        CASE WHEN receita_ant_30d > 0 THEN ((receita_30d - receita_ant_30d) / receita_ant_30d * 100)
        ELSE NULL END, 2
    ) AS variacao_pct
FROM receita_periodos
WHERE receita_ant_30d > 0
ORDER BY delta ASC
LIMIT 50;

-- 4) Top clientes por receita (90 dias)
CREATE OR REPLACE VIEW vw_insights_top_clientes AS
SELECT
    o.customer_name AS cliente,
    COUNT(DISTINCT o.id) AS pedidos,
    SUM(o.total) AS receita
FROM tiny_orders o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY o.customer_name
ORDER BY receita DESC
LIMIT 50;

-- 5) Curva ABC (produtos, últimos 90 dias)
CREATE OR REPLACE VIEW vw_insights_curva_abc AS
WITH receita_produto AS (
    SELECT
        i.sku,
        i.product_name,
        SUM(i.total) AS receita
    FROM tiny_order_items i
    JOIN tiny_orders o ON o.id = i.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY i.sku, i.product_name
),
classificacao AS (
    SELECT
        sku,
        product_name,
        receita,
        ROUND(SUM(receita) OVER (ORDER BY receita DESC) / NULLIF(SUM(receita) OVER (),0) * 100, 1) AS perc_acum
    FROM receita_produto
)
SELECT
    sku,
    product_name,
    receita,
    perc_acum,
    CASE
        WHEN perc_acum <= 70 THEN 'A'
        WHEN perc_acum <= 90 THEN 'B'
        ELSE 'C'
    END AS classe
FROM classificacao
ORDER BY receita DESC;

-- 6) Ticket médio mensal por canal (últimos 6 meses)
CREATE OR REPLACE VIEW vw_insights_ticket_medio AS
SELECT
    o.channel,
    DATE_TRUNC('month', o.order_date) AS mes,
    COUNT(DISTINCT o.id) AS pedidos,
    SUM(o.total) AS receita,
    ROUND(SUM(o.total) / NULLIF(COUNT(DISTINCT o.id), 0), 2) AS ticket_medio
FROM tiny_orders o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '180 days'
GROUP BY o.channel, DATE_TRUNC('month', o.order_date)
ORDER BY mes DESC;
