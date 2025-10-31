-- Vendas por dia
CREATE OR REPLACE VIEW dw.v_sales_daily AS
SELECT d.date_value AS dia,
       SUM(f.valor_total) AS faturamento,
       SUM(f.quantidade)  AS qty
FROM dw.f_sales f
JOIN dw.dim_date d ON d.date_key = f.date_key
GROUP BY d.date_value
ORDER BY d.date_value;

-- Top 20 SKUs (30 dias)
CREATE OR REPLACE VIEW dw.v_top_skus_30d AS
SELECT f.codigo, COALESCE(dp.nome, f.descricao) AS nome,
       SUM(f.quantidade) AS qty, SUM(f.valor_total) AS valor
FROM dw.f_sales f
LEFT JOIN dw.dim_product dp ON dp.product_key = f.product_key
WHERE dATE(f.date_key::text, 'YYYYMMDD') BETWEEN (CURRENT_DATE - INTERVAL '30 days') AND CURRENT_DATE
GROUP BY f.codigo, COALESCE(dp.nome, f.descricao)
ORDER BY valor DESC
LIMIT 20;
