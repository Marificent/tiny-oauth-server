// run_queries.js
require('dotenv').config();
const { Client } = require('pg');

function fmtMoney(n) {
  if (n === null || n === undefined) return null;
  return Number(n).toFixed(2);
}

async function main() {
  const client = new Client({
    host: process.env.PGHOST || 'localhost',
    port: process.env.PGPORT || 5432,
    database: process.env.PGDATABASE || 'iah_plumas',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD,
  });

  // janelas padrão
  const today = new Date();
  const toDate = today.toISOString().slice(0,10); // yyyy-mm-dd
  const from30 = new Date(today); from30.setDate(today.getDate() - 30);
  const from60 = new Date(today); from60.setDate(today.getDate() - 60);
  const from90 = new Date(today); from90.setDate(today.getDate() - 90);
  const from30Str = from30.toISOString().slice(0,10);
  const from60Str = from60.toISOString().slice(0,10);
  const from90Str = from90.toISOString().slice(0,10);

  try {
    await client.connect();
    console.log('🔎 Conectado. Executando consultas...\n');

    // 1) Produtos com queda de receita: últimos 30d vs 30d anteriores
    console.log('1) Produtos com QUEDA de receita — 30d vs 30d anteriores (Top 20):');
    const q1 = `
      WITH cur AS (
        SELECT i.sku, i.product_name,
               SUM(i.total) AS receita_cur
        FROM public.tiny_order_items i
        JOIN public.tiny_orders o ON o.id = i.order_id
        WHERE o.order_date >= $1::date AND o.order_date < $2::date
          AND o.status ILIKE '%Aprov%'
        GROUP BY i.sku, i.product_name
      ),
      prev AS (
        SELECT i.sku,
               SUM(i.total) AS receita_prev
        FROM public.tiny_order_items i
        JOIN public.tiny_orders o ON o.id = i.order_id
        WHERE o.order_date >= $3::date AND o.order_date < $1::date
          AND o.status ILIKE '%Aprov%'
        GROUP BY i.sku
      )
      SELECT c.sku, c.product_name,
             COALESCE(p.receita_prev,0) AS receita_prev,
             COALESCE(c.receita_cur,0)  AS receita_cur,
             CASE
               WHEN COALESCE(p.receita_prev,0)=0 THEN NULL
               ELSE (COALESCE(c.receita_cur,0) - COALESCE(p.receita_prev,0)) / COALESCE(p.receita_prev,1)
             END AS var_pct
      FROM cur c
      LEFT JOIN prev p ON p.sku = c.sku
      WHERE COALESCE(c.receita_cur,0) < COALESCE(p.receita_prev,0)  -- queda
      ORDER BY var_pct ASC NULLS LAST, receita_prev DESC
      LIMIT 20;
    `;
    let r1 = await client.query(q1, [from30Str, toDate, from60Str]);
    console.table(r1.rows.map(x => ({
      sku: x.sku, produto: x.product_name,
      receita_prev: fmtMoney(x.receita_prev),
      receita_cur: fmtMoney(x.receita_cur),
      var_pct: x.var_pct === null ? null : (x.var_pct*100).toFixed(1) + '%'
    })));

    // 2) Canais com MAIOR CRESCIMENTO semana-a-semana (últimas 8 semanas)
    console.log('\n2) Canais com maior crescimento WoW (últimas 8 semanas, Top 15):');
    const q2 = `
      WITH wk AS (
        SELECT COALESCE(o.channel, '(sem canal)') AS canal,
               date_trunc('week', o.order_date)::date AS semana,
               SUM(o.total) AS receita
        FROM public.tiny_orders o
        WHERE o.order_date >= (CURRENT_DATE - INTERVAL '56 days')
          AND o.status ILIKE '%Aprov%'
        GROUP BY 1,2
      ),
      wlag AS (
        SELECT canal, semana, receita,
               LAG(receita) OVER (PARTITION BY canal ORDER BY semana) AS receita_ant
        FROM wk
      )
      SELECT canal, semana, receita, receita_ant,
             CASE WHEN receita_ant IS NULL OR receita_ant=0 THEN NULL
                  ELSE (receita - receita_ant)/receita_ant END AS var_pct
      FROM wlag
      WHERE receita_ant IS NOT NULL
      ORDER BY var_pct DESC NULLS LAST
      LIMIT 15;
    `;
    let r2 = await client.query(q2);
    console.table(r2.rows.map(x => ({
      canal: x.canal,
      semana: x.semana?.toISOString?.() ? x.semana.toISOString().slice(0,10) : x.semana,
      receita: fmtMoney(x.receita),
      receita_ant: fmtMoney(x.receita_ant),
      var_pct: x.var_pct === null ? null : (x.var_pct*100).toFixed(1) + '%'
    })));

    // 3) Ticket médio por canal / mês (últimos 6 meses)
    console.log('\n3) Ticket médio por canal / mês (últimos 6 meses):');
    const q3 = `
      SELECT COALESCE(o.channel,'(sem canal)') AS canal,
             date_trunc('month', o.order_date)::date AS mes,
             COUNT(*) AS pedidos,
             SUM(o.total) AS receita,
             CASE WHEN COUNT(*)=0 THEN 0 ELSE SUM(o.total)/COUNT(*) END AS ticket_medio
      FROM public.tiny_orders o
      WHERE o.order_date >= (date_trunc('month', CURRENT_DATE) - INTERVAL '5 months')
        AND o.status ILIKE '%Aprov%'
      GROUP BY 1,2
      ORDER BY 2 DESC, 1;
    `;
    let r3 = await client.query(q3);
    console.table(r3.rows.map(x => ({
      canal: x.canal,
      mes: x.mes?.toISOString?.() ? x.mes.toISOString().slice(0,10) : x.mes,
      pedidos: Number(x.pedidos),
      receita: fmtMoney(x.receita),
      ticket_medio: fmtMoney(x.ticket_medio)
    })));

    // 4) Curva ABC de produtos por receita (90 dias)
    console.log('\n4) Curva ABC (produtos, últimos 90 dias, Top 30):');
    const q4 = `
      WITH base AS (
        SELECT i.sku, i.product_name, SUM(i.total) AS receita
        FROM public.tiny_order_items i
        JOIN public.tiny_orders o ON o.id = i.order_id
        WHERE o.order_date >= $1::date AND o.order_date < $2::date
          AND o.status ILIKE '%Aprov%'
        GROUP BY i.sku, i.product_name
      ),
      rk AS (
        SELECT b.*,
               RANK() OVER (ORDER BY receita DESC) AS pos,
               SUM(receita) OVER () AS receita_total,
               SUM(receita) OVER (ORDER BY receita DESC) AS receita_acum
        FROM base b
      )
      SELECT sku, product_name, receita,
             receita_acum/receita_total AS perc_acum,
             CASE
               WHEN receita_acum/receita_total <= 0.80 THEN 'A'
               WHEN receita_acum/receita_total <= 0.95 THEN 'B'
               ELSE 'C'
             END AS classe
      FROM rk
      ORDER BY receita DESC
      LIMIT 30;
    `;
    let r4 = await client.query(q4, [from90Str, toDate]);
    console.table(r4.rows.map(x => ({
      sku: x.sku, produto: x.product_name,
      receita: fmtMoney(x.receita),
      perc_acum: (Number(x.perc_acum)*100).toFixed(1) + '%',
      classe: x.classe
    })));

    // 5) Margem bruta estimada por produto (se houver cost_price), 90 dias
    console.log('\n5) Margem bruta estimada (90 dias, Top 20 por receita):');
    const q5 = `
      SELECT i.sku, i.product_name,
             SUM(i.qty) AS qte,
             SUM(i.total) AS receita,
             SUM(i.qty * COALESCE(p.cost_price,0)) AS custo_estimado,
             (SUM(i.total) - SUM(i.qty * COALESCE(p.cost_price,0))) AS margem_bruta,
             CASE WHEN SUM(i.total)=0 THEN NULL
                  ELSE (SUM(i.total) - SUM(i.qty * COALESCE(p.cost_price,0)))/SUM(i.total)
             END AS margem_pct
      FROM public.tiny_order_items i
      JOIN public.tiny_orders o ON o.id = i.order_id
      LEFT JOIN public.tiny_products p ON p.sku = i.sku
      WHERE o.order_date >= $1::date AND o.order_date < $2::date
        AND o.status ILIKE '%Aprov%'
      GROUP BY i.sku, i.product_name
      HAVING SUM(i.total) > 0
      ORDER BY receita DESC
      LIMIT 20;
    `;
    let r5 = await client.query(q5, [from90Str, toDate]);
    console.table(r5.rows.map(x => ({
      sku: x.sku, produto: x.product_name,
      receita: fmtMoney(x.receita),
      custo: fmtMoney(x.custo_estimado),
      margem: fmtMoney(x.margem_bruta),
      margem_pct: x.margem_pct === null ? null : (x.margem_pct*100).toFixed(1) + '%'
    })));

    // 6) Top clientes por receita (90 dias) — usando nome
    console.log('\n6) Top clientes por receita (90 dias, Top 20):');
    const q6 = `
      SELECT COALESCE(o.customer_name,'(sem nome)') AS cliente,
             COUNT(DISTINCT o.id) AS pedidos,
             SUM(o.total) AS receita
      FROM public.tiny_orders o
      WHERE o.order_date >= $1::date AND o.order_date < $2::date
        AND o.status ILIKE '%Aprov%'
      GROUP BY 1
      ORDER BY receita DESC NULLS LAST
      LIMIT 20;
    `;
    let r6 = await client.query(q6, [from90Str, toDate]);
    console.table(r6.rows.map(x => ({
      cliente: x.cliente,
      pedidos: Number(x.pedidos),
      receita: fmtMoney(x.receita)
    })));

    console.log('\n✅ Consultas concluídas.');
  } catch (err) {
    console.error('❌ Erro ao executar consultas:', err);
    process.exit(1);
  } finally {
    process.exit(0);
  }
}

main();
