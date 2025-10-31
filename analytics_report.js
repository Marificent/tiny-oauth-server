// analytics_report.js
require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.PGHOST || "localhost",
  port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
  user: process.env.PGUSER || "postgres",
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE || "iah_plumas",
});

function money(n) {
  if (n == null) return "R$ 0,00";
  return "R$ " + Number(n).toFixed(2).replace(".", ",");
}

function dateFmt(d) {
  const dt = new Date(d);
  const dd = String(dt.getDate()).padStart(2, "0");
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const yyyy = dt.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

async function main() {
  const client = await pool.connect();
  try {
    console.log("📊 Gerando relatório…\n");

    // 1) Últimos 14 dias: pedidos, receita, ticket
    const last14 = await client.query(
      `SELECT dia, qtd_pedidos, receita_total, ticket_medio
         FROM public.mv_orders_daily
       ORDER BY dia DESC
       LIMIT 14`
    );

    // 2) Últimos 30 dias: por canal
    const byChannel = await client.query(
      `WITH ult30 AS (
         SELECT *
         FROM public.mv_orders_by_channel
         WHERE dia >= now() - interval '30 days'
       )
       SELECT canal,
              SUM(qtd_pedidos)::int AS qtd_pedidos,
              SUM(receita_total)::numeric(14,2) AS receita_total
       FROM ult30
       GROUP BY canal
       ORDER BY receita_total DESC NULLS LAST`
    );

    // 3) Top produtos 90d por receita (top 10)
    const topProdReceita = await client.query(
      `SELECT produto_codigo, COALESCE(produto_nome,'(sem nome)') AS produto_nome,
              receita_total, qtd_total
         FROM public.mv_top_products_90d
       ORDER BY receita_total DESC, qtd_total DESC
       LIMIT 10`
    );

    // 4) Itens/dia 90d para média de itens por pedido (aproximação)
    const itemsDaily = await client.query(
      `SELECT dia, itens_vendidos, receita_itens
         FROM public.mv_items_daily_90d
       ORDER BY dia DESC
       LIMIT 14`
    );

    // ===== Impressão =====
    // 1) Headline últimos 14 dias
    let totalPedidos14 = 0;
    let totalReceita14 = 0;
    for (const r of last14.rows) {
      totalPedidos14 += Number(r.qtd_pedidos || 0);
      totalReceita14 += Number(r.receita_total || 0);
    }
    const ticketMedio14 = totalPedidos14 ? totalReceita14 / totalPedidos14 : 0;

    console.log("== Últimos 14 dias ==");
    console.log(`Pedidos: ${totalPedidos14}`);
    console.log(`Receita: ${money(totalReceita14)}`);
    console.log(`Ticket médio: ${money(ticketMedio14)}\n`);

    console.log("📅 Série diária (últimos 14 dias):");
    for (const r of last14.rows.reverse()) {
      console.log(
        `  ${dateFmt(r.dia)} | pedidos: ${r.qtd_pedidos} | receita: ${money(r.receita_total)} | ticket: ${money(r.ticket_medio)}`
      );
    }
    console.log("");

    // 2) Canais (30d)
    console.log("🛒 Canais de venda (últimos 30 dias, por receita):");
    if (byChannel.rows.length === 0) {
      console.log("  (sem dados nos últimos 30 dias)\n");
    } else {
      for (const r of byChannel.rows) {
        console.log(
          `  ${r.canal.padEnd(20)} | pedidos: ${String(r.qtd_pedidos).padStart(4)} | receita: ${money(r.receita_total)}`
        );
      }
      console.log("");
    }

    // 3) Top produtos 90d
    console.log("🏆 Top 10 produtos por receita (últimos 90 dias):");
    if (topProdReceita.rows.length === 0) {
      console.log("  (sem dados)\n");
    } else {
      let rank = 1;
      for (const r of topProdReceita.rows) {
        const nome = r.produto_nome.length > 60 ? r.produto_nome.slice(0, 57) + "..." : r.produto_nome;
        console.log(
          `  #${String(rank).padStart(2)} ${nome} [${r.produto_codigo || "-"}] | receita: ${money(r.receita_total)} | qte: ${r.qtd_total}`
        );
        rank++;
      }
      console.log("");
    }

    // 4) Itens/dia (últimos 14 de 90d)
    let somaItens = 0;
    for (const r of itemsDaily.rows) somaItens += Number(r.itens_vendidos || 0);
    const mediaItensDia = itemsDaily.rows.length ? somaItens / itemsDaily.rows.length : 0;
    console.log("📦 Itens/dia (janela 90d, últimos 14 dias):");
    console.log(`  Média itens/dia: ${mediaItensDia.toFixed(1)}\n`);

    console.log("✅ Relatório concluído.");
  } catch (err) {
    console.error("❌ Erro no relatório:", err.message || err);
  } finally {
    await pool.end();
  }
}

main();
