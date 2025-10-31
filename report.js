// report.js — lê do Postgres e gera um resumo em texto + console
require("dotenv").config();
const { Client } = require("pg");
const fs = require("fs");
const path = require("path");

(async () => {
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD,
  } = process.env;

  const client = new Client({
    host: PGHOST || "localhost",
    port: Number(PGPORT || 5432),
    database: PGDATABASE || "iaplumas",
    user: PGUSER || "postgres",
    password: PGPASSWORD,
    application_name: "report.js",
  });

  const today = new Date().toISOString().slice(0, 10);
  const logsDir = path.join(process.cwd(), "logs");
  if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
  const outFile = path.join(logsDir, `report_${today}.txt`);

  function line(s = "") {
    console.log(s);
    fs.appendFileSync(outFile, s + "\n");
  }

  try {
    fs.writeFileSync(outFile, ""); // zera arquivo do dia
    line(`📊 IA Plumas — Relatório ${today}`);
    line("=".repeat(60));

    await client.connect();

    // 1) Canais (últimos 30 dias)
    let canais = [];
    try {
      const { rows } = await client.query(`
        SELECT channel, revenue::numeric AS revenue, orders_count
        FROM public.mv_channels_30d
        ORDER BY revenue DESC NULLS LAST;
      `);
      canais = rows;
    } catch (e) {
      // view pode não existir ainda
    }

    line("\nCanais de venda (últimos 30 dias, por receita):");
    if (!canais.length) {
      line("  (sem dados nos últimos 30 dias)");
    } else {
      for (const r of canais) {
        line(`  - ${r.channel || "(sem canal)"} | receita: R$ ${Number(r.revenue || 0).toFixed(2)} | pedidos: ${r.orders_count || 0}`);
      }
    }

    // 2) Top 10 produtos por receita (90 dias)
    let top = [];
    try {
      const { rows } = await client.query(`
        SELECT rank, product_name, sku, revenue::numeric AS revenue, qty
        FROM public.mv_top_products_90d
        ORDER BY rank ASC
        LIMIT 10;
      `);
      top = rows;
    } catch (e) {}

    line("\n🏆 Top 10 produtos por receita (últimos 90 dias):");
    if (!top.length) {
      line("  (sem dados)");
    } else {
      let i = 1;
      for (const r of top) {
        line(`  #${String(i).padStart(2," ")} ${r.product_name} [${r.sku}] | receita: R$ ${Number(r.revenue||0).toFixed(2)} | qte: ${r.qty||0}`);
        i++;
      }
    }

    // 3) Itens/dia (média últimos 14 dias) usando mv_orders_daily
    let media = null;
    try {
      const { rows } = await client.query(`
        SELECT AVG(items)::numeric AS avg_items
        FROM public.mv_orders_daily
        WHERE day >= (CURRENT_DATE - INTERVAL '14 days');
      `);
      media = rows?.[0]?.avg_items;
    } catch (e) {}

    line("\n📦 Itens/dia (janela 90d, últimos 14 dias):");
    if (!media) {
      line("  (sem dados)");
    } else {
      line(`  Média itens/dia: ${Number(media).toFixed(1)}`);
    }

    line("\n✅ Relatório concluído.");
    line("=".repeat(60));
    console.log(`\n📝 Arquivo salvo: ${outFile}`);
  } catch (err) {
    console.error("❌ Erro ao gerar relatório:", err?.message || err);
    process.exitCode = 1;
  } finally {
    try { await client.end(); } catch {}
  }
})();
