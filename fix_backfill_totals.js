// fix_backfill_totals.js
require('dotenv').config();
const { Client } = require('pg');

(async () => {
  const client = new Client({
    host: process.env.PGHOST || 'localhost',
    port: process.env.PGPORT || 5432,
    database: process.env.PGDATABASE || 'iah_plumas',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD,
  });

  try {
    await client.connect();
    console.log('🔧 Conectado. Iniciando backfill de totais...');

    // 1) Item: total = qty * price quando vier nulo/zero
    const updItems = `
      UPDATE public.tiny_order_items
      SET total = (COALESCE(qty,0) * COALESCE(price,0))
      WHERE (total IS NULL OR total = 0)
    `;
    const r1 = await client.query(updItems);
    console.log(`✅ Itens atualizados (total): ${r1.rowCount}`);

    // 2) Pedido: total = soma dos itens (COALESCE do total do item ou qty*price)
    const updOrders = `
      WITH soma AS (
        SELECT oi.order_id,
               SUM(COALESCE(oi.total, COALESCE(oi.qty,0) * COALESCE(oi.price,0))) AS sum_total
        FROM public.tiny_order_items oi
        GROUP BY oi.order_id
      )
      UPDATE public.tiny_orders o
      SET total = s.sum_total
      FROM soma s
      WHERE o.id = s.order_id
        AND (o.total IS NULL OR o.total = 0);
    `;
    const r2 = await client.query(updOrders);
    console.log(`✅ Pedidos atualizados (total): ${r2.rowCount}`);

    // 3) Canal: padroniza nulos como '(sem canal)' para o relatório não quebrar
    const updChannel = `
      UPDATE public.tiny_orders
      SET channel = '(sem canal)'
      WHERE channel IS NULL OR channel = '' OR channel ILIKE 'null';
    `;
    const r3 = await client.query(updChannel);
    console.log(`✅ Pedidos com canal padronizado: ${r3.rowCount}`);

    // 4) Recalcular as materialized views
    // Tenta concurrently; se falhar, faz sem concurrently
    async function refreshMV(name) {
      try {
        await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY public.${name};`);
        console.log(`🔄 REFRESH CONCURRENTLY ok: ${name}`);
      } catch (e) {
        console.log(`⚠️  Concurrent falhou em ${name}. Tentando REFRESH normal...`);
        await client.query(`REFRESH MATERIALIZED VIEW public.${name};`);
        console.log(`🔄 REFRESH normal ok: ${name}`);
      }
    }

    await refreshMV('mv_orders_daily');
    await refreshMV('mv_channels_30d');
    await refreshMV('mv_top_products_90d');

    console.log('🎉 Backfill + refresh concluídos.');
  } catch (err) {
    console.error('❌ Erro no backfill:', err);
    process.exit(1);
  } finally {
    await client.end();
  }
})();
