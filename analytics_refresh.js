// analytics_refresh.js
require("dotenv").config();
const { Client } = require("pg");

(async () => {
  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
  });

  try {
    await client.connect();
    console.log("🔧 Criando índices únicos e atualizando materialized views...");

    // Garantir índices únicos exigidos pelo REFRESH CONCURRENTLY
    await client.query(`
      -- mv_orders_daily (day + channel opcional, mas aqui deixamos só day)
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_mv_orders_daily_day'
        ) THEN
          CREATE UNIQUE INDEX ux_mv_orders_daily_day ON public.mv_orders_daily("day");
        END IF;
      END$$;

      -- mv_channels_30d
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_mv_channels_30d_channel'
        ) THEN
          CREATE UNIQUE INDEX ux_mv_channels_30d_channel ON public.mv_channels_30d("channel");
        END IF;
      END$$;

      -- mv_top_products_90d (rank é único)
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_mv_top_products_90d_rank'
        ) THEN
          CREATE UNIQUE INDEX ux_mv_top_products_90d_rank ON public.mv_top_products_90d("rank");
        END IF;
      END$$;
    `);

    // REFRESH (concorrente p/ não travar leituras)
    await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_orders_daily;`);
    await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_channels_30d;`);
    await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_top_products_90d;`);

    // Valida sem depender de nomes de colunas (apenas COUNT(*))
    const { rows: r1 } = await client.query(`SELECT COUNT(*)::int AS n FROM public.mv_orders_daily;`);
    const { rows: r2 } = await client.query(`SELECT COUNT(*)::int AS n FROM public.mv_channels_30d;`);
    const { rows: r3 } = await client.query(`SELECT COUNT(*)::int AS n FROM public.mv_top_products_90d;`);

    console.log(`✅ Atualização concorrente concluída!`);
    console.log(`   mv_orders_daily: ${r1[0].n} linhas`);
    console.log(`   mv_channels_30d: ${r2[0].n} linhas`);
    console.log(`   mv_top_products_90d: ${r3[0].n} linhas`);
  } catch (err) {
    console.error("❌ Erro ao atualizar:", err.message || err);
    process.exit(1);
  } finally {
    await client.end();
  }
})();
