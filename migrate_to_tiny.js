// migrate_to_tiny.js — versão corrigida, mapeando colunas reais
require('dotenv').config();
const { Client } = require('pg');

(async () => {
  const c = new Client({
    host: process.env.PGHOST,
    port: +process.env.PGPORT || 5432,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
  });
  await c.connect();
  console.log("🔄 Iniciando migração de orders → tiny_orders e order_items → tiny_order_items");

  // Mover pedidos (orders)
  const migOrders = `
    INSERT INTO public.tiny_orders (id, order_number, order_date, channel, customer_name, total, status, raw_json)
    SELECT
      id,
      numero AS order_number,
      data_pedido::date AS order_date,
      canal_venda AS channel,
      cliente_nome AS customer_name,
      valor_total AS total,
      situacao AS status,
      to_jsonb(orders.*) AS raw_json
    FROM public.orders
    ON CONFLICT (id) DO NOTHING;
  `;
  await c.query(migOrders);
  console.log("✅ Pedidos migrados para tiny_orders");

  // Mover itens (order_items)
  const migItems = `
    INSERT INTO public.tiny_order_items (order_id, sku, product_name, qty, price, total, raw_json)
    SELECT
      order_id,
      produto_codigo AS sku,
      produto_nome AS product_name,
      quantidade AS qty,
      valor_unitario AS price,
      valor_total AS total,
      to_jsonb(order_items.*) AS raw_json
    FROM public.order_items
    ON CONFLICT DO NOTHING;
  `;
  await c.query(migItems);
  console.log("✅ Itens migrados para tiny_order_items");

  await c.end();
  console.log("🎉 Migração concluída com sucesso.");
})();
