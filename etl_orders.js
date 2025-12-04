// etl_orders.js — carrega pedidos (últimos 30 dias) + itens; popula staging e f_sales
require("dotenv").config();
const axios = require("axios");
const { Client } = require("pg");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;

function pgClient() {
  return new Client({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
  });
}

function fmtBR(d) {
  return d.toLocaleDateString("pt-BR", { timeZone: "America/Sao_Paulo" });
}

/**
 * Chamada genérica para a API v2 do Tiny com tentativas (retry) em erros de conexão.
 */
async function postTiny(endpoint, form, tentativa = 1) {
  const body = new URLSearchParams({ token: TOKEN, formato: "json", ...form });

  try {
    const { data } = await axios.post(
      `${API_V2}/${endpoint}.php`,
      body.toString(),
      {
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        timeout: 60000,
      }
    );
    return data;
  } catch (err) {
    const code = err.code || (err.cause && err.cause.code);
    console.warn(
      `⚠️ Erro ao chamar Tiny em ${endpoint} (tentativa ${tentativa}):`,
      code || err.message
    );

    // erros de conexão que vale a pena tentar de novo
    const podeTentarDeNovo =
      code === "ECONNRESET" ||
      code === "ETIMEDOUT" ||
      code === "ECONNABORTED";

    if (podeTentarDeNovo && tentativa < 3) {
      const delayMs = 2000 * tentativa;
      console.log(`⏳ Aguardando ${delayMs}ms e tentando novamente...`);
      await new Promise((r) => setTimeout(r, delayMs));
      return postTiny(endpoint, form, tentativa + 1);
    }

    // se chegou aqui, ou não é erro “transitório” ou já esgotou tentativas
    throw err;
  }
}

async function loadOrders() {
  const db = pgClient();
  await db.connect();

  const hoje = new Date();
  const d30 = new Date();
  d30.setDate(hoje.getDate() - 30); // janela de 30 dias
  const dataInicial = fmtBR(d30);
  const dataFinal = fmtBR(hoje);

  console.log(
    `📅 Carregando pedidos do Tiny de ${dataInicial} até ${dataFinal}...`
  );

  let pagina = 1,
    totalPag = 1,
    totPedidos = 0,
    totItens = 0;

  do {
    console.log(`🔎 Buscando página ${pagina} de pedidos...`);

    const resp = await postTiny("pedidos.pesquisa", {
      dataInicial,
      dataFinal,
      pagina: String(pagina),
      pesquisar: "S",
    });

    if (resp?.retorno?.status !== "OK") {
      throw new Error(JSON.stringify(resp?.retorno, null, 2));
    }

    totalPag = Number(resp.retorno.numero_paginas || 1);
    const pedidos = (resp.retorno.pedidos || []).map((p) => p.pedido);

    for (const p of pedidos) {
      // staging pedidos
      await db.query(
        `
        INSERT INTO staging.stg_orders (
          id,
          numero,
          data_pedido,
          situacao,
          cliente_nome,
          vendedor,
          total_pedido,
          raw_json
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (id) DO UPDATE SET
          numero        = EXCLUDED.numero,
          data_pedido   = EXCLUDED.data_pedido,
          situacao      = EXCLUDED.situacao,
          cliente_nome  = EXCLUDED.cliente_nome,
          vendedor      = EXCLUDED.vendedor,
          total_pedido  = EXCLUDED.total_pedido,
          raw_json      = EXCLUDED.raw_json
      `,
        [
          Number(p.id),
          p.numero || null,
          p.data_pedido || p.data_criacao || null,
          p.situacao || null,
          p.nome || p.cliente || null,
          p.vendedor || null,
          p.valor || p.total_pedido || null,
          p,
        ]
      );
      totPedidos++;

      // detalhe para itens — protegemos com try/catch para 1 pedido ruim não derrubar tudo
      try {
        const det = await postTiny(
          "pedido.obter",
          p.id ? { id: String(p.id) } : { numero: String(p.numero) }
        );

        if (det?.retorno?.status === "OK") {
          const pedido = det.retorno.pedido || {};
          const itens = (pedido.itens || []).map((i) => i.item);

          for (const it of itens) {
            await db.query(
              `
              INSERT INTO staging.stg_order_items (
                id_pedido,
                numero_pedido,
                codigo,
                descricao,
                quantidade,
                valor_unit,
                valor_total,
                raw_json
              )
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            `,
              [
                Number(p.id),
                p.numero || null,
                it.codigo || null,
                it.descricao || null,
                Number(it.quantidade || 0),
                Number(it.valor_unitario || it.valor || 0),
                Number(it.valor_total || 0),
                it,
              ]
            );
            totItens++;
          }
        } else {
          console.warn(
            `⚠️ Não foi possível obter detalhes do pedido ${p.id || p.numero}`
          );
        }
      } catch (err) {
        console.warn(
          `⚠️ Erro ao obter detalhes do pedido ${p.id || p.numero}:`,
          err.message || err
        );
      }
    }

    console.log(
      `📄 Página ${pagina}/${totalPag} — pedidos acumulados: ${totPedidos} | itens: ${totItens}`
    );
    pagina++;
  } while (pagina <= totalPag);

  console.log("🧹 Limpando janela de 30 dias em dw.f_sales...");
  // MATERIALIZA FATO f_sales
  // 1) limpar janela (últimos 30 dias) para reprocessar idempotente
  await db.query(`
    DELETE FROM dw.f_sales
    WHERE date_key IN (
      SELECT date_key
      FROM dw.dim_date
      WHERE date_value BETWEEN (CURRENT_DATE - INTERVAL '30 days') AND CURRENT_DATE
    );
  `);

  console.log("💾 Inserindo registros em dw.f_sales a partir da staging...");
  // 2) inserir da staging
  await db.query(`
    INSERT INTO dw.f_sales (
      date_key,
      id_pedido,
      numero_pedido,
      product_key,
      codigo,
      descricao,
      quantidade,
      valor_unit,
      valor_total,
      situacao_pedido
    )
    SELECT
      CAST(
        TO_CHAR(
          TO_DATE(COALESCE(o.data_pedido, CURRENT_DATE::TEXT), 'DD/MM/YYYY'),
          'YYYYMMDD'
        ) AS INTEGER
      ) AS date_key,
      oi.id_pedido,
      oi.numero_pedido,
      dp.product_key,
      oi.codigo,
      oi.descricao,
      oi.quantidade,
      oi.valor_unit,
      oi.valor_total,
      o.situacao
    FROM staging.stg_order_items oi
    JOIN staging.stg_orders o
      ON o.id = oi.id_pedido
    LEFT JOIN dw.dim_product dp
      ON dp.codigo = oi.codigo;
  `);

  await db.end();
  console.log(`✅ Pedidos carregados: ${totPedidos} | Itens carregados: ${totItens}`);
  console.log("✅ Fato f_sales atualizada para os últimos 30 dias.");
}

loadOrders().catch((e) => {
  console.error("❌ ETL pedidos falhou:", e.message);
  process.exit(1);
});
