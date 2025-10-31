// etl_products.js — carrega produtos da API v2 no staging e atualiza dim_product
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

async function postTiny(endpoint, form) {
  const body = new URLSearchParams({ token: TOKEN, formato: "json", ...form });
  const { data } = await axios.post(
    `${API_V2}/${endpoint}.php`,
    body.toString(),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 60000 }
  );
  return data;
}

async function loadProducts() {
  const db = pgClient(); await db.connect();
  let pagina = 1, total = 1, count = 0;
  do {
    const resp = await postTiny("produtos.pesquisa", { pesquisa: "a", pagina: String(pagina) });
    if (resp?.retorno?.status !== "OK") throw new Error(JSON.stringify(resp?.retorno, null, 2));

    total = Number(resp.retorno.numero_paginas || 1);
    const produtos = (resp.retorno.produtos || []).map(p => p.produto);

    for (const p of produtos) {
      await db.query(`
        INSERT INTO staging.stg_products (id, codigo, nome, gtin, unidade, preco, preco_promocional, situacao, localizacao, data_criacao, raw_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (id) DO UPDATE SET
          codigo=EXCLUDED.codigo, nome=EXCLUDED.nome, gtin=EXCLUDED.gtin, unidade=EXCLUDED.unidade,
          preco=EXCLUDED.preco, preco_promocional=EXCLUDED.preco_promocional, situacao=EXCLUDED.situacao,
          localizacao=EXCLUDED.localizacao, data_criacao=EXCLUDED.data_criacao, raw_json=EXCLUDED.raw_json
      `, [
        Number(p.id), p.codigo || null, p.nome || p.descricao || null, p.gtin || null, p.unidade || null,
        p.preco ?? null, p.preco_promocional ?? null, p.situacao || null, p.localizacao || null,
        p.data_criacao || null, p
      ]);
      count++;
    }
    console.log(`Página ${pagina}/${total} — acumulado ${count}`);
    pagina++;
  } while (pagina <= total);

  // Upsert para dimensão
  await db.query(`
    INSERT INTO dw.dim_product (id_tiny, codigo, nome, gtin, unidade, situacao, localizacao, preco, preco_promocional, created_at_raw)
    SELECT s.id, s.codigo, s.nome, s.gtin, s.unidade, s.situacao, s.localizacao, s.preco, s.preco_promocional, s.data_criacao
    FROM staging.stg_products s
    ON CONFLICT (codigo) DO UPDATE SET
      id_tiny=EXCLUDED.id_tiny, nome=EXCLUDED.nome, gtin=EXCLUDED.gtin, unidade=EXCLUDED.unidade,
      situacao=EXCLUDED.situacao, localizacao=EXCLUDED.localizacao, preco=EXCLUDED.preco,
      preco_promocional=EXCLUDED.preco_promocional, created_at_raw=EXCLUDED.created_at_raw,
      updated_at=NOW();
  `);

  await db.end();
  console.log(`✅ Produtos carregados: ${count}`);
}

loadProducts().catch(e => {
  console.error("❌ ETL produtos falhou:", e.message);
  process.exit(1);
});
