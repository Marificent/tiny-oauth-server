// etl_products.js — carrega produtos da API v2 no staging e atualiza dim_product
require("dotenv").config();
const axios = require("axios");
const { Client } = require("pg");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;

// Extrai tags do JSON do Tiny em um formato "tag1,tag2,tag3"
function extractTagsFromProduct(p) {
  if (!p) return null;

  // 1) Caso venha um array "tags_produto": [{ idTag, descricao }]
  if (Array.isArray(p.tags_produto)) {
    return p.tags_produto
      .map((t) => t.descricao || t.nome || String(t.idTag || t.id || ""))
      .filter(Boolean)
      .join(",");
  }

  // 2) Caso venha um array "tags": ["tag1", "tag2"] ou [{ descricao }]
  if (Array.isArray(p.tags)) {
    return p.tags
      .map((t) => {
        if (typeof t === "string") return t;
        return t.descricao || t.nome || String(t.id || "");
      })
      .filter(Boolean)
      .join(",");
  }

  // 3) Caso venha como string única (mais raro)
  if (typeof p.tags === "string") {
    return p.tags;
  }

  return null;
}

// Converte "31/10/2023 10:24:18" -> "2023-10-31 10:24:18"
function parseTinyDateTime(str) {
  if (!str) return null;
  if (str instanceof Date) return str; // se em algum momento já vier como Date

  const [datePart, timePart = "00:00:00"] = String(str).split(" ");
  const [dd, mm, yyyy] = datePart.split("/");

  if (!dd || !mm || !yyyy) return null;

  // Postgres entende bem "YYYY-MM-DD HH:MM:SS"
  return `${yyyy}-${mm}-${dd} ${timePart}`;
}

function pgClient() {
  return new Client({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: {
      rejectUnauthorized: false, // 🔥 obrigatório pro Postgres do Render
    },
  });
}

async function postTiny(endpoint, form) {
  const body = new URLSearchParams({ token: TOKEN, formato: "json", ...form });
  const { data } = await axios.post(
    `${API_V2}/${endpoint}.php`,
    body.toString(),
    {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      timeout: 60000,
    }
  );
  return data;
}

async function loadProducts() {
  const db = pgClient();
  await db.connect();

  let pagina = 1,
    total = 1,
    count = 0;

  do {
    const resp = await postTiny("produtos.pesquisa", {
      pesquisa: "a",
      pagina: String(pagina),
    });

    if (resp?.retorno?.status !== "OK") {
      throw new Error(JSON.stringify(resp?.retorno, null, 2));
    }

    total = Number(resp.retorno.numero_paginas || 1);
    const produtos = (resp.retorno.produtos || []).map((p) => p.produto);

    for (const p of produtos) {
      await db.query(
        `
        INSERT INTO staging.stg_products (
          id,
          codigo,
          nome,
          gtin,
          unidade,
          preco,
          preco_promocional,
          situacao,
          localizacao,
          data_criacao,
          tags,
          raw_json
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (id) DO UPDATE SET
          codigo            = EXCLUDED.codigo,
          nome              = EXCLUDED.nome,
          gtin              = EXCLUDED.gtin,
          unidade           = EXCLUDED.unidade,
          preco             = EXCLUDED.preco,
          preco_promocional = EXCLUDED.preco_promocional,
          situacao          = EXCLUDED.situacao,
          localizacao       = EXCLUDED.localizacao,
          data_criacao      = EXCLUDED.data_criacao,
          tags              = EXCLUDED.tags,
          raw_json          = EXCLUDED.raw_json
        `,
        [
          Number(p.id),
          p.codigo || null,
          p.nome || p.descricao || null,
          p.gtin || null,
          p.unidade || null,
          p.preco ?? null,
          p.preco_promocional ?? null,
          p.situacao || null,
          p.localizacao || null,
          parseTinyDateTime(p.data_criacao) || null,
          extractTagsFromProduct(p),
          p,
        ]
      );

      count++;
    }

    console.log(`Página ${pagina}/${total} — acumulado ${count}`);
    pagina++;
  } while (pagina <= total);

  // Upsert para dimensão — deduplicando por codigo
  await db.query(`
    INSERT INTO dw.dim_product (
      id_tiny,
      codigo,
      nome,
      gtin,
      unidade,
      situacao,
      localizacao,
      preco,
      preco_promocional,
      created_at_raw,
      tags
    )
    SELECT
      s.id             AS id_tiny,
      s.codigo,
      s.nome,
      s.gtin,
      s.unidade,
      s.situacao,
      s.localizacao,
      s.preco,
      s.preco_promocional,
      s.data_criacao   AS created_at_raw,
      s.tags
    FROM (
      SELECT DISTINCT ON (codigo)
        id,
        codigo,
        nome,
        gtin,
        unidade,
        situacao,
        localizacao,
        preco,
        preco_promocional,
        data_criacao,
        tags
      FROM staging.stg_products
      WHERE codigo IS NOT NULL
      ORDER BY codigo, data_criacao DESC
    ) s
    ON CONFLICT (codigo) DO UPDATE SET
      id_tiny           = EXCLUDED.id_tiny,
      nome              = EXCLUDED.nome,
      gtin              = EXCLUDED.gtin,
      unidade           = EXCLUDED.unidade,
      situacao          = EXCLUDED.situacao,
      localizacao       = EXCLUDED.localizacao,
      preco             = EXCLUDED.preco,
      preco_promocional = EXCLUDED.preco_promocional,
      created_at_raw    = EXCLUDED.created_at_raw,
      tags              = EXCLUDED.tags,
      updated_at        = NOW();
  `);

  await db.end();
  console.log(`✅ Produtos carregados: ${count}`);
}

loadProducts().catch((e) => {
  console.error("❌ ETL produtos falhou:", e.message);
  process.exit(1);
});
