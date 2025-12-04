// ingest_products.js
require("dotenv").config();
const axios = require("axios");
const { Client } = require("pg");

const {
  TINY_API_TOKEN,          // seu token v2 (já salvo no .env)
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE,
} = process.env;

const db = new Client({
  host: PGHOST,
  port: PGPORT,
  user: PGUSER,
  password: PGPASSWORD,
  database: PGDATABASE,
  ssl: {
    rejectUnauthorized: false, // 🔥 obrigatório pro Postgres do Render
  },
});


function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function toNullNumber(v) {
  if (v === undefined || v === null || v === "" || Number.isNaN(Number(v))) return null;
  return Number(v);
}

function parseBrDateTime(s) {
  // Ex: "31/10/2023 10:24:18" -> "2023-10-31T10:24:18"
  if (!s || typeof s !== "string") return null;
  const [d, t] = s.split(" ");
  if (!d) return null;
  const [dd, mm, yyyy] = d.split("/");
  const time = t || "00:00:00";
  return `${yyyy}-${mm}-${dd}T${time}`;
}

async function upsertProduct(p) {
  const sql = `
    INSERT INTO dw.products (
      id, codigo, nome, preco, preco_promocional, unidade, gtin,
      tipo_variacao, localizacao, preco_custo, preco_custo_medio,
      situacao, criado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    ON CONFLICT (id) DO UPDATE SET
      codigo = EXCLUDED.codigo,
      nome = EXCLUDED.nome,
      preco = EXCLUDED.preco,
      preco_promocional = EXCLUDED.preco_promocional,
      unidade = EXCLUDED.unidade,
      gtin = EXCLUDED.gtin,
      tipo_variacao = EXCLUDED.tipo_variacao,
      localizacao = EXCLUDED.localizacao,
      preco_custo = EXCLUDED.preco_custo,
      preco_custo_medio = EXCLUDED.preco_custo_medio,
      situacao = EXCLUDED.situacao,
      criado_em = EXCLUDED.criado_em
  `;
  const values = [
    Number(p.id),
    p.codigo ?? null,
    p.nome ?? null,
    toNullNumber(p.preco),
    toNullNumber(p.preco_promocional),
    p.unidade ?? null,
    p.gtin ?? null,
    p.tipoVariacao ?? null,
    p.localizacao ?? null,
    toNullNumber(p.preco_custo),
    toNullNumber(p.preco_custo_medio),
    p.situacao ?? null,
    parseBrDateTime(p.data_criacao),
  ];
  await db.query(sql, values);
}

async function fetchPage(pagina) {
  const url = "https://api.tiny.com.br/api2/produtos.pesquisa.php";
  const params = {
    token: TINY_API_TOKEN,
    formato: "json",
    pagina,
    // dica: adicione "pesquisa" para filtrar; omitido = tudo
  };
  const { data } = await axios.get(url, { params, timeout: 30000 });
  return data;
}

async function main() {
  if (!TINY_API_TOKEN) {
    console.error("❌ TINY_API_TOKEN não está no .env");
    process.exit(1);
  }
  await db.connect();

  let pagina = 1;
  let totalPaginas = 1;
  let totalInseridos = 0;

  console.log("📦 Iniciando carga de produtos v2...");

  while (pagina <= totalPaginas) {
    try {
      const data = await fetchPage(pagina);

      const ret = data?.retorno;
      if (!ret) throw new Error("Resposta sem 'retorno'");

      // Trate erros do Tiny
      if (ret.status === "Erro") {
        const erroMsg = JSON.stringify(ret.erros || ret.mensagens || ret);
        // API bloqueada / rate limit -> espere e tente de novo
        if (erroMsg.includes("API Bloqueada") || erroMsg.includes("número de acessos")) {
          console.log(`⏳ Rate limit na página ${pagina}. Aguardando 20s...`);
          await sleep(20000);
          continue; // re-tenta a mesma página
        }
        throw new Error(`Erro Tiny página ${pagina}: ${erroMsg}`);
      }

      totalPaginas = Number(ret.numero_paginas || ret.numero_paginas_total || totalPaginas) || totalPaginas;

      const lista = ret.produtos || [];
      for (const wrap of lista) {
        const p = wrap.produto || wrap; // segurança
        await upsertProduct(p);
        totalInseridos++;
      }

      console.log(`✅ Página ${pagina}/${totalPaginas} gravada (${lista.length} itens).`);
      pagina++;

      // Respeitar limites: pausa suave
      await sleep(500);
    } catch (e) {
      const msg = e?.response?.data ? JSON.stringify(e.response.data) : e.message;
      console.log(`⚠️ Falha na página ${pagina}: ${msg}`);
      // backoff curto e re-tenta a mesma página
      await sleep(8000);
    }
  }

  console.log(`🎉 Concluído. Produtos inseridos/atualizados: ${totalInseridos}`);
  await db.end();
}

main().catch(async (e) => {
  console.error("❌ Erro geral:", e.message);
  try { await db.end(); } catch {}
  process.exit(1);
});
