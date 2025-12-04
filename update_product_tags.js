// update_product_tags.js — preenche dw.dim_product.tags a partir da categoria do produto (última parte)
require("dotenv").config();
const axios = require("axios");
const { Client } = require("pg");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;

function pgClient() {
  return new Client({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: {
      rejectUnauthorized: false,
    },
  });
}

// Extrai UMA tag a partir da categoria do produto
function extractTagFromProduct(p) {
  if (!p || !p.categoria) return null;

  // Exemplo de categoria:
  // "Casa, Móveis e Decoração >> Enfeites e Decoração da Casa >> Plumas >> Pluma Aparada"
  const parts = p.categoria.split(">>").map((s) => s.trim()).filter(Boolean);
  if (!parts.length) return null;

  const last = parts[parts.length - 1]; // última parte, ex: "Pluma Aparada"
  return last || null;
}

// Chamada genérica à API Tiny com retry em erros de conexão
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

    throw err;
  }
}

async function main() {
  const db = pgClient();
  console.log("🔌 Conectando ao banco...");
  await db.connect();

    console.log("🔍 Buscando produtos ATIVOS sem tags em dw.dim_product...");
  const { rows } = await db.query(`
    SELECT id_tiny, codigo, nome
    FROM dw.dim_product
    WHERE id_tiny IS NOT NULL
      AND tags IS NULL
      AND situacao = 'A'   -- 🔴 só produtos ativos
    ORDER BY id_tiny
    LIMIT 500;  -- processa em lotes de 500
  `);


  if (!rows.length) {
    console.log("✅ Nenhum produto sem tags encontrado (tags já preenchidas ou marcadas como vazias).");
    await db.end();
    return;
  }

  console.log(`📦 Encontrados ${rows.length} produtos sem tags. Atualizando a partir da API do Tiny...`);

  let ok = 0;
  let fail = 0;

  for (const row of rows) {
    const { id_tiny, codigo, nome } = row;
    console.log(`↪️ Produto id=${id_tiny} | código=${codigo} | nome=${nome}`);

    try {
      const resp = await postTiny("produto.obter", { id: String(id_tiny) });

      if (resp?.retorno?.status !== "OK" || !resp.retorno?.produto) {
        console.warn(
          `⚠️ Tiny retornou status != OK para produto ${id_tiny}:`,
          JSON.stringify(resp?.retorno || {}, null, 2)
        );
        fail++;
        continue;
      }

      const produto = resp.retorno.produto;
      const tag = extractTagFromProduct(produto);

      console.log(`   → Tag extraída da categoria: ${tag || "(nenhuma)"}`);

      // se não tiver categoria, salvamos "" só pra marcar como processado
      const tagToSave = tag || "";

      await db.query(
        `
        UPDATE dw.dim_product
        SET tags = $1,
            updated_at = NOW()
        WHERE codigo = $2;
      `,
        [tagToSave, codigo]
      );

      ok++;
    } catch (err) {
      console.warn(
        `⚠️ Erro ao atualizar produto ${id_tiny} (${codigo}):`,
        err.message || err
      );
      fail++;
    }
  }

  await db.end();
  console.log(`\n✅ Tags atualizadas (via categoria) para ${ok} produtos. Falharam ${fail}.`);
  console.log("👋 Fim da atualização de tags.");
}

main().catch((e) => {
  console.error("❌ Erro geral no update_product_tags:", e.message);
  process.exit(1);
});
