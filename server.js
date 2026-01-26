// ====== SUPER-SHIM DE DIAGNÓSTICO ======
console.log("[BOOT] entrando em server.js");

const realExit = process.exit;
process.exit = (code) => {
  console.error("[BOOT] process.exit chamado com código:", code, "\nSTACK:", new Error().stack);
  realExit(code || 0);
};

process.on("uncaughtException", (err) => {
  console.error("[BOOT] uncaughtException:", err && err.stack || err);
});
process.on("unhandledRejection", (reason) => {
  console.error("[BOOT] unhandledRejection:", reason);
});

setInterval(() => {}, 60 * 60 * 1000);
// ====== FIM SUPER-SHIM ======


// 1) REQUIRES E SETUP BÁSICO
const express = require("express");
const { createPrivateKey, createPublicKey } = require("crypto");
const app = express();

// IMPORTS DO GPTZÃO  ------------------------------
const { buildQueryFromQuestion } = require("./queryBuilder");
const { explainTinyData } = require("./ai");
const pool = require("./db");
// ------------------------------------------------


// 2) PARSERS (necessário para /oauth/token)
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

const path = require("path");

app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get(/.*/, (req, res) => {
  if (
    req.path.startsWith("/api/") ||
    req.path.startsWith("/oauth") ||
    req.path.startsWith("/.well-known")
  ) {
    return res.status(404).send("Not Found");
  }

  return res.sendFile(path.join(__dirname, "public", "index.html"));
});




// CORS básico
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(204).end();
  next();
});


// 3) VARIÁVEIS E HELPERS
const ISSUER = (process.env.ISSUER_URL || "").replace(/\/$/, "");
const KID = process.env.JWKS_KID || "kid-1";

function safeJson(res, obj, status = 200) {
  res.status(status).set("Content-Type", "application/json").send(JSON.stringify(obj));
}


// 4) ROTAS BÁSICAS
app.get("/healthz", (_, res) => res.status(200).send("ok"));


// 5) DISCOVERY OIDC
app.get("/.well-known/openid-configuration", (req, res) => {
  if (!ISSUER) {
    console.warn("[BOOT] WARN: ISSUER_URL não definido.");
  }
  safeJson(res, {
    issuer: ISSUER || "",
    authorization_endpoint: (ISSUER || "") + "/oauth/authorize",
    token_endpoint: (ISSUER || "") + "/oauth/token",
    jwks_uri: (ISSUER || "") + "/.well-known/jwks.json",
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code", "client_credentials", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["client_secret_post", "none"],
    scopes_supported: ["openid", "profile", "email"]
  });
});


// 6) JWKS
app.get("/.well-known/jwks.json", (req, res) => {
  try {
    const pem = process.env.JWT_PRIVATE_KEY_PEM;
    if (!pem) {
      console.warn("[BOOT] JWT_PRIVATE_KEY_PEM ausente.");
      return safeJson(res, { keys: [] });
    }
    const priv = createPrivateKey({ key: pem });
    const pub = createPublicKey(priv);
    const jwk = pub.export({ format: "jwk" });

    const key = {
      ...jwk,
      use: "sig",
      alg: "RS256",
      kid: KID
    };

    return safeJson(res, { keys: [key] });
  } catch (e) {
    console.error("[BOOT] erro ao montar JWKS:", e);
    return safeJson(res, { keys: [] });
  }
});


// 7) ALIASES /oauth/*
app.get("/oauth/authorize", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(302, "/authorize" + (qs ? "?" + qs : ""));
});
app.post("/oauth/token", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/token" + (qs ? "?" + qs : ""));
});
app.post("/oauth/revoke", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/revoke" + (qs ? "?" + qs : ""));
});


// 8) SUAS ROTAS REAIS
app.get("/authorize", (req, res) => {
  res.status(501).send("authorize handler not implemented");
});
app.post("/token", (req, res) => {
  res.status(501).send("token handler not implemented");
});


// ---------------------------
// 🔥 9) ROTA DO GPTZÃO  🔥
// ---------------------------
app.post("/api/chat-tiny", async (req, res) => {
  try {
    const { question } = req.body;
    const q = String(question || "").trim();
    const qlc = q.toLowerCase();

// ✅ comando: listar categorias/tags (não usa OpenAI)
const wantsCategoriesList =
  /(lista(?:r)?|listagem|quais|mostrar)\s+(categoria(?:s)?|tag(?:s)?)/.test(qlc);

if (wantsCategoriesList) {
  const sql = `
    SELECT DISTINCT tags
    FROM analytics.vw_tiny_sales_enriched
    WHERE tags IS NOT NULL
    ORDER BY tags
    LIMIT 200
  `;

  const { rows } = await pool.query(sql);

  // para a UI ficar bonita: devolve como array simples de strings
  const categories = rows.map(r => r.tags).filter(Boolean);

  return res.json({
    question: q,
    sql,
    data: categories,     // <- aparece na “Amostra dos dados”
    answer: `✅ Encontrei ${categories.length} categorias. Veja a lista na amostra ao lado.`
  });
}


// “small talk” = não roda SQL, resposta curtinha
const isSmallTalk =
  qlc.length <= 40 ||
  /^(oi|olá|ola|teste|testando|ping|funciona|você funciona|vc funciona)\b/.test(qlc);

// só roda SQL quando o usuário pedir claramente
const wantsAnalysis =
  /(analise|análise|relatório|resumo|insights|tendência|tendencias|vendas|faturamento|receita|lucro|margem|ticket|lista(?:gem)?|categoria(?:s)?|tag(?:s)?|produto|top|ranking|compar(ar|ação|acao)|vale mais a pena|melhor|pior|vs|versus)/.test(qlc);

const looksComparative =
  qlc.includes(" ou ") || qlc.includes(" vs ") || qlc.includes("versus") || qlc.includes("vale mais a pena");

const shouldUseData = !isSmallTalk && (wantsAnalysis || looksComparative || wantsCategoriesList);

// modo de resposta curta (economiza tokens)
const conciseMode = isSmallTalk || !wantsAnalysis;


    if (!question) {
      return res.status(400).json({ error: "missing_question" });
    }

    // 1) Interpretar pergunta → gerar SQL
    let sql = null;
    let params = [];
    let rows = [];

if (shouldUseData) {
  // 1) Interpretar pergunta → gerar SQL
  const built = buildQueryFromQuestion(q);
  sql = built.sql;
  params = built.params || [];

  // 2) Executar no Postgres REAL
  const result = await db.query(sql, params);
  rows = result.rows || [];
}

const LIMIT_ROWS_AI = 10;

// se for modo curto, não manda dados nenhum pro GPT
const limitedRows = conciseMode ? [] : rows.slice(0, LIMIT_ROWS_AI);

let answer;

if (conciseMode) {
  // resposta fixa para small talk / teste (não usa OpenAI)
  answer = "✅ Sim! Estou funcionando normalmente e pronto para analisar suas vendas no Tiny.";
} else {
  // só aqui chama o GPT
  answer = await explainTinyData(q, limitedRows);
}

return res.json({
  question: q,
  sql,
  data: limitedRows,
  answer
});



  } catch (err) {
    console.error("Erro no /api/chat-tiny:", err);
    return res.status(500).json({ error: "internal_error" });
  }
});
// ---------------------------
// 🔥 FIM DO GPTZÃO 🔥
// ---------------------------

// =====================================================================
//  RELATÓRIOS SEM IA (usados pela aba "Relatórios & Filtros")
// =====================================================================

function parseDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

function ensurePeriod(req, res) {
  const start = parseDateOrNull(req.query.start);
  const end = parseDateOrNull(req.query.end);

  if (!start || !end) {
    res.status(400).json({
      error: "Parâmetros inválidos. Use ?start=YYYY-MM-DD&end=YYYY-MM-DD",
    });
    return null;
  }

  return { start, end };
}

// Lista de tags distintas (para popular o select no front)
app.get("/api/tags", async (req, res) => {
  try {
    const sql = `
      SELECT DISTINCT
        UNNEST(string_to_array(tags, ','))::text AS tag
      FROM dw.dim_product
      WHERE situacao = 'A'
        AND tags IS NOT NULL
        AND tags <> ''
      ORDER BY tag;
    `;
    const result = await db.query(sql);
    const tags = result.rows
      .map((r) => (r.tag || "").trim())
      .filter((t) => t.length > 0);
    return res.json(tags);
  } catch (err) {
    console.error("Erro em /api/tags:", err);
    return res.status(500).json({ error: "Erro ao buscar tags" });
  }
});


// Resumo geral do período (faturamento, unidades, ticket, etc.)
app.get("/api/resumo-periodo", async (req, res) => {
  try {
    const period = ensurePeriod(req, res);
    if (!period) return;

    const { tag } = req.query;
    const hasTag = !!tag;

    const sql = `
      SELECT
        $1::date AS data_inicial,
        $2::date AS data_final,
        ${hasTag ? "$3::text AS tag," : "NULL::text AS tag,"}
        SUM(quantidade)  AS total_unidades,
        SUM(valor_total) AS total_faturado,
        COUNT(DISTINCT produto) AS produtos_distintos,
        COUNT(DISTINCT data)    AS dias_com_venda,
        CASE
          WHEN SUM(quantidade) > 0
          THEN SUM(valor_total) / SUM(quantidade)
          ELSE 0
        END AS ticket_medio_por_unidade
      FROM analytics.vw_tiny_sales_enriched
      WHERE data >= $1
        AND data <  $2 + INTERVAL '1 day'
        ${hasTag ? "AND tags ILIKE '%' || $3 || '%'" : ""}
    `;

    const params = hasTag
      ? [period.start, period.end, tag]
      : [period.start, period.end];

    const result = await db.query(sql, params);
    return res.json(result.rows[0] || null);
  } catch (err) {
    console.error("Erro em /api/resumo-periodo:", err);
    return res.status(500).json({ error: "internal_error" });
  }
});

// Top N produtos no período (por faturamento)
app.get("/api/top-produtos", async (req, res) => {
  try {
    const period = ensurePeriod(req, res);
    if (!period) return;

    const limit = Number(req.query.limit || 10);
    const { tag } = req.query;
    const hasTag = !!tag;

    const sql = `
      SELECT
        produto,
        ${hasTag ? "$3::text AS tag," : "NULL::text AS tag,"}
        SUM(quantidade)  AS total_unidades,
        SUM(valor_total) AS total_faturado
      FROM analytics.vw_tiny_sales_enriched
      WHERE data >= $1
        AND data <  $2 + INTERVAL '1 day'
        ${hasTag ? "AND tags ILIKE '%' || $3 || '%'" : ""}
      GROUP BY produto
      ORDER BY total_faturado DESC
      LIMIT ${limit}
    `;

    const params = hasTag
      ? [period.start, period.end, tag]
      : [period.start, period.end];

    const result = await db.query(sql, params);
    return res.json(result.rows);
  } catch (err) {
    console.error("Erro em /api/top-produtos:", err);
    return res.status(500).json({ error: "internal_error" });
  }
});

// Vendas por mês (para gráfico de coluna)
app.get("/api/vendas-por-mes", async (req, res) => {
  try {
    const period = ensurePeriod(req, res);
    if (!period) return;

    const { tag } = req.query;
    const hasTag = !!tag;

    const sql = `
      SELECT
        date_trunc('month', data)::date AS mes,
        ${hasTag ? "$3::text AS tag," : "NULL::text AS tag,"}
        SUM(quantidade)  AS total_unidades,
        SUM(valor_total) AS total_faturado
      FROM analytics.vw_tiny_sales_enriched
      WHERE data >= $1
        AND data <  $2 + INTERVAL '1 day'
        ${hasTag ? "AND tags ILIKE '%' || $3 || '%'" : ""}
      GROUP BY mes
      ORDER BY mes
    `;

    const params = hasTag
      ? [period.start, period.end, tag]
      : [period.start, period.end];

    const result = await db.query(sql, params);
    return res.json(result.rows);
  } catch (err) {
    console.error("Erro em /api/vendas-por-mes:", err);
    return res.status(500).json({ error: "internal_error" });
  }
});


// 9) START SERVER
console.log("[BOOT] prestes a chamar app.listen");
const PORT = process.env.PORT || 10000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[BOOT] servidor ouvindo na porta ${PORT}`);
});
console.log("[BOOT] app.listen chamado (retornou)");


// 10) INICIALIZAÇÕES PÓS-LISTEN
(async () => {
  try {
    if (!process.env.JWT_PRIVATE_KEY_PEM) {
      console.warn("[BOOT] WARN: JWT_PRIVATE_KEY_PEM ausente.");
    }
    console.log("[BOOT] inicializações pós-listen concluídas");
  } catch (e) {
    console.error("[BOOT] falha pós-listen:", e);
  }
})();
