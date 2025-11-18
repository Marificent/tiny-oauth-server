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
const db = require("./db");
// ------------------------------------------------


// 2) PARSERS (necessário para /oauth/token)
app.use(express.json());
app.use(express.urlencoded({ extended: false }));


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

    if (!question) {
      return res.status(400).json({ error: "missing_question" });
    }

    // 1) Interpretar pergunta → gerar SQL
    const { sql, params } = buildQueryFromQuestion(question);

    // 2) Executar no Postgres REAL
    const result = await db.query(sql, params);
    const rows = result.rows || [];

    // 3) GPT explica os dados
    const answer = await explainTinyData(question, rows);

    return res.json({
      question,
      sql,
      data: rows.slice(0, 300),
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
