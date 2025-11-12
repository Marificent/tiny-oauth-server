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

// Mantém o event loop vivo mesmo se nada mais prender o processo (diagnóstico)
setInterval(() => {}, 60 * 60 * 1000);
// ====== FIM SUPER-SHIM ======

// 1) REQUIRES E SETUP BÁSICO
const express = require("express");
const { createPrivateKey, createPublicKey } = require("crypto");
const app = express();

// 2) PARSERS (necessário para /oauth/token)
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// CORS básico (permitir chamadas do portal)
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*"); // depois podemos restringir ao domínio do portal
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(204).end();
  next();
});

// 3) VARIÁVEIS E HELPERS
const ISSUER = (process.env.ISSUER_URL || "").replace(/\/$/, "");
const KID = process.env.JWKS_KID || "kid-1";

// base64url helper (Node já entrega n/e em formato JWK quando exportamos como 'jwk')
function safeJson(res, obj, status = 200) {
  res.status(status).set("Content-Type", "application/json").send(JSON.stringify(obj));
}

// 4) ROTAS BÁSICAS SEM RISCO
app.get("/healthz", (_, res) => res.status(200).send("ok"));

// 5) DISCOVERY OIDC
app.get("/.well-known/openid-configuration", (req, res) => {
  if (!ISSUER) {
    console.warn("[BOOT] WARN: ISSUER_URL não definido — defina no Render para discovery correto.");
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
    scopes_supported: ["openid", "profile", "email"] // ajuste conforme seus escopos
  });
});

// 6) JWKS (gera a JWK pública a partir da PRIVATE KEY no Render)
app.get("/.well-known/jwks.json", (req, res) => {
  try {
    const pem = process.env.JWT_PRIVATE_KEY_PEM;
    if (!pem) {
      console.warn("[BOOT] WARN: JWT_PRIVATE_KEY_PEM ausente — retornando JWKS vazio.");
      return safeJson(res, { keys: [] });
    }
    // Cria chave pública a partir da privada e exporta como JWK (Node 16+)
    const priv = createPrivateKey({ key: pem });
    const pub = createPublicKey(priv);
    const jwk = pub.export({ format: "jwk" }); // { kty, n, e } para RSA
    const key = {
      ...jwk,             // kty, n, e
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

// 7) ALIASES /oauth/*  → redirecionam para suas rotas reais
//    Se suas rotas reais NÃO forem /authorize e /token, ajuste os destinos aqui!
app.get("/oauth/authorize", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(302, "/authorize" + (qs ? "?" + qs : ""));
});

// Redireciona com 307 (mantém método e body). No curl, use -L para seguir.
app.post("/oauth/token", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/token" + (qs ? "?" + qs : ""));
});

// (Opcional) Revogação
app.post("/oauth/revoke", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/revoke" + (qs ? "?" + qs : ""));
});

// 8) SUAS ROTAS REAIS (ex.: /authorize, /token, /userinfo, etc.)
//    Mantenha o que você já tinha aqui. Exemplo de placeholders:
app.get("/authorize", (req, res, next) => {
  // TODO: sua lógica real de authorize (login/consent + emitir code)
  // Se ainda não implementado, devolve 501 para ficar claro:
  res.status(501).send("authorize handler not implemented");
});

app.post("/token", (req, res, next) => {
  // TODO: sua lógica real de token (authorization_code, client_credentials, refresh_token)
  res.status(501).send("token handler not implemented");
});

// 9) START — o listen fica ANTES de inicializações frágeis
console.log("[BOOT] prestes a chamar app.listen");
const PORT = process.env.PORT || 10000;

// TEMP: ver se a env está chegando
app.get("/debug/env", (req, res) => {
  const s = process.env.JWT_PRIVATE_KEY_PEM || "";
  res.json({
    hasKey: !!s,
    length: s.length,
    beginsWith: s.slice(0, 30)
  });
});

// TEMP: debug do JWKS (mostra erro se falhar)
app.get("/debug/jwks", (req, res) => {
  try {
    const pem = process.env.JWT_PRIVATE_KEY_PEM;
    if (!pem) throw new Error("JWT_PRIVATE_KEY_PEM ausente");
    const { createPrivateKey, createPublicKey } = require("crypto");
    const priv = createPrivateKey({ key: pem });
    const pub = createPublicKey(priv);
    const jwk = pub.export({ format: "jwk" });
    res.json({ ok: true, jwkKeys: Object.keys(jwk) });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// 🔐 Proxy Tiny seguro (whitelist de endpoints) — Tiny API clássica usa /api2/*.php
const ALLOW_TINY = new Set([
  "pedidos.pesquisa.php",
  "clientes.pesquisa.php",
  "produtos.pesquisa.php",
  // adicione mais aqui conforme for usando
]);

app.get("/api/tiny/:endpoint", async (req, res) => {
  try {
    const base = (process.env.TINY_API_BASE || "").replace(/\/$/, "");
    const token = process.env.TINY_API_TOKEN || "";
    if (!base || !token) {
      return res.status(500).json({ error: "tiny_not_configured" });
    }

    const endpoint = String(req.params.endpoint || "");
    if (!ALLOW_TINY.has(endpoint)) {
      return res.status(400).json({ error: "endpoint_not_allowed", endpoint });
    }

    // monta query: repassa tudo que veio do cliente + injeta token e formato=json
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(req.query || {})) {
      if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
    }
    // Tiny clássico: token e formato (ajuste se seu Tiny usar outro estilo)
    if (!params.has("token")) params.set("token", token);
    if (!params.has("formato")) params.set("formato", "json");

    const url = `${base}/api2/${endpoint}?${params.toString()}`;

    // chamada server→server
    const r = await fetch(url, {
      method: "GET",
      headers: { "Accept": "application/json" }
    });

    const txt = await r.text();
    let out; try { out = JSON.parse(txt); } catch { out = { raw: txt }; }

    return res.status(r.status).json(out);
  } catch (e) {
    console.error("[tiny-proxy] erro:", e);
    return res.status(500).json({ error: "proxy_error", detail: String(e) });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[BOOT] servidor ouvindo na porta ${PORT}`);
});
console.log("[BOOT] app.listen chamado (retornou)");

// 10) INICIALIZAÇÕES PÓS-LISTEN (NUNCA dar process.exit aqui)
(async () => {
  try {
    if (!process.env.JWT_PRIVATE_KEY_PEM) {
      console.warn("[BOOT] WARN: JWT_PRIVATE_KEY_PEM ausente — tokens podem não assinar; prossigo.");
    }
    // Ex.: conexão DB, leitura de arquivos, aquecimento de caches/integrações:
    // await connectDB();
    // await warmUpTiny();
    console.log("[BOOT] inicializações pós-listen concluídas");
  } catch (e) {
    console.error("[BOOT] falha em inicializações pós-listen:", e);
    // importante: não derrubar o processo
  }
})();
