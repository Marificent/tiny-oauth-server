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
