// server.js

// --- DIAGNÓSTICO DE SAÍDA PRECOCE ---
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

console.log("[BOOT] entrando em server.js");

process.on("uncaughtException", (err) => {
  console.error("[BOOT] uncaughtException:", err);
});
process.on("unhandledRejection", (err) => {
  console.error("[BOOT] unhandledRejection:", err);
});

const express = require("express");
const axios = require("axios");
const qs = require("qs");
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
require("dotenv").config();

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Evita erro de variável indefinida em produção
const data = { notas: [] };

const fs = require("fs");

// ativa o cookie-parser logo depois de criar o app
app.use(cookieParser(process.env.COOKIE_SECRET || "dev-secret"));

app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// permite que o servidor receba JSON no body das requisições
app.use(express.json());

// Configurações básicas
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;

// Base pública do app (localhost ou túnel). Usada como fallback nos callbacks.
const APP_BASE_URL = process.env.APP_BASE_URL || `http://localhost:${PORT}`;

// Healthcheck para monitoramento e plataformas de deploy
app.get('/healthz', (req, res) => {
  res.status(200).send('ok');
});

const {
  TINY_CLIENT_ID,
  TINY_CLIENT_SECRET,
  TINY_REDIRECT_URI,
  AUTH_URL,
  TOKEN_URL,
} = process.env;

// ------------------------------
// TINY TOKENS: salvar e renovar
// ------------------------------
let tinyTokens = null; // { access_token, refresh_token, expires_at }

function setTinyTokens(tok) {
  const expiresInMs = (tok.expires_in ? Number(tok.expires_in) : 3600) * 1000;
  tinyTokens = {
    access_token: tok.access_token,
    refresh_token: tok.refresh_token,
    expires_at: Date.now() + expiresInMs
  };
}

async function getTinyAccessToken() {
  if (!tinyTokens || !tinyTokens.access_token) {
    throw new Error("Tiny não autenticado ainda.");
  }
  // renova 1 min antes de expirar
  if (tinyTokens.refresh_token && Date.now() > (tinyTokens.expires_at - 60_000)) {
    const body = qs.stringify({
      grant_type: "refresh_token",
      refresh_token: tinyTokens.refresh_token,
      client_id: process.env.TINY_CLIENT_ID,
      client_secret: process.env.TINY_CLIENT_SECRET
    });
    const { data } = await axios.post(process.env.TOKEN_URL, body, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      timeout: 20000,
    });
    setTinyTokens(data);
  }
  return tinyTokens.access_token;
}

// ------------------------------
// Utils de datas (mês → [início, fim])
// ------------------------------
function monthRange(year, month) {
  const y = Number(year);
  const m = Number(month);
  if (!y || !m || m < 1 || m > 12) {
    const now = new Date();
    const yNow = now.getFullYear();
    const mNow = now.getMonth() + 1;
    const start = new Date(yNow, mNow - 1, 1);
    const end = new Date(yNow, mNow, 0);
    return {
      start: start.toISOString().slice(0, 10),
      end: end.toISOString().slice(0, 10),
      year: yNow,
      month: mNow,
    };
  }
  const start = new Date(y, m - 1, 1);
  const end = new Date(y, m, 0);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
    year: y,
    month: m,
  };
}

// ------------------------------
// Busca notas no Tiny e soma o faturamento (versão POST + parâmetros alternativos)
// ------------------------------
async function fetchTinyInvoicesTotal(fromDate, toDate) {
  if (!process.env.TINY_INVOICES_SEARCH_URL) {
    throw new Error("Defina TINY_INVOICES_SEARCH_URL no .env (endpoint de busca de NF-e no Tiny).");
  }

  const resp = await axios.post(
    process.env.TINY_INVOICES_SEARCH_URL,
    body,
    {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      timeout: 30000
    }
  );

  return { total, count: list.length, raw: data };
}

  // ⚠️ Ajuste o caminho/nomes conforme o seu retorno.
  // Abaixo eu tento cobrir dois formatos típicos: { notas: [...] } ou { data: { items: [...] } }
  const list =
  Array.isArray(data.notas) ? data.notas
  : Array.isArray(data.data?.items) ? data.data.items
  : [];

  // Ajuste os campos: tente "valorTotal", senão "amount", senão 0
  const total = list.reduce((acc, n) => {
    const v =
      (typeof n.valorTotal === 'number' && n.valorTotal) ||
      (typeof n.amount === 'number' && n.amount) ||
      (typeof n.total === 'number' && n.total) ||
      0;
    return acc + Number(v);
  }, 0);

  return { total, count: list.length, raw: data };

// ------------------------------
// MERCADO LIVRE (OAuth2)
// ------------------------------
const MELI_CLIENT_ID = process.env.MELI_CLIENT_ID || '';
const MELI_CLIENT_SECRET = process.env.MELI_CLIENT_SECRET || '';
const MELI_AUTH_URL = process.env.MELI_AUTH_URL || 'https://auth.mercadolibre.com.br/authorization';
const MELI_TOKEN_URL = process.env.MELI_TOKEN_URL || 'https://api.mercadolibre.com/oauth/token';
const MELI_REDIRECT_URI = process.env.MELI_REDIRECT_URI || `${APP_BASE_URL}/oauth/meli/callback`;

// DEBUG: ver o que o servidor está usando do .env (NÃO deixe isso em produção)
if (process.env.NODE_ENV !== 'production') {
  app.get("/debug/env", (req, res) => {
    res.json({
      TINY_CLIENT_ID: process.env.TINY_CLIENT_ID,
      TINY_REDIRECT_URI: process.env.TINY_REDIRECT_URI,
      AUTH_URL: process.env.AUTH_URL,
      TOKEN_URL: process.env.TOKEN_URL,
      scope_usado: "openid email offline_access"
    });
  });
}

// Rota que mostra a URL exata de login (sem PKCE)
app.get("/auth/url", (req, res) => {
  const state = crypto.randomBytes(16).toString("hex");

  // só guardamos o state
  res.cookie("oauth_state", state, {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    signed: true,
    maxAge: 10 * 60 * 1000
  });

  const params = new URLSearchParams({
    client_id: process.env.TINY_CLIENT_ID,
    redirect_uri: process.env.TINY_REDIRECT_URI,
    response_type: "code",
    scope: "openid email offline_access",
    state
  });

  const authLink = `${process.env.AUTH_URL}?${params.toString()}`;
  res.type("html").send(`<p>Copie e cole no navegador:</p><code>${authLink}</code>`);
});

// Redireciona direto para o login do Tiny (sem PKCE)
app.get("/auth/login", (req, res) => {
  const state = crypto.randomBytes(16).toString("hex");

  res.cookie("oauth_state", state, {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    signed: true,
    maxAge: 10 * 60 * 1000
  });

  const params = new URLSearchParams({
    client_id: process.env.TINY_CLIENT_ID,
    redirect_uri: process.env.TINY_REDIRECT_URI,
    response_type: "code",
    scope: "openid email offline_access",
    state
  });

  res.redirect(`${process.env.AUTH_URL}?${params.toString()}`);
});

// ⬇️⬇️⬇️  IMPORTANTE: ROTAS DO MELI DEVEM FICAR NO NÍVEL SUPERIOR (fora de outras rotas)  ⬇️⬇️⬇️

// Inicia o login no Mercado Livre
app.get('/oauth/meli/start', (req, res) => {
  if (!MELI_CLIENT_ID || !MELI_CLIENT_SECRET) {
    return res
      .status(400)
      .send('MELI OAuth não configurado: defina MELI_CLIENT_ID e MELI_CLIENT_SECRET no .env.');
  }
  const params = new URLSearchParams({
    response_type: 'code',
    client_id: MELI_CLIENT_ID,
    redirect_uri: MELI_REDIRECT_URI,
  });
  return res.redirect(`${MELI_AUTH_URL}?${params.toString()}`);
});

// Callback do Mercado Livre
app.get('/oauth/meli/callback', async (req, res) => {
  try {
    const { code, error, error_description } = req.query;
    if (error) {
      return res.status(400).send(`Erro do MELI: ${error} - ${error_description || ''}`);
    }
    if (!code) {
      return res.status(400).send('Código "code" ausente no callback do MELI.');
    }

    // Troca o "code" por tokens
    const body = new URLSearchParams({
      grant_type: 'authorization_code',
      client_id: MELI_CLIENT_ID,
      client_secret: MELI_CLIENT_SECRET,
      code: String(code),
      redirect_uri: MELI_REDIRECT_URI,
    });

    const tokenResp = await axios.post(MELI_TOKEN_URL, body.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    });

    const tokens = tokenResp.data;

    // TODO: persistir tokens no seu storage (DB). Exemplo:
    // await saveOauthTokens({ provider: 'meli', ...tokens })

    return res.send(`<h3>MELI conectado!</h3><pre>${JSON.stringify(tokens, null, 2)}</pre>`);
  } catch (e) {
    console.error('Erro no callback do MELI:', e?.response?.data || e);
    return res.status(500).send('Erro interno ao processar callback do MELI.');
  }
});

// ⬆️⬆️⬆️  FIM DAS ROTAS DO MELI NO NÍVEL SUPERIOR  ⬆️⬆️⬆️

// Callback do Tiny
app.get("/auth/callback", async (req, res) => {
  const { code, state } = req.query;

  // Checa se veio o código de autorização
  if (!code) {
    return res.redirect("/auth/error?reason=missing_code");
  }

  // Valida o state com o cookie assinado
  const savedState = req.signedCookies?.oauth_state;
  if (!state || !savedState || state !== savedState) {
    return res.redirect("/auth/error?reason=state_mismatch");
  }

  // Apaga o cookie (uso único)
  res.clearCookie("oauth_state");

  // Troca code por token no Tiny
  try {
    const body = qs.stringify({
      grant_type: "authorization_code",
      code,
      redirect_uri: process.env.TINY_REDIRECT_URI,
      client_id: process.env.TINY_CLIENT_ID,
      client_secret: process.env.TINY_CLIENT_SECRET
    });

    const { data } = await axios.post(process.env.TOKEN_URL, body, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      timeout: 20000,
    });

    setTinyTokens(data);
    console.log("Tokens Tiny recebidos e armazenados em memória.");
    return res.redirect("/auth/success");
    } catch (err) {
      console.error("Falha na troca de token (Tiny):", err.response?.data || err.message);
      return res.redirect("/auth/error?reason=token_exchange_failed");
    }
  }
);

app.get("/auth/success", (req, res) => {
  res.send("<h1>Conexão concluída com sucesso.</h1><p>Você já pode fechar esta janela.</p>");
});

app.get("/auth/error", (req, res) => {
  res.status(400).send("<h1>Ocorreu um problema.</h1><p>Tente novamente.</p>");
});

// Healthcheck simples
app.get('/health', (req, res) => {
  res.json({ ok: true, up: true, port: PORT });
});

// Emulação temporária do endpoint de contadores
app.all('/meli/anuncios/contador', (req, res) => {
  return res.json({
    ok: true,
    data: {
      vendas: 0,
      faturamento: 0,
      envios: 0,
      vendas_full: 0,
      vendas_perdidas_ruptura: 0,
      anuncios_sem_venda: 0,
    },
  });
});

// ------------------------------
// TESTE RÁPIDO: /tiny/ping
// Verifica o token chamando o endpoint OIDC userinfo
// ------------------------------
app.get('/tiny/ping', async (req, res) => {
  try {
    const accessToken = await getTinyAccessToken();

    // Deriva a URL de userinfo a partir do AUTH_URL (que termina com /auth)
    // Ex.: https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/userinfo
    const authUrl = process.env.AUTH_URL;
    const userinfoUrl = authUrl.replace(/auth$/, 'userinfo');

    const { data } = await axios.get(userinfoUrl, {
      headers: { Authorization: `Bearer ${accessToken}` },
      timeout: 15000,
    });

    return res.json({ ok: true, userinfo: data });
  } catch (e) {
    const msg = e?.response?.data || e.message;
    console.error('tiny/ping error:', msg);
    return res.status(500).json({ ok: false, error: msg });
  }
});

// ------------------------------
// GET /tiny/invoices/total?year=YYYY&month=MM
// Retorna o total faturado no mês solicitado
// ------------------------------
app.get('/tiny/invoices/total', async (req, res) => {
  try {
    const { year, month } = req.query;
    const { start, end, year: y, month: m } = monthRange(year, month);
    const { total, count } = await fetchTinyInvoicesTotal(start, end);
    return res.json({
      ok: true,
      period: { year: y, month: m, start, end },
      invoices_count: count,
      total_faturado: Number(total),
    });
  } catch (e) {
    const msg = e?.response?.data || e.message;
    console.error('invoices/total error:', msg);
    return res.status(500).json({ ok: false, error: msg });
  }
});

// rotas de OAuth que faltavam
app.get('/oauth/authorize', (req, res) => {
  const qs = req.url.split('?')[1] || '';
  res.redirect(302, `/authorize${qs ? '?' + qs : ''}`);
});

app.post("/oauth/token", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/token" + (qs ? "?" + qs : ""));
});

app.post("/oauth/revoke", (req, res) => {
  const qs = req.originalUrl.split("?")[1];
  res.redirect(307, "/revoke" + (qs ? "?" + qs : ""));
});

app.get("/.well-known/openid-configuration", (req, res) => {
  const iss = (process.env.ISSUER_URL || "").replace(/\/$/, "");
  res.json({
    issuer: iss,
    authorization_endpoint: iss + "/oauth/authorize",
    token_endpoint: iss + "/oauth/token",
    jwks_uri: iss + "/.well-known/jwks.json",
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code", "client_credentials", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["client_secret_post", "none"],
    scopes_supported: ["openid", "profile", "email"],
  });
});

app.listen(PORT, () => {
  console.log(`Servidor no ar: porta ${PORT}`);
});

// 7) ⚙️ INICIALIZAÇÕES MAIS FRÁGEIS (DB, arquivos, jobs) — SEM process.exit()
// (se algo falhar, apenas logue o erro; não derrube o processo)
(async () => {
  try {
    // 1) ENV OBRIGATÓRIAS — não derrubar o processo, apenas avisar
    if (!process.env.JWT_PRIVATE_KEY_PEM) {
      console.warn("[BOOT] WARN: JWT_PRIVATE_KEY_PEM ausente — JWKS/assinatura podem falhar; prosseguindo sem derrubar.");
      // opcional: habilitar modo “dev” sem assinatura, ou montar um JWKS vazio
    }

    // 2) DB — conectar de forma resiliente (ex.: com timeout e retry simples)
    // await connectDB(); // se falhar:
    // console.error("[BOOT] falha ao conectar no DB:", e);

    // 3) Leitura de arquivos / chaves
    // let raw = "";
    // try { raw = fs.readFileSync("./config.json","utf8"); } catch (e) { console.warn("[BOOT] config.json não encontrado; usando defaults"); }
    // let cfg = {};
    // try { cfg = raw ? JSON.parse(raw) : {}; } catch (e) { console.warn("[BOOT] config.json inválido; usando defaults"); }

    // 4) Chamada a Tiny/Meli (se precisar token/cache no boot)
    // try { await warmUpTiny(); } catch (e) { console.warn("[BOOT] Tiny warmup falhou; continua"); }

    console.log("[BOOT] inicializações pós-listen concluídas");
  } catch (e) {
    console.error("[BOOT] falha em inicializações pós-listen:", e);
    // IMPORTANTE: não fazer process.exit aqui
  }
})();
