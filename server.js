// server.js
const express = require("express");
const axios = require("axios");
const qs = require("qs");
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
require("dotenv").config();

// Inicializa app
const app = express();

// ativa o cookie-parser logo depois de criar o app
app.use(cookieParser(process.env.COOKIE_SECRET || "dev-secret"));



// Configurações básicas
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;

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

  // Segue pro restante do fluxo normal (POST pro TOKEN_URL)
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

    // tokens obtidos — por enquanto apenas guardados em memória local
    // (depois vamos criptografar e salvar em BD)
    console.log("Tokens recebidos com sucesso (não enviados ao cliente).");

    return res.redirect("/auth/success");
  } catch (err) {
    console.error("Falha na troca de token:", err.response?.data || err.message);
    return res.redirect("/auth/error?reason=token_exchange_failed");
  }
});

app.get("/auth/success", (req, res) => {
  res.send("<h1>Conexão concluída com sucesso.</h1><p>Você já pode fechar esta janela.</p>");
});

app.get("/auth/error", (req, res) => {
  res.status(400).send("<h1>Ocorreu um problema.</h1><p>Tente novamente.</p>");
});


app.listen(PORT, () => {
  console.log(`Servidor no ar: porta ${PORT}`);
});

