// introspect.js — verifica se o access_token OAuth do Tiny está ativo
const axios = require("axios");
const qs = require("qs");
require("dotenv").config();

(async () => {
  try {
    const { TOKEN_URL, TINY_CLIENT_ID, TINY_CLIENT_SECRET, TINY_ACCESS_TOKEN } = process.env;
    if (!TOKEN_URL || !TINY_CLIENT_ID || !TINY_CLIENT_SECRET || !TINY_ACCESS_TOKEN) {
      throw new Error("Faltam variáveis no .env (TOKEN_URL, TINY_CLIENT_ID, TINY_CLIENT_SECRET, TINY_ACCESS_TOKEN).");
    }

    // endpoint de introspecção (Keycloak)
    const introspectUrl = TOKEN_URL.replace("/token", "/token/introspect");

    const body = qs.stringify({
      token: TINY_ACCESS_TOKEN,
      token_type_hint: "access_token",
    });

    const basic = Buffer.from(`${TINY_CLIENT_ID}:${TINY_CLIENT_SECRET}`).toString("base64");

    const { data } = await axios.post(introspectUrl, body, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: `Basic ${basic}`,
      },
      timeout: 20000,
    });

    console.log("🔎 Introspecção do token:");
    console.dir(data, { depth: null });
  } catch (err) {
    console.error("❌ Falha na introspecção.");
    console.dir({ status: err.response?.status, data: err.response?.data || err.message }, { depth: null });
  }
})();
