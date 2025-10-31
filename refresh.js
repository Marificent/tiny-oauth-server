// refresh.js — renova o access_token usando refresh_token (formato Tiny/Keycloak)
const axios = require("axios");
const qs = require("qs");
require("dotenv").config();

(async () => {
  try {
    const {
      TOKEN_URL,
      TINY_CLIENT_ID,
      TINY_CLIENT_SECRET,
      TINY_REFRESH_TOKEN,
    } = process.env;

    if (!TOKEN_URL || !TINY_CLIENT_ID || !TINY_CLIENT_SECRET || !TINY_REFRESH_TOKEN) {
      throw new Error("Faltam variáveis no .env (TOKEN_URL, TINY_CLIENT_ID, TINY_CLIENT_SECRET, TINY_REFRESH_TOKEN).");
    }

    // Header Authorization: Basic base64(client_id:client_secret)
    const basic = Buffer.from(`${TINY_CLIENT_ID}:${TINY_CLIENT_SECRET}`).toString("base64");

    // Body x-www-form-urlencoded
    const body = qs.stringify({
      grant_type: "refresh_token",
      refresh_token: TINY_REFRESH_TOKEN,
    });

    const { data } = await axios.post(TOKEN_URL, body, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Basic ${basic}`,
      },
      timeout: 20000,
    });

    console.log("✅ Tokens renovados:");
    console.log("access_token:", data.access_token?.slice(0, 25) + "...");
    console.log("refresh_token:", data.refresh_token?.slice(0, 25) + "...");

    console.log("\n👉 Copie esses dois valores e atualize no seu .env nas chaves:");
    console.log("TINY_ACCESS_TOKEN=...");
    console.log("TINY_REFRESH_TOKEN=...");

  } catch (err) {
    console.error("❌ Falha ao renovar token.");
    console.dir(err.response?.data || err.message, { depth: null });
  }
})();
