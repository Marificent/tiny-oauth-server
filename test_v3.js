// test_v3.js — chamada à API V3 do Tiny com headers que evitam bloqueio do WAF/Cloudflare
const axios = require("axios");
require("dotenv").config();

const client = axios.create({
  headers: {
    // Simula um browser real para não cair no desafio da Cloudflare
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
  },
  timeout: 20000,
});

async function call(url) {
  try {
    const r = await client.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.TINY_ACCESS_TOKEN}`,
      },
    });
    console.log("✅", r.status, url);
    console.dir(r.data, { depth: null });
    return true;
  } catch (err) {
    const s = err.response?.status;
    const d = err.response?.data || err.message;
    console.log("↩️", s, url);
    console.dir(d, { depth: null });
    return false;
  }
}

// tente alguns endpoints possíveis da V3
(async () => {
  const urls = [
    "https://api.tiny.com.br/api/v3/me",
    "https://api.tiny.com.br/api/v3/orders?limit=1",
    "https://api.tiny.com.br/api/v3/pedidos?limit=1",
    "https://api.tiny.com.br/api/v3/products?limit=1",
    "https://api.tiny.com.br/api/v3/produtos?limit=1",
  ];
  for (const u of urls) {
    const ok = await call(u);
    if (ok) break;
  }
})();
