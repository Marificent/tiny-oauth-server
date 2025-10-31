// test.js — chamar a API Tiny (exemplo: pesquisa de produtos)
const axios = require("axios");
require("dotenv").config();

async function main() {
  try {
    const resp = await axios.get(
      "https://api.tiny.com.br/api2/produtos.pesquisa.php",
      {
        params: {
          token: process.env.TINY_ACCESS_TOKEN,
          formato: "json",
          pesquisa: "camisa"
        },
        timeout: 20000,
      }
    );

    console.log("✅ Resposta da API Tiny:");
    console.dir(resp.data, { depth: null });
  } catch (err) {
    console.error("❌ Erro na requisição ao Tiny.");
    console.dir(
      { status: err.response?.status, data: err.response?.data || err.message },
      { depth: null }
    );
  }
}

main();
