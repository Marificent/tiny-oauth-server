// test_v2_produtos.js
require("dotenv").config();
const axios = require("axios");

async function main() {
  try {
    const body = new URLSearchParams({
      token: process.env.TINY_API2_TOKEN,
      formato: "json",
      // "pesquisa" é OBRIGATÓRIO na doc de produtos (pode ser um trecho do nome/código)
      pesquisa: "a" // usei "a" só para listar algo; pode trocar por "camisa", etc.
    });

    const { data } = await axios.post(
      "https://api.tiny.com.br/api2/produtos.pesquisa.php",
      body.toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 20000 }
    );

    console.log("✅ Retorno:");
    console.dir(data, { depth: null });
  } catch (err) {
    console.error("❌ Erro:");
    console.dir({ status: err.response?.status, data: err.response?.data || err.message }, { depth: null });
  }
}

main();
