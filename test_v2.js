// test_v2.js
require("dotenv").config();
const axios = require("axios");

async function testarApiV2() {
  try {
    const { TINY_API2_TOKEN } = process.env;

    const { data } = await axios.get(
      "https://api.tiny.com.br/api2/produtos.pesquisar.php",
      {
        params: {
          token: TINY_API2_TOKEN,
          formato: "json",
          pesquisa: "", // vazio lista todos
          pagina: 1,
        },
      }
    );

    console.log("✅ Resposta API v2:", JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("❌ Erro ao chamar API v2:", err.response?.data || err.message);
  }
}

testarApiV2();
