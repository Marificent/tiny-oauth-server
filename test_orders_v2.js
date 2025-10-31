// test_orders_v2.js
require("dotenv").config();
const axios = require("axios");

const TINY_API_TOKEN = process.env.TINY_API_TOKEN;

async function main() {
  try {
    const hoje = new Date();
    const inicio = new Date();
    inicio.setDate(hoje.getDate() - 5); // últimos 5 dias

    function fmt(d) {
      const dd = String(d.getDate()).padStart(2, "0");
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const yyyy = d.getFullYear();
      return `${dd}/${mm}/${yyyy}`;
    }

    const dataInicial = fmt(inicio);
    const dataFinal = fmt(hoje);

    console.log(`🔎 Testando pedidos de ${dataInicial} a ${dataFinal} ...`);

    const { data } = await axios.get("https://api.tiny.com.br/api2/pedidos.pesquisa.php", {
      params: {
        token: TINY_API_TOKEN,
        dataInicial,
        dataFinal,
        formato: "json",
        pagina: 1,
      },
      timeout: 20000,
    });

    console.log("✅ Resposta:", JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("❌ Erro:", err.response?.data || err.message);
  }
}

main();
