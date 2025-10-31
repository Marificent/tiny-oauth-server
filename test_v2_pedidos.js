// test_v2_pedidos.js — lista pedidos do Tiny v2 (últimos 30 dias) via POST x-www-form-urlencoded
require("dotenv").config();
const axios = require("axios");

async function main() {
  try {
    const hoje = new Date();
    const d30 = new Date();
    d30.setDate(hoje.getDate() - 30);

    // formata DD/MM/AAAA
    const fmt = d => d.toLocaleDateString("pt-BR", { timeZone: "America/Sao_Paulo" });

    const body = new URLSearchParams({
      token: process.env.TINY_API2_TOKEN,
      formato: "json",
      dataInicial: fmt(d30),
      dataFinal: fmt(hoje),
      pagina: "1",
      pesquisar: "S",        // ativa o filtro por data
      situacao: "",          // deixe vazio para trazer todas as situações
      idVendedor: "",        // ajuste se quiser filtrar por vendedor
    });

    const { data } = await axios.post(
      "https://api.tiny.com.br/api2/pedidos.pesquisa.php",
      body.toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 20000 }
    );

    console.log("✅ Retorno pedidos (últimos 30 dias):");
    console.dir(data, { depth: null });
  } catch (err) {
    console.error("❌ Erro pedidos:");
    console.dir({ status: err.response?.status, data: err.response?.data || err.message }, { depth: null });
  }
}

main();
