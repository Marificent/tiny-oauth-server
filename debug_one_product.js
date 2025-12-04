require("dotenv").config();
const axios = require("axios");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;

async function main() {
  // você pode trocar esse id_tiny por outro, se quiser testar outro produto
  const idTiny = 581990104;

  console.log(`🔎 Buscando produto.obter para id_tiny = ${idTiny}...`);

  const body = new URLSearchParams({
    token: TOKEN,
    formato: "json",
    id: String(idTiny),
  });

  const { data } = await axios.post(
    `${API_V2}/produto.obter.php`,
    body.toString(),
    {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      timeout: 60000,
    }
  );

  console.log("\n📦 Resposta completa do Tiny (retorno):");
  console.dir(data, { depth: null });

  if (data?.retorno?.produto) {
    const p = data.retorno.produto;
    console.log("\n🔑 Chaves do objeto produto:");
    console.log(Object.keys(p));

    console.log("\n🧩 Campos que parecem relacionados a tags:");
    console.log("p.tags:", p.tags);
    console.log("p.tags_produto:", p.tags_produto);
    console.log("p.grupos:", p.grupos);
    console.log("p.categoria:", p.categoria);
  } else {
    console.log("\n⚠️ Não veio 'retorno.produto' na resposta.");
  }
}

main().catch((e) => {
  console.error("❌ Erro no debug_one_product:", e.message);
  process.exit(1);
});
