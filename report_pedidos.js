// report_pedidos.js — Fase 2: puxar PEDIDOS (últimos 30 dias) + ITENS na API v2
require("dotenv").config();
const axios = require("axios");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;

// Util: POST x-www-form-urlencoded
async function tinyPost(endpoint, formObj) {
  const body = new URLSearchParams({ token: TOKEN, formato: "json", ...formObj });
  const { data } = await axios.post(
    `${API_V2}/${endpoint}.php`,
    body.toString(),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 30000 }
  );
  return data;
}

// Pesquisa paginada de pedidos por data (últimos 30 dias)
async function fetchPedidosPagina(pagina, dataInicial, dataFinal) {
  const data = await tinyPost("pedidos.pesquisa", {
    dataInicial, // DD/MM/AAAA
    dataFinal,   // DD/MM/AAAA
    pagina: String(pagina),
    pesquisar: "S", // ativa o filtro de datas
  });

  if (data?.retorno?.status !== "OK") {
    const msg = JSON.stringify(data?.retorno?.erros || data, null, 2);
    throw new Error(`Erro na pesquisa de pedidos pág. ${pagina}: ${msg}`);
  }

  const pedidos = (data.retorno.pedidos || []).map(p => p.pedido);
  const numero_paginas = Number(data.retorno.numero_paginas || 1);
  return { pedidos, numero_paginas };
}

// Obter detalhes (itens) de um pedido — aceita id (preferível) ou numero
async function fetchPedidoDetalhe({ id, numero }) {
  // Prioriza ID; se não houver, tenta por número
  let data;
  if (id) {
    data = await tinyPost("pedido.obter", { id: String(id) });
  } else if (numero) {
    data = await tinyPost("pedido.obter", { numero: String(numero) });
  } else {
    throw new Error("Pedido sem id/numero para obter detalhes.");
  }

  if (data?.retorno?.status !== "OK") {
    const msg = JSON.stringify(data?.retorno?.erros || data, null, 2);
    throw new Error(`Erro ao obter pedido ${id || numero}: ${msg}`);
  }

  // Estrutura vem como { retorno: { pedido: { itens: [ { item: {...} } ] } } }
  const pedido = data.retorno.pedido || {};
  const itens = (pedido.itens || []).map(i => i.item);
  return { pedido, itens };
}

function fmtBR(d) {
  // formata DD/MM/AAAA em America/Sao_Paulo
  return d.toLocaleDateString("pt-BR", { timeZone: "America/Sao_Paulo" });
}

async function main() {
  try {
    if (!TOKEN) throw new Error("Falta TINY_API2_TOKEN no .env");

    const hoje = new Date();
    const d30 = new Date();
    d30.setDate(hoje.getDate() - 30);
    const dataInicial = fmtBR(d30);
    const dataFinal   = fmtBR(hoje);

    console.log(`🧾 Buscando pedidos de ${dataInicial} até ${dataFinal} ...`);

    // Paginação de pedidos
    let pagina = 1;
    let totalPaginas = 1;
    const listaPedidos = [];

    do {
      const { pedidos, numero_paginas } = await fetchPedidosPagina(pagina, dataInicial, dataFinal);
      totalPaginas = numero_paginas;
      listaPedidos.push(...pedidos);
      console.log(`✅ Página ${pagina}/${totalPaginas} — acumulado: ${listaPedidos.length} pedidos`);
      pagina++;
    } while (pagina <= totalPaginas);

    if (listaPedidos.length === 0) {
      console.log("⚠️ Nenhum pedido no período.");
      return;
    }

    // Para cada pedido, obter itens
    console.log("📦 Buscando itens de cada pedido (detalhe)...");
    const todosItens = [];
    for (const p of listaPedidos) {
      try {
        const { itens } = await fetchPedidoDetalhe({ id: p.id, numero: p.numero });
        // acrescenta metadados úteis no item
        itens.forEach(it => {
          todosItens.push({
            numeroPedido: p.numero,
            idPedido: p.id,
            data: p.data_pedido || p.data_criacao || p.dataEmissao,
            situacao: p.situacao,
            codigo: it.codigo,
            descricao: it.descricao,
            quantidade: Number(it.quantidade || 0),
            valorUnit: Number(it.valor_unitario || it.valor || 0),
            valorTotal: Number(it.valor_total || 0),
          });
        });
      } catch (e) {
        console.warn(`⚠️ Falha nos itens do pedido ${p.id || p.numero}: ${e.message}`);
      }
    }

    // Resumo
    const totalItens = todosItens.length;
    const valorBruto = todosItens.reduce((acc, it) => acc + (Number(it.valorTotal) || 0), 0);

    console.log("\n📊 Resumo do período");
    console.log(`Pedidos coletados: ${listaPedidos.length}`);
    console.log(`Itens coletados:   ${totalItens}`);
    console.log(`Faturamento bruto (somatório dos itens): R$ ${valorBruto.toFixed(2)}`);

    console.log("\n🧪 Amostra de itens:");
    console.table(todosItens.slice(0, 10));

    // Se quiser salvar para próxima etapa (insights), descomente:
    // const fs = require("fs");
    // fs.writeFileSync("pedidos_itens.json", JSON.stringify(todosItens, null, 2));
  } catch (err) {
    console.error("❌ Falha geral:");
    console.dir(err.message || err, { depth: null });
  }
}

main();
