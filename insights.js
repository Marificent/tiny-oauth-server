// insights.js — Tiny API v2 -> Produtos + Pedidos/Itens -> Insights + CSVs
require("dotenv").config();
const axios = require("axios");
const fs = require("fs");

const API_V2 = "https://api.tiny.com.br/api2";
const TOKEN = process.env.TINY_API2_TOKEN;
if (!TOKEN) {
  console.error("Falta TINY_API2_TOKEN no .env");
  process.exit(1);
}

// ---------- Utilidades ----------
function fmtBR(d) {
  return d.toLocaleDateString("pt-BR", { timeZone: "America/Sao_Paulo" });
}
function toCSV(rows) {
  if (!rows || rows.length === 0) return "";
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(headers.map(h => {
      const v = r[h] ?? "";
      const s = typeof v === "string" ? v.replace(/"/g, '""') : String(v);
      return `"${s}"`;
    }).join(","));
  }
  return lines.join("\n");
}
async function tinyPost(endpoint, formObj) {
  const body = new URLSearchParams({ token: TOKEN, formato: "json", ...formObj });
  const { data } = await axios.post(
    `${API_V2}/${endpoint}.php`,
    body.toString(),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 60000 }
  );
  return data;
}

// ---------- Produtos (pagina por pagina) ----------
async function fetchProdutosPeriodo(pesquisa = "a") {
  let pagina = 1;
  let totalPaginas = 1;
  const produtos = [];
  do {
    const body = new URLSearchParams({
      token: TOKEN, formato: "json", pesquisa, pagina: String(pagina)
    });
    const { data } = await axios.post(
      `${API_V2}/produtos.pesquisa.php`,
      body.toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 60000 }
    );
    if (data?.retorno?.status !== "OK") {
      const msg = JSON.stringify(data?.retorno?.erros || data, null, 2);
      throw new Error(`Erro produtos pág. ${pagina}: ${msg}`);
    }
    const pageItems = (data.retorno.produtos || []).map(p => p.produto);
    produtos.push(...pageItems);
    totalPaginas = Number(data.retorno.numero_paginas || 1);
    pagina++;
  } while (pagina <= totalPaginas);
  return produtos;
}

// ---------- Pedidos (lista por data, depois obter itens de cada pedido) ----------
async function fetchPedidosLista(dataInicialBR, dataFinalBR) {
  let pagina = 1;
  let totalPaginas = 1;
  const pedidos = [];
  do {
    const resp = await tinyPost("pedidos.pesquisa", {
      dataInicial: dataInicialBR,
      dataFinal: dataFinalBR,
      pagina: String(pagina),
      pesquisar: "S",
    });
    if (resp?.retorno?.status !== "OK") {
      const msg = JSON.stringify(resp?.retorno?.erros || resp, null, 2);
      throw new Error(`Erro pedidos pág. ${pagina}: ${msg}`);
    }
    const page = (resp.retorno.pedidos || []).map(p => p.pedido);
    pedidos.push(...page);
    totalPaginas = Number(resp.retorno.numero_paginas || 1);
    pagina++;
  } while (pagina <= totalPaginas);
  return pedidos;
}

async function fetchPedidoDetalhe({ id, numero }) {
  const resp = await tinyPost("pedido.obter", id ? { id: String(id) } : { numero: String(numero) });
  if (resp?.retorno?.status !== "OK") {
    const msg = JSON.stringify(resp?.retorno?.erros || resp, null, 2);
    throw new Error(`Erro pedido ${id || numero}: ${msg}`);
  }
  const pedido = resp.retorno.pedido || {};
  const itens = (pedido.itens || []).map(i => i.item);
  return { pedido, itens };
}

async function fetchItensPedidos(dataInicialBR, dataFinalBR) {
  const lista = await fetchPedidosLista(dataInicialBR, dataFinalBR);
  const itens = [];
  for (const p of lista) {
    try {
      const { itens: its } = await fetchPedidoDetalhe({ id: p.id, numero: p.numero });
      its.forEach(it => {
        itens.push({
          numeroPedido: p.numero,
          idPedido: p.id,
          data: p.data_pedido || p.data_criacao || p.dataEmissao || "",
          situacao: p.situacao || "",
          codigo: it.codigo || "",
          descricao: it.descricao || "",
          quantidade: Number(it.quantidade || 0),
          valorUnit: Number(it.valor_unitario || it.valor || 0),
          valorTotal: Number(it.valor_total || 0),
        });
      });
    } catch (e) {
      console.warn(`⚠️ Itens falharam no pedido ${p.id || p.numero}: ${e.message}`);
    }
  }
  return { pedidos: lista, itens };
}

// ---------- Insights ----------
function agregadosPorSKU(items) {
  const map = new Map();
  for (const it of items) {
    const key = it.codigo || it.descricao || "SEM_CODIGO";
    const acc = map.get(key) || { codigo: key, descricao: it.descricao || "", qtd: 0, valor: 0 };
    acc.qtd += Number(it.quantidade || 0);
    acc.valor += Number(it.valorTotal || 0);
    map.set(key, acc);
  }
  return Array.from(map.values());
}
function topNPorValor(aggs, n = 20) {
  return [...aggs].sort((a, b) => b.valor - a.valor).slice(0, n);
}
function ticketMedio(totalValor, numPedidos) {
  return numPedidos > 0 ? totalValor / numPedidos : 0;
}
function itensSemGiro(produtos, aggsPeriodo) {
  const vendidos = new Set(aggsPeriodo.map(a => a.codigo));
  return produtos
    .filter(p => p.codigo && !vendidos.has(p.codigo))
    .map(p => ({ codigo: p.codigo, nome: p.nome || p.descricao || "", situacao: p.situacao || "" }));
}
function compararPeriodos(aggsAtual, aggsAnterior, limiteQueda = 0.3) {
  const mapAnt = new Map(aggsAnterior.map(a => [a.codigo, a]));
  const quedas = [];
  for (const at of aggsAtual) {
    const ant = mapAnt.get(at.codigo);
    if (!ant) continue;
    if (ant.valor > 0) {
      const delta = at.valor - ant.valor;
      const pct = delta / ant.valor;
      if (pct <= -limiteQueda) {
        quedas.push({
          codigo: at.codigo,
          descricao: at.descricao,
          valor_atual: Number(at.valor.toFixed(2)),
          valor_anterior: Number(ant.valor.toFixed(2)),
          variacao_pct: Number((pct * 100).toFixed(1)),
        });
      }
    }
  }
  return quedas.sort((a, b) => a.variacao_pct - b.variacao_pct);
}

// ---------- Pipeline principal ----------
(async () => {
  try {
    // Janelas
    const hoje = new Date();
    const d0 = new Date(hoje);
    const d30 = new Date(hoje); d30.setDate(d0.getDate() - 30);
    const d60 = new Date(hoje); d60.setDate(d0.getDate() - 60);

    const atualIni = fmtBR(d30);
    const atualFim = fmtBR(d0);
    const antIni = fmtBR(d60);
    const antFim = fmtBR(new Date(d30.getTime() - 24 * 60 * 60 * 1000)); // 1 dia antes do atualIni

    console.log(`🧾 Período atual: ${atualIni} → ${atualFim}`);
    console.log(`🧾 Período anterior: ${antIni} → ${antFim}`);

    console.log("📦 Carregando produtos...");
    const produtos = await fetchProdutosPeriodo("a"); // "a" garante retorno amplo
    console.log(`Produtos: ${produtos.length}`);

    console.log("🧾 Carregando pedidos + itens (período ATUAL)...");
    const { pedidos: pedidosAtual, itens: itensAtual } = await fetchItensPedidos(atualIni, atualFim);
    console.log(`Pedidos atual: ${pedidosAtual.length} | Itens atual: ${itensAtual.length}`);

    console.log("🧾 Carregando pedidos + itens (período ANTERIOR)...");
    const { pedidos: pedidosAnt, itens: itensAnt } = await fetchItensPedidos(antIni, antFim);
    console.log(`Pedidos anterior: ${pedidosAnt.length} | Itens anterior: ${itensAnt.length}`);

    // KPIs
    const valorAtual = itensAtual.reduce((s, it) => s + (Number(it.valorTotal) || 0), 0);
    const valorAnt = itensAnt.reduce((s, it) => s + (Number(it.valorTotal) || 0), 0);
    const ticketAtual = ticketMedio(valorAtual, pedidosAtual.length);
    const ticketAnterior = ticketMedio(valorAnt, pedidosAnt.length);

    // Agregados por SKU
    const aggsAtual = agregadosPorSKU(itensAtual);
    const aggsAnterior = agregadosPorSKU(itensAnt);
    const top20 = topNPorValor(aggsAtual, 20);
    const semGiro = itensSemGiro(produtos, aggsAtual);
    const quedas = compararPeriodos(aggsAtual, aggsAnterior, 0.30); // queda >= 30%

    // Console resumido
    console.log("\n📊 KPIs");
    console.log(`Faturamento atual: R$ ${valorAtual.toFixed(2)}`);
    console.log(`Faturamento anterior: R$ ${valorAnt.toFixed(2)}`);
    console.log(`Ticket médio atual: R$ ${ticketAtual.toFixed(2)}`);
    console.log(`Ticket médio anterior: R$ ${ticketAnterior.toFixed(2)}`);

    console.log("\n🏆 Top 20 SKUs por faturamento (período atual)");
    console.table(top20.map(x => ({
      codigo: x.codigo,
      descricao: x.descricao,
      qtd: x.qtd,
      valor: Number(x.valor.toFixed(2))
    })));

    console.log("\n⛔ Itens sem giro no período atual (amostra de 20)");
    console.table(semGiro.slice(0, 20));

    console.log("\n📉 Quedas de vendas vs período anterior (>= 30%) — top 20 quedas");
    console.table(quedas.slice(0, 20));

    // CSVs
    fs.writeFileSync("top_skus.csv",
      toCSV(top20.map(x => ({
        codigo: x.codigo, descricao: x.descricao,
        quantidade: x.qtd, valor: x.valor.toFixed(2)
      })))
    );
    fs.writeFileSync("itens_sem_giro.csv",
      toCSV(semGiro.map(p => ({
        codigo: p.codigo, nome: p.nome, situacao: p.situacao
      })))
    );
    fs.writeFileSync("quedas_vendas.csv",
      toCSV(quedas)
    );

    console.log("\n💾 Arquivos gerados:");
    console.log(" - top_skus.csv");
    console.log(" - itens_sem_giro.csv");
    console.log(" - quedas_vendas.csv");
    console.log("✅ Pronto!");
  } catch (err) {
    console.error("❌ Falha geral:");
    console.dir(err.response?.data || err.message || err, { depth: null });
  }
})();
