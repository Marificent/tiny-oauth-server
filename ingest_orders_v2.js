// ingest_orders_v2.js
// ETL Tiny API v2 -> Postgres (orders + order_items)

require("dotenv").config();
const axios = require("axios");
const { Pool } = require("pg");
const https = require("https");

// ===== HTTPS agent (reduz "socket hang up") =====
const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 4,
});

// ===== ENV =====
const TINY_API_TOKEN = process.env.TINY_API_TOKEN;
if (!TINY_API_TOKEN) {
  console.error("❌ TINY_API_TOKEN não está no .env");
  process.exit(1);
}

const pool = new Pool({
  host: process.env.PGHOST || "localhost",
  port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
  user: process.env.PGUSER || "postgres",
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE || "iah_plumas",
  ssl: {
    rejectUnauthorized: false, // 🔥 obrigatório pro Postgres do Render
  },
});


// ===== INTERVALO DE BUSCA =====
// Data fixa de início (01/01/2020) e final = hoje
const DATA_INICIAL_FIXA = new Date("2020-01-01T00:00:00-03:00");

// ===== RATE LIMIT =====
const SLEEP_BETWEEN_DETAIL_MS = 500; // pausa entre detalhes
const SLEEP_BETWEEN_PAGES_MS = 1000; // pausa entre páginas

// ===== Helpers =====
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function formatDateBR(d) {
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

// ===== Tiny GET com retry/backoff =====
async function tinyGet(endpoint, params, attempt = 1) {
  const url = `https://api.tiny.com.br${endpoint}`; // <<< HOST CORRETO DA V2
  try {
    const { data } = await axios.get(url, {
      params: { ...params, token: TINY_API_TOKEN, formato: "json" },
      timeout: 45000,
      httpsAgent,
      headers: {
        "User-Agent": "IAHolding-ETL/1.0",
        Accept: "application/json,text/plain,*/*",
      },
      maxRedirects: 5, // Cloudflare pode responder 301
      transitional: { clarifyTimeoutError: true },
      validateStatus: (s) => s >= 200 && s < 300,
    });
    return data;
  } catch (err) {
    const code = err?.code || "";
    const msg = (err?.message || "").toLowerCase();

    const retryable =
      code === "ECONNRESET" ||
      code === "ETIMEDOUT" ||
      code === "EAI_AGAIN" ||
      msg.includes("socket hang up") ||
      msg.includes("timeout");

    if (retryable && attempt <= 5) {
      const wait = 1500 * attempt; // backoff incremental
      console.log(`⏳ Retentando ${endpoint} (tentativa ${attempt}/5) após ${wait}ms...`);
      await sleep(wait);
      return tinyGet(endpoint, params, attempt + 1);
    }

    if (err.response?.data) {
      console.log(
        "↩️ Resposta de erro Tiny:",
        typeof err.response.data === "string" ? err.response.data : JSON.stringify(err.response.data)
      );
    }
    throw err;
  }
}

// ===== Schema =====
const ensureTablesSQL = `
CREATE TABLE IF NOT EXISTS public.orders (
  id              BIGINT PRIMARY KEY,
  numero          TEXT,
  data_pedido     TIMESTAMPTZ,
  cliente_id      TEXT,
  cliente_nome    TEXT,
  canal_venda     TEXT,
  situacao        TEXT,
  valor_produtos  NUMERIC,
  valor_desconto  NUMERIC,
  valor_frete     NUMERIC,
  valor_total     NUMERIC,
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
  order_id        BIGINT,
  item_seq        INTEGER,
  produto_id      TEXT,
  produto_codigo  TEXT,
  produto_nome    TEXT,
  quantidade      NUMERIC,
  valor_unitario  NUMERIC,
  valor_total     NUMERIC,
  PRIMARY KEY (order_id, item_seq),
  FOREIGN KEY (order_id) REFERENCES public.orders(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_orders_data ON public.orders (data_pedido);
CREATE INDEX IF NOT EXISTS idx_order_items_prod ON public.order_items (produto_codigo);
`;

async function upsertOrder(client, o) {
  const sql = `
  INSERT INTO public.orders
    (id, numero, data_pedido, cliente_id, cliente_nome, canal_venda, situacao,
     valor_produtos, valor_desconto, valor_frete, valor_total, atualizado_em)
  VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
  ON CONFLICT (id) DO UPDATE SET
    numero = EXCLUDED.numero,
    data_pedido = EXCLUDED.data_pedido,
    cliente_id = EXCLUDED.cliente_id,
    cliente_nome = EXCLUDED.cliente_nome,
    canal_venda = EXCLUDED.canal_venda,
    situacao = EXCLUDED.situacao,
    valor_produtos = EXCLUDED.valor_produtos,
    valor_desconto = EXCLUDED.valor_desconto,
    valor_frete = EXCLUDED.valor_frete,
    valor_total = EXCLUDED.valor_total,
    atualizado_em = now();
  `;
  await client.query(sql, [
    o.id,
    o.numero,
    o.data_pedido,
    o.cliente_id,
    o.cliente_nome,
    o.canal_venda,
    o.situacao,
    o.valor_produtos,
    o.valor_desconto,
    o.valor_frete,
    o.valor_total,
  ]);
}

async function upsertOrderItems(client, orderId, items) {
  await client.query(`DELETE FROM public.order_items WHERE order_id = $1`, [orderId]);

  if (!items || !items.length) return;

  const insertSQL = `
    INSERT INTO public.order_items
      (order_id, item_seq, produto_id, produto_codigo, produto_nome,
       quantidade, valor_unitario, valor_total)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (order_id, item_seq) DO UPDATE SET
      produto_id = EXCLUDED.produto_id,
      produto_codigo = EXCLUDED.produto_codigo,
      produto_nome = EXCLUDED.produto_nome,
      quantidade = EXCLUDED.quantidade,
      valor_unitario = EXCLUDED.valor_unitario,
      valor_total = EXCLUDED.valor_total;
  `;

  let seq = 1;
  for (const it of items) {
    await client.query(insertSQL, [
      orderId,
      seq++,
      it.produto_id || null,
      it.codigo || null,
      it.descricao || null,
      it.quantidade != null ? Number(it.quantidade) : null,
      it.valor_unitario != null ? Number(it.valor_unitario) : null,
      it.valor_total != null ? Number(it.valor_total) : null,
    ]);
  }
}

// ===== Mappers =====
function parseOrderFromPesquisa(p) {
  const base = p.pedido || p;

  const dataStr = base.data_pedido || base.data_emissao || base.data || null;
  let dataISO = null;
  if (dataStr && /\d{2}\/\d{2}\/\d{4}/.test(dataStr)) {
    const [d, m, y] = dataStr.split("/");
    // Ajuste o timezone da sua operação, -03:00 para BRT
    dataISO = new Date(`${y}-${m}-${d}T00:00:00-03:00`).toISOString();
  }

  return {
    id: base.id ? Number(base.id) : null,
    numero: base.numero || null,
    data_pedido: dataISO,
    cliente_id: base.id_cliente || base.cliente_id || null,
    cliente_nome: base.nome || base.cliente || base.cliente_nome || null,
    canal_venda: base.canal_venda || base.origem || null,
    situacao: base.situacao || base.status || null,
    valor_produtos: base.valor_produtos != null ? Number(base.valor_produtos) : null,
    valor_desconto: base.valor_desconto != null ? Number(base.valor_desconto) : null,
    valor_frete: base.valor_frete != null ? Number(base.valor_frete) : null,
    valor_total: base.valor_total != null ? Number(base.valor_total) : null,
  };
}

function mapItensFromDetalhe(det) {
  const itens = [];
  const lista = det?.retorno?.pedido?.itens || det?.retorno?.itens || [];
  for (const raw of lista) {
    const item = raw.item || raw;
    itens.push({
      produto_id: item.id_produto || item.id || null,
      codigo: item.codigo || item.sku || null,
      descricao: item.descricao || item.nome || null,
      quantidade: item.quantidade != null ? Number(item.quantidade) : null,
      valor_unitario: item.valor_unitario != null ? Number(item.valor_unitario) : null,
      valor_total: item.valor_total != null ? Number(item.valor_total) : null,
    });
  }
  return itens;
}

// ===== Main =====
(async () => {
  const client = await pool.connect();
  try {
    console.log("🔧 Garantindo tabelas...");
    await client.query(ensureTablesSQL);

    const hoje = new Date();

    // ===== Descobre última data já salva no banco (para ficar incremental) =====
    console.log("🔎 Verificando última data de pedido já gravada no banco...");
    const { rows } = await client.query(
      "SELECT MAX(data_pedido) AS max_data FROM public.orders"
    );

    let inicio;

      if (!rows[0] || !rows[0].max_data) {
      // Banco vazio: começa na data fixa (01/01/2020)
      inicio = DATA_INICIAL_FIXA;
      console.log(
        `📂 Nenhum pedido encontrado em public.orders. Usando data inicial fixa ${formatDateBR(
      inicio
     )}.`
  );
} else {
  const ultimaData = new Date(rows[0].max_data);
  const proximoDia = new Date(ultimaData);
  proximoDia.setDate(ultimaData.getDate() + 1);
  inicio = proximoDia;

  console.log(
    `📂 Último pedido no banco em: ${formatDateBR(
      ultimaData
    )}. Buscando a partir de ${formatDateBR(inicio)}.`
  );
}

// Se por algum motivo a data inicial calculada for maior que hoje, não faz nada
if (inicio > hoje) {
  console.log(
    `✅ Não há novos dias para buscar. Última data (${formatDateBR(
      inicio
    )}) já é maior que hoje (${formatDateBR(hoje)}).`
  );
  client.release();
  await pool.end();
  return;
}

const dataInicial = formatDateBR(inicio);
const dataFinal = formatDateBR(hoje);

console.log(`📅 Buscando pedidos de ${dataInicial} a ${dataFinal} ...`);

let totalPaginas = 1;
let totalUpserts = 0;
let pagina = 1;


    do {
      const pesquisa = await tinyGet("/api2/pedidos.pesquisa.php", {
        dataInicial,
        dataFinal,
        pagina,
      });

      const ret = pesquisa?.retorno || {};
      totalPaginas = Number(ret?.numero_paginas || ret?.numeroPaginas || 1);
      const lista = ret?.pedidos || [];

      if (!Array.isArray(lista) || lista.length === 0) {
        console.log(`↩️ Página ${pagina}/${totalPaginas} sem registros.`);
        pagina++;
        await sleep(SLEEP_BETWEEN_PAGES_MS);
        continue;
      }

      for (const p of lista) {
        const ped = parseOrderFromPesquisa(p);
        if (!ped.id) continue;

        // detalhe do pedido (para itens)
        let detalhe = null;
        try {
          detalhe = await tinyGet("/api2/pedido.obter.php", { id: ped.id });
        } catch {
          if (ped.numero) {
            try {
              detalhe = await tinyGet("/api2/pedido.obter.php", { numero: ped.numero });
            } catch {
              console.warn(`⚠️ Falha ao obter detalhe do pedido ${ped.id}/${ped.numero}`);
            }
          } else {
            console.warn(`⚠️ Falha ao obter detalhe do pedido ${ped.id}`);
          }
        }

        const itens = mapItensFromDetalhe(detalhe);

        // UPSERT
        await upsertOrder(client, ped);
        await upsertOrderItems(client, ped.id, itens);
        totalUpserts++;

        await sleep(SLEEP_BETWEEN_DETAIL_MS);
      }

      console.log(`✅ Página ${pagina}/${totalPaginas} processada (${lista.length} pedidos).`);
      pagina++;
      await sleep(SLEEP_BETWEEN_PAGES_MS);
    } while (pagina <= totalPaginas);

    console.log(`🎉 Concluído. Pedidos upsertados: ${totalUpserts}`);
  } catch (err) {
    const msg = err?.response?.data || err.message || err;
    console.error("❌ Erro na ingestão:", msg);
  } finally {
    client.release();
    await pool.end();
  }
})();

