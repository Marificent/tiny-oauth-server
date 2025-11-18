// queryBuilder.js

// LISTA DE TAGS DISPONÍVEIS → você pode colocar todas as suas
const KNOWN_TAGS = [
  "pluma chorona",
  "pluma chorona e kits",
  "plumas",
  "kits",
  "base",
  "luxo",
  // adicione quantas quiser
];

function extractYear(question) {
  const m = question.match(/20\d{2}/);
  return m ? m[0] : null;
}

function extractMonth(question) {
  const months = {
    janeiro: 1, fevereiro: 2, marco: 3, março: 3,
    abril: 4, maio: 5, junho: 6,
    julho: 7, agosto: 8, setembro: 9,
    outubro: 10, novembro: 11, dezembro: 12
  };
  for (const m in months) {
    if (question.includes(m)) return months[m];
  }
  return null;
}

function extractTags(question) {
  const found = [];
  for (const tag of KNOWN_TAGS) {
    if (question.includes(tag)) found.push(tag);
  }
  return found;
}

function wantsAggregation(question) {
  return (
    question.includes("quanto") ||
    question.includes("total") ||
    question.includes("faturamento") ||
    question.includes("vendi") ||
    question.includes("vendas")
  );
}

function buildQueryFromQuestion(question) {
  const q = question.toLowerCase();

  const year = extractYear(q);
  const month = extractMonth(q);
  const tags = extractTags(q);
  const aggregate = wantsAggregation(q);

  let sql = "";
  let where = [];

  // ------------------------
  // 1. BASE DA QUERY
  // ------------------------

  if (aggregate) {
    sql = `
      SELECT
        SUM(valor_total) AS receita_total,
        SUM(quantidade) AS quantidade_total,
        COUNT(*) AS linhas
      FROM analytics.vw_tiny_sales_enriched
    `;
  } else {
    sql = `
      SELECT
        data,
        produto,
        tags,
        quantidade,
        valor_total AS receita
      FROM analytics.vw_tiny_sales_enriched
    `;
  }

  // ------------------------
  // 2. FILTROS POR TAGS
  // ------------------------

  if (tags.length > 0) {
    const tagFilters = tags.map(t => `tags ILIKE '%${t}%'`);
    where.push(`(${tagFilters.join(" OR ")})`);
  }

  // ------------------------
  // 3. FILTRO POR ANO
  // ------------------------

  if (year) {
    where.push(`EXTRACT(YEAR FROM data) = ${year}`);
  }

  // ------------------------
  // 4. FILTRO POR MÊS
  // ------------------------

  if (month) {
    where.push(`EXTRACT(MONTH FROM data) = ${month}`);
  }

  // ------------------------
  // 5. JUNTAR FILTROS
  // ------------------------

  if (where.length > 0) {
    sql += " WHERE " + where.join(" AND ");
  }

  // ------------------------
  // 6. ORDENAR SE NÃO FOR AGREGADO
  // ------------------------

  if (!aggregate) {
    sql += " ORDER BY data ASC";
  }

  return { sql, params: [] };
}

module.exports = { buildQueryFromQuestion };
