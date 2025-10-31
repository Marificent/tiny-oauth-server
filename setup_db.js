// setup_db.js (versão corrigida)
require("dotenv").config();
const { Client } = require("pg");

const {
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
} = process.env;

(async () => {
  // 1) Conecta no "postgres" para garantir/criar o DB de trabalho
  const admin = new Client({
    host: PGHOST, port: PGPORT, user: PGUSER, password: PGPASSWORD, database: "postgres",
  });
  await admin.connect();

  try {
    await admin.query(`CREATE DATABASE ${PGDATABASE};`);
    console.log(`✅ Database criado: ${PGDATABASE}`);
  } catch (e) {
    // 42P04 = database already exists
    if (e.code === "42P04") {
      console.log(`ℹ️ Database já existia: ${PGDATABASE}`);
    } else {
      console.error("❌ Erro criando database:", e.message);
      process.exit(1);
    }
  } finally {
    await admin.end();
  }

  // 2) Conecta no DB de trabalho e cria schema/tabelas
  const db = new Client({
    host: PGHOST, port: PGPORT, user: PGUSER, password: PGPASSWORD, database: PGDATABASE,
  });
  await db.connect();

  try {
    await db.query("CREATE SCHEMA IF NOT EXISTS dw;");

    // Tabelas base (mínimo para começarmos)
    await db.query(`
      CREATE TABLE IF NOT EXISTS dw.products (
        id BIGINT PRIMARY KEY,
        codigo TEXT,
        nome TEXT,
        preco NUMERIC,
        preco_promocional NUMERIC,
        unidade TEXT,
        gtin TEXT,
        tipo_variacao TEXT,
        localizacao TEXT,
        preco_custo NUMERIC,
        preco_custo_medio NUMERIC,
        situacao TEXT,
        criado_em TIMESTAMP NULL
      );

      CREATE TABLE IF NOT EXISTS dw.orders (
        id BIGINT PRIMARY KEY,
        numero TEXT,
        situacao TEXT,
        criado_em TIMESTAMP NULL,
        total NUMERIC,
        total_produtos NUMERIC,
        desconto NUMERIC,
        frete NUMERIC,
        cliente_id BIGINT
      );

      CREATE TABLE IF NOT EXISTS dw.order_items (
        order_id BIGINT,
        produto_id BIGINT,
        codigo TEXT,
        descricao TEXT,
        qtde NUMERIC,
        valor_unitario NUMERIC,
        PRIMARY KEY (order_id, produto_id)
      );
    `);

    // Dimensão de datas (sem CTE recursivo; evita o erro de tipo)
    await db.query(`
      CREATE TABLE IF NOT EXISTS dw.dim_date (
        dt DATE PRIMARY KEY,
        year INT,
        month INT,
        day INT,
        month_name TEXT,
        dow INT,
        dow_name TEXT,
        is_weekend BOOLEAN
      );
    `);

    // Popula dimensão (6 anos para trás até 1 ano à frente)
    await db.query(`
      INSERT INTO dw.dim_date (dt, year, month, day, month_name, dow, dow_name, is_weekend)
      SELECT
        d::date                                      AS dt,
        EXTRACT(YEAR  FROM d)::INT                   AS year,
        EXTRACT(MONTH FROM d)::INT                   AS month,
        EXTRACT(DAY   FROM d)::INT                   AS day,
        TO_CHAR(d, 'TMMonth')                        AS month_name,
        EXTRACT(DOW   FROM d)::INT                   AS dow,
        TO_CHAR(d, 'TMDay')                          AS dow_name,
        (EXTRACT(DOW FROM d)::INT IN (0,6))          AS is_weekend
      FROM generate_series(
        (CURRENT_DATE - INTERVAL '6 years'),
        (CURRENT_DATE + INTERVAL '1 year'),
        INTERVAL '1 day'
      ) AS g(d)
      ON CONFLICT (dt) DO NOTHING;
    `);

    console.log("✅ Schema e tabelas criadas.");
    console.log("✅ dim_date populada.");
  } catch (e) {
    console.error("❌ Erro no setup:", e.message);
  } finally {
    await db.end();
  }
})();
