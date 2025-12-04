// db.js
require("dotenv").config();
const { Pool } = require("pg");

// Pool de conexão com SSL obrigatório para Render Postgres
const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  
  // 🔥 O Postgres do Render SEMPRE exige SSL, inclusive localmente
  ssl: {
    rejectUnauthorized: false,
  },
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool,
};
