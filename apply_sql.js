// apply_sql.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

(async () => {
  const file = path.join(__dirname, 'sql', 'insights_views.sql');
  const sql = fs.readFileSync(file, 'utf8');

  const client = new Client({
    host: process.env.PGHOST || 'localhost',
    port: process.env.PGPORT || 5432,
    database: process.env.PGDATABASE || 'iah_plumas',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD,
  });

  try {
    await client.connect();
    console.log('🧱 Aplicando SQL de views:', file);
    await client.query(sql);
    console.log('✅ Views criadas/atualizadas com sucesso.');
  } catch (e) {
    console.error('❌ Erro aplicando SQL:', e.message);
    process.exit(1);
  } finally {
    await client.end();
  }
})();
