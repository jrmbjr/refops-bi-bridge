const express = require("express");
const sql = require("mssql");

const app = express();
app.use(express.json());

// CONFIG SQL SERVER
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER, // ex: 123.123.123.123
  database: process.env.DB_NAME,
  options: {
    encrypt: false, // true se usar Azure
    trustServerCertificate: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// CONEXÃO GLOBAL (POOL)
let pool;

async function getPool() {
  if (!pool) {
    pool = await sql.connect(config);
  }
  return pool;
}

// HEALTH CHECK
app.get("/", (req, res) => {
  res.send("API Bridge rodando 🚀");
});

// QUERY GENÉRICA (CUIDADO EM PRODUÇÃO)
app.post("/query", async (req, res) => {
  try {
    const { query } = req.body;

    if (!query) {
      return res.status(400).json({ error: "Query não enviada" });
    }

    const pool = await getPool();
    const result = await pool.request().query(query);

    res.json(result.recordset);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao executar query" });
  }
});

// EXEMPLO DE ROTA ESPECÍFICA (RECOMENDADO)
app.get("/clientes", async (req, res) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT TOP 100 *
      FROM Clientes
    `);

    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// PORTA DINÂMICA (ESSENCIAL PRA RAILWAY)
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
