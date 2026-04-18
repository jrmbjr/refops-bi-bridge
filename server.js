const express = require("express");
const sql = require("mssql");

const app = express();
app.use(express.json());

// ==============================
// CONFIGURAÇÃO SQL SERVER
// ==============================
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER, // ex: 123.123.123.123
  database: process.env.DB_NAME,
  options: {
    encrypt: false, // true se for Azure
    trustServerCertificate: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// ==============================
// POOL DE CONEXÃO (REUTILIZÁVEL)
// ==============================
let pool;

async function getPool() {
  if (!pool) {
    pool = await sql.connect(config);
  }
  return pool;
}

// ==============================
// HEALTH CHECK
// ==============================
app.get("/", (req, res) => {
  res.send("API Bridge rodando 🚀");
});

// ==============================
// TESTE DE BANCO
// ==============================
app.get("/teste-db", async (req, res) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query("SELECT GETDATE() AS data");

    res.json(result.recordset);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// EXEMPLO USANDO VIEW (RECOMENDADO)
// ==============================
app.get("/clientes", async (req, res) => {
  try {
    const pool = await getPool();

    const result = await pool.request().query(`
      SELECT TOP 100 *
      FROM SUA_VIEW_AQUI
    `);

    res.json(result.recordset);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// START DO SERVIDOR (RAILWAY OK)
// ==============================
const PORT = 3000;

app.get("/", (req, res) => {
  res.status(200).send("OK");
});

app.get("/ping", (req, res) => {
  res.status(200).json({ status: "alive" });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando na porta " + PORT);
});
