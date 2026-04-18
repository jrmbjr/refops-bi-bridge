const express = require("express");
const sql = require("mssql");

const app = express();
app.use(express.json());

// ==============================
// CONFIG SQL SERVER
// ==============================
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// ==============================
// POOL REUTILIZÁVEL
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
// TESTE DB
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
// LISTAR VIEWS
// ==============================
app.get("/views", async (req, res) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT TABLE_NAME
      FROM INFORMATION_SCHEMA.VIEWS
      ORDER BY TABLE_NAME
    `);

    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// CONSULTAR VIEW
// ==============================
app.get("/view/:nome", async (req, res) => {
  try {
    const { nome } = req.params;
    const pool = await getPool();

    const result = await pool.request().query(`
      SELECT TOP 100 * FROM ${nome}
    `);

    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// QUERY LIVRE
// ==============================
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
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// START SERVER
// ==============================
const PORT = process.env.PORT || 3000;

console.log("PORT ENV:", process.env.PORT);

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando na porta " + PORT);
});
