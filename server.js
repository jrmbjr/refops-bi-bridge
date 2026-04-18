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

let pool;

async function getPool() {
  if (!pool) {
    console.log("[DB] conectando...");
    pool = await sql.connect(config);
    console.log("[DB] conectado");
  }
  return pool;
}

// ==============================
// HEALTH
// ==============================
app.get("/", (req, res) => {
  res.send("API Bridge rodando 🚀");
});

app.get("/ping", (req, res) => {
  res.json({ status: "alive" });
});

// ==============================
// TESTE BANCO
// ==============================
app.get("/teste-db", async (req, res) => {
  try {
    const p = await getPool();
    const result = await p.request().query("SELECT GETDATE() AS data");
    res.json(result.recordset);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==============================
// 🔥 QUERY DINÂMICA (ESSENCIAL)
// ==============================
app.post("/api/query", async (req, res) => {
  const { query } = req.body;

  if (!query) {
    return res.status(400).json({ error: "QUERY_REQUIRED" });
  }

  try {
    const p = await getPool();
    const result = await p.request().query(query);

    res.json({
      success: true,
      data: result.recordset,
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "QUERY_FAILED",
      message: err.message,
    });
  }
});

// ==============================
// START (CORRETO NO RAILWAY)
// ==============================
const PORT = process.env.PORT;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando na porta " + PORT);
});
