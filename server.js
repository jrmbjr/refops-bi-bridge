const express = require("express");
const cors = require("cors");
const sql = require("mssql");

const app = express();

// 🔑 Porta do Railway (NÃO alterar)
const PORT = process.env.PORT;

// 🔐 API Key (opcional)
const API_KEY = process.env.BRIDGE_API_KEY;

// ⚠️ aviso apenas
if (!API_KEY) {
  console.warn("[WARN] BRIDGE_API_KEY não encontrada");
}

// Middlewares
app.use(cors());
app.use(express.json());

// 🔥 ROTA RAIZ (ESSENCIAL)
app.get("/", (req, res) => {
  res.send("RefOps BI Bridge rodando");
});

// ❤️ HEALTH (rápido, sem banco)
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "refops-bi-bridge",
  });
});

// 🔎 CONFIG SQL (só será usado quando necessário)
const sqlConfig = {
  user: process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  server: process.env.SQLSERVER_HOST,
  port: parseInt(process.env.SQLSERVER_PORT || "1433", 10),
  database: process.env.SQLSERVER_DB,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// 🔌 Conexão lazy
let pool;
async function getPool() {
  if (!pool) {
    console.log("[DB] conectando...");
    pool = await sql.connect(sqlConfig);
    console.log("[DB] conectado");
  }
  return pool;
}

// 🔐 Middleware de autenticação (se houver API key)
function requireApiKey(req, res, next) {
  if (!API_KEY) return next();

  const key = req.header("x-api-key");
  if (key !== API_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }

  next();
}

// 🔎 Teste de banco (isolado)
app.get("/health/db", async (req, res) => {
  try {
    const p = await getPool();
    await p.request().query("SELECT 1");

    res.json({ status: "ok", db: true });
  } catch (err) {
    res.json({
      status: "ok",
      db: false,
      error: err.message,
    });
  }
});

// 📊 Query simples (teste)
app.post("/api/query", requireApiKey, async (req, res) => {
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
    res.status(500).json({
      error: "QUERY_FAILED",
      message: err.message,
    });
  }
});

// 🚀 START
app.listen(PORT, () => {
  console.log("[SERVER] rodando na porta", PORT);
});
