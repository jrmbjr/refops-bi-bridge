/**
 * SQL Server Bridge — HTTP API segura para consumo por Supabase Edge Functions
 */

const express = require("express");
const cors = require("cors");
const sql = require("mssql");

// 🔑 ENV
const PORT = process.env.PORT; // Railway injeta automaticamente
const API_KEY = process.env.BRIDGE_API_KEY;

// ⚠️ Não derruba mais a aplicação
if (!API_KEY) {
  console.warn("[WARN] BRIDGE_API_KEY não encontrada, rodando sem autenticação");
}

// 🧠 CONFIG SQL SERVER
const sqlConfig = {
  user: process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  server: process.env.SQLSERVER_HOST,
  port: parseInt(process.env.SQLSERVER_PORT || "1433", 10),
  database: process.env.SQLSERVER_DB,
  pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
  options: {
    encrypt: process.env.SQLSERVER_ENCRYPT !== "false",
    trustServerCertificate: process.env.SQLSERVER_TRUST_CERT !== "false",
    enableArithAbort: true,
  },
  connectionTimeout: 30000,
  requestTimeout: 60000,
};

// 🔌 POOL
let pool;
async function getPool() {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
    console.log("[DB] Pool SQL Server conectado");
  }
  return pool;
}

// 🚀 APP
const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

// 🔐 AUTH (opcional se API_KEY não existir)
function requireApiKey(req, res, next) {
  if (!API_KEY) return next();

  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }

  next();
}

// 📊 WHITELIST
const ALLOWED_PREFIXES = ["vw_", "v_bi_", "v_gold_"];
const MAX_LIMIT = 5000;

function isViewAllowed(viewName) {
  if (typeof viewName !== "string") return false;
  if (!/^[A-Za-z0-9_]{1,128}$/.test(viewName)) return false;
  return ALLOWED_PREFIXES.some((p) => viewName.startsWith(p));
}

// ❤️ HEALTH (NUNCA QUEBRA)
app.get("/health", async (_req, res) => {
  try {
    const p = await getPool();
    await p.request().query("SELECT 1 AS ok");

    return res.json({
      status: "ok",
      db: true,
    });
  } catch (err) {
    console.error("Erro no health:", err.message);

    return res.json({
      status: "ok",
      db: false,
      error: err.message,
    });
  }
});

// 📥 QUERY
app.post("/api/bi/query", requireApiKey, async (req, res) => {
  const { view, limit } = req.body || {};

  if (!isViewAllowed(view)) {
    return res.status(403).json({ error: "VIEW_NOT_ALLOWED", view });
  }

  const safeLimit = Math.min(
    Math.max(parseInt(limit, 10) || 1000, 1),
    MAX_LIMIT
  );

  try {
    const p = await getPool();

    const query = `SELECT TOP (${safeLimit}) * FROM [${view}]`;
    const start = Date.now();

    const result = await p.request().query(query);
    const duration = Date.now() - start;

    console.log(
      `[QUERY] view=${view} rows=${result.recordset.length} duration=${duration}ms`
    );

    return res.json({
      success: true,
      view,
      data: result.recordset,
      meta: {
        count: result.recordset.length,
        duration_ms: duration,
      },
    });
  } catch (err) {
    console.error(`[ERROR] view=${view}`, err.message);

    return res.status(500).json({
      error: "QUERY_FAILED",
      message: err.message,
    });
  }
});

// 🚀 START
app.listen(PORT, () => {
  console.log(`[SERVER] rodando na porta ${PORT}`);
});

// 🛑 SHUTDOWN
process.on("SIGTERM", async () => {
  if (pool) await pool.close();
  process.exit(0);
});
