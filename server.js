/**
 * SQL Server Bridge — HTTP API segura para consumo por Supabase Edge Functions
 *
 * Endpoints:
 *   GET  /health                → status
 *   POST /api/bi/query          → executa SELECT em view permitida
 *
 * Autenticação:
 *   Header: x-api-key: <BRIDGE_API_KEY>
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   BRIDGE_API_KEY=<chave-forte-aleatoria>
 *   SQLSERVER_HOST=<host>
 *   SQLSERVER_PORT=1433
 *   SQLSERVER_DB=<database>
 *   SQLSERVER_USER=<user>
 *   SQLSERVER_PASSWORD=<password>
 *   SQLSERVER_ENCRYPT=true
 *   SQLSERVER_TRUST_CERT=true
 */

const express = require("express");
const cors = require("cors");
const sql = require("mssql");

const PORT = parseInt(process.env.PORT || "3000", 10);
const API_KEY = process.env.BRIDGE_API_KEY;

if (!API_KEY) {
  console.warn("[WARN] BRIDGE_API_KEY não encontrada, rodando sem autenticação");
}

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

let pool;
async function getPool() {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
    console.log("[DB] Pool SQL Server conectado");
  }
  return pool;
}

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

// Middleware de autenticação
function requireApiKey(req, res, next) {
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }
  next();
}

// Whitelist de prefixos / nomes permitidos (espelho do edge bi-query)
const ALLOWED_PREFIXES = ["vw_", "v_bi_", "v_gold_"];
const MAX_LIMIT = 5000;

function isViewAllowed(viewName) {
  if (typeof viewName !== "string") return false;
  if (!/^[A-Za-z0-9_]{1,128}$/.test(viewName)) return false;
  return ALLOWED_PREFIXES.some((p) => viewName.startsWith(p));
}

// Health check
app.get("/health", async (_req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().query("SELECT 1 AS ok");
    res.json({ status: "ok", db: r.recordset[0].ok === 1 });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// Query principal
app.post("/api/bi/query", requireApiKey, async (req, res) => {
  const { view, limit } = req.body || {};

  if (!isViewAllowed(view)) {
    return res.status(403).json({ error: "VIEW_NOT_ALLOWED", view });
  }

  const safeLimit = Math.min(
    Math.max(parseInt(limit, 10) || 1000, 1),
    MAX_LIMIT,
  );

  try {
    const p = await getPool();
    const query = `SELECT TOP (${safeLimit}) * FROM [${view}]`;
    const start = Date.now();
    const result = await p.request().query(query);
    const duration = Date.now() - start;

    console.log(
      `[QUERY] view=${view} rows=${result.recordset.length} duration=${duration}ms`,
    );

    res.json({
      success: true,
      view,
      data: result.recordset,
      meta: { count: result.recordset.length, duration_ms: duration },
    });
  } catch (err) {
    console.error(`[ERROR] view=${view}`, err.message);
    res.status(500).json({ error: "QUERY_FAILED", message: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`[SERVER] SQL Server Bridge rodando na porta ${PORT}`);
});

// Encerramento gracioso
process.on("SIGTERM", async () => {
  if (pool) await pool.close();
  process.exit(0);
});
