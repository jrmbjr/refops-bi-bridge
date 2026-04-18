/**
 * SQL Server Bridge — HTTP API segura para consumo por Supabase Edge Functions
 *
 * Endpoints:
 *   GET  /health             → status + ping no banco
 *   POST /api/bi/query       → executa SELECT em view permitida (legado bi-query)
 *   POST /query              → executa SQL vindo da edge sqlserver-query
 *                              (named queries OU raw_sql já validado pela edge)
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

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const sql = require("mssql");

const PORT = parseInt(process.env.PORT || "3000", 10);
const API_KEY = process.env.BRIDGE_API_KEY;

if (!API_KEY) {
  console.error("[FATAL] BRIDGE_API_KEY não configurado no .env");
  process.exit(1);
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
app.use(express.json({ limit: "1mb" }));

// ───────────────────────────────────────────────
// Auth
// ───────────────────────────────────────────────
function requireApiKey(req, res, next) {
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }
  next();
}

// ───────────────────────────────────────────────
// Whitelist legada (rota /api/bi/query)
// ───────────────────────────────────────────────
const ALLOWED_PREFIXES = ["vw_", "v_bi_", "v_gold_"];
const MAX_LIMIT = 5000;

function isViewAllowed(viewName) {
  if (typeof viewName !== "string") return false;
  if (!/^[A-Za-z0-9_]{1,128}$/.test(viewName)) return false;
  return ALLOWED_PREFIXES.some((p) => viewName.startsWith(p));
}

// ───────────────────────────────────────────────
// Validação de SQL para /query
// Aceita apenas SELECT / WITH (CTE) — bloqueia DML/DDL
// ───────────────────────────────────────────────
const FORBIDDEN_TOKENS = [
  /\bINSERT\b/i,
  /\bUPDATE\b/i,
  /\bDELETE\b/i,
  /\bDROP\b/i,
  /\bALTER\b/i,
  /\bTRUNCATE\b/i,
  /\bCREATE\b/i,
  /\bEXEC\b/i,
  /\bEXECUTE\b/i,
  /\bMERGE\b/i,
  /\bGRANT\b/i,
  /\bREVOKE\b/i,
  /;\s*\w/, // múltiplas statements
];

function isSqlReadOnly(rawSql) {
  if (typeof rawSql !== "string") return false;
  const trimmed = rawSql.trim().replace(/;+\s*$/, "");
  if (!trimmed) return false;
  if (!/^(SELECT|WITH)\b/i.test(trimmed)) return false;
  return !FORBIDDEN_TOKENS.some((re) => re.test(trimmed));
}

// ───────────────────────────────────────────────
// Health
// ───────────────────────────────────────────────
app.get("/health", async (_req, res) => {
  try {
    const p = await getPool();
    const r = await p.request().query("SELECT 1 AS ok");
    res.json({ status: "ok", db: r.recordset[0].ok === 1 });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ───────────────────────────────────────────────
// Rota legada (mantida para compatibilidade)
// ───────────────────────────────────────────────
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

// ───────────────────────────────────────────────
// NOVA rota: /query
// Consumida pela edge function sqlserver-query
//
// Body esperado:
// {
//   "sql": "SELECT TOP 100 * FROM vw_veiculos",
//   "params": { "id": 123 },     // opcional, parametrização nomeada
//   "source": "named|raw_sql",   // opcional, só para log
//   "queryName": "list_tables"   // opcional, só para log
// }
// ───────────────────────────────────────────────
app.post("/query", requireApiKey, async (req, res) => {
  const { sql: rawSql, params, source, queryName } = req.body || {};

  if (!rawSql || typeof rawSql !== "string") {
    return res.status(400).json({ error: "MISSING_SQL" });
  }

  if (rawSql.length > 20000) {
    return res.status(413).json({ error: "SQL_TOO_LARGE" });
  }

  if (!isSqlReadOnly(rawSql)) {
    return res.status(403).json({
      error: "SQL_NOT_ALLOWED",
      message: "Apenas SELECT/WITH são permitidos.",
    });
  }

  try {
    const p = await getPool();
    const request = p.request();

    // Bind de parâmetros nomeados — usar @nome no SQL
    if (params && typeof params === "object") {
      for (const [key, value] of Object.entries(params)) {
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
          return res
            .status(400)
            .json({ error: "INVALID_PARAM_NAME", param: key });
        }
        request.input(key, value);
      }
    }

    const start = Date.now();
    const result = await request.query(rawSql);
    const duration = Date.now() - start;

    console.log(
      `[/query] source=${source || "?"} name=${queryName || "?"} rows=${result.recordset?.length ?? 0} duration=${duration}ms`,
    );

    res.json({
      success: true,
      data: result.recordset || [],
      meta: {
        count: result.recordset?.length ?? 0,
        duration_ms: duration,
        source: source || null,
        queryName: queryName || null,
      },
    });
  } catch (err) {
    console.error(
      `[/query ERROR] source=${source || "?"} name=${queryName || "?"}`,
      err.message,
    );
    res.status(500).json({ error: "QUERY_FAILED", message: err.message });
  }
});

// ───────────────────────────────────────────────
// Start
// ───────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[SERVER] SQL Server Bridge rodando na porta ${PORT}`);
});

process.on("SIGTERM", async () => {
  if (pool) await pool.close();
  process.exit(0);
});
