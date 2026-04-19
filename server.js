/**
 * SQL Server Bridge — HTTP API segura para consumo por Supabase Edge Functions
 *
 * v1.1.0 — Streaming + hard limit anti-OOM (Railway free tier 512MB)
 *
 * Endpoints:
 *   GET  /health             → status + ping no banco
 *   POST /api/bi/query       → executa SELECT em view permitida (legado bi-query)
 *   POST /query              → executa SQL vindo da edge sqlserver-query
 *                              (streaming + hard limit de linhas)
 *
 * Autenticação: Header x-api-key: <BRIDGE_API_KEY>
 * Override de limite: Header x-allow-large: 1  (uso pontual, com cuidado)
 *
 * Variáveis de ambiente (.env):
 *   PORT=3000
 *   BRIDGE_API_KEY=<chave-forte-aleatoria>
 *   HARD_LIMIT_ROWS=100000        ← NOVO: corta query antes de OOM
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
const HARD_LIMIT_ROWS = parseInt(process.env.HARD_LIMIT_ROWS || "100000", 10);

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
// Validação SQL — só SELECT/WITH
// ───────────────────────────────────────────────
const FORBIDDEN_TOKENS = [
  /\bINSERT\b/i, /\bUPDATE\b/i, /\bDELETE\b/i, /\bDROP\b/i,
  /\bALTER\b/i, /\bTRUNCATE\b/i, /\bCREATE\b/i, /\bEXEC\b/i,
  /\bEXECUTE\b/i, /\bMERGE\b/i, /\bGRANT\b/i, /\bREVOKE\b/i,
  /;\s*\w/,
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
    res.json({ status: "ok", db: r.recordset[0].ok === 1, hard_limit_rows: HARD_LIMIT_ROWS });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ───────────────────────────────────────────────
// Rota legada /api/bi/query (mantida)
// ───────────────────────────────────────────────
app.post("/api/bi/query", requireApiKey, async (req, res) => {
  const { view, limit } = req.body || {};
  if (!isViewAllowed(view)) return res.status(403).json({ error: "VIEW_NOT_ALLOWED", view });

  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 1000, 1), MAX_LIMIT);

  try {
    const p = await getPool();
    const query = `SELECT TOP (${safeLimit}) * FROM [${view}]`;
    const start = Date.now();
    const result = await p.request().query(query);
    const duration = Date.now() - start;

    console.log(`[QUERY] view=${view} rows=${result.recordset.length} duration=${duration}ms`);

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
// /query — STREAMING (anti-OOM)
//
// Em vez de carregar o recordset inteiro em memória e fazer res.json(),
// escrevemos cada linha direto no socket usando request.stream = true.
// Isso mantém o uso de memória ~constante (~30-50MB) mesmo com 100k linhas.
//
// Hard limit: aborta a query se passar de HARD_LIMIT_ROWS (default 100k).
// Override: header x-allow-large: 1 (uso pontual).
// ───────────────────────────────────────────────
app.post("/query", requireApiKey, async (req, res) => {
  const { sql: rawSql, params, source, queryName } = req.body || {};
  const allowLarge = req.header("x-allow-large") === "1";
  const rowCap = allowLarge ? Number.MAX_SAFE_INTEGER : HARD_LIMIT_ROWS;

  if (!rawSql || typeof rawSql !== "string") return res.status(400).json({ error: "MISSING_SQL" });
  if (rawSql.length > 20000) return res.status(413).json({ error: "SQL_TOO_LARGE" });
  if (!isSqlReadOnly(rawSql)) {
    return res.status(403).json({ error: "SQL_NOT_ALLOWED", message: "Apenas SELECT/WITH são permitidos." });
  }

  let pool;
  try {
    pool = await getPool();
  } catch (err) {
    return res.status(500).json({ error: "DB_CONNECT_FAILED", message: err.message });
  }

  const request = pool.request();
  request.stream = true; // ⭐ chave do streaming

  // Bind de parâmetros nomeados (@nome)
  if (params && typeof params === "object") {
    for (const [key, value] of Object.entries(params)) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
        return res.status(400).json({ error: "INVALID_PARAM_NAME", param: key });
      }
      request.input(key, value);
    }
  }

  const start = Date.now();
  let rowCount = 0;
  let aborted = false;
  let firstRow = true;
  let responded = false;

  // Inicia resposta JSON em streaming manualmente
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Transfer-Encoding", "chunked");
  res.write('{"success":true,"data":[');

  request.on("row", (row) => {
    if (aborted) return;

    if (rowCount >= rowCap) {
      aborted = true;
      console.warn(`[/query] ROW_LIMIT atingido (${rowCap}) source=${source} name=${queryName} — abortando`);
      try { request.cancel(); } catch (_) { /* noop */ }
      return;
    }

    try {
      const prefix = firstRow ? "" : ",";
      res.write(prefix + JSON.stringify(row));
      firstRow = false;
      rowCount++;
    } catch (err) {
      console.error("[/query] erro ao serializar row:", err.message);
      aborted = true;
      try { request.cancel(); } catch (_) { /* noop */ }
    }
  });

  request.on("error", (err) => {
    if (responded) return;
    responded = true;
    const duration = Date.now() - start;
    console.error(
      `[/query ERROR] source=${source || "?"} name=${queryName || "?"} rows=${rowCount} duration=${duration}ms`,
      err.message,
    );
    // Como já começamos a escrever, fecha o JSON com flag de erro nos meta
    try {
      res.write(`],"meta":{"count":${rowCount},"duration_ms":${duration},"error":${JSON.stringify(err.message)}}}`);
      res.end();
    } catch (_) {
      try { res.end(); } catch (_e) { /* noop */ }
    }
  });

  request.on("done", () => {
    if (responded) return;
    responded = true;
    const duration = Date.now() - start;
    console.log(
      `[/query] source=${source || "?"} name=${queryName || "?"} rows=${rowCount} duration=${duration}ms ${aborted ? "[CAPPED]" : ""}`,
    );
    res.write(
      `],"meta":{"count":${rowCount},"duration_ms":${duration},"source":${JSON.stringify(source || null)},"queryName":${JSON.stringify(queryName || null)}${aborted ? `,"truncated":true,"row_cap":${rowCap}` : ""}}}`,
    );
    res.end();
  });

  // Cliente cancelou (timeout do edge etc.) — aborta a query
  req.on("close", () => {
    if (!responded) {
      aborted = true;
      try { request.cancel(); } catch (_) { /* noop */ }
    }
  });

  try {
    request.query(rawSql);
  } catch (err) {
    if (responded) return;
    responded = true;
    console.error(`[/query SYNC ERROR] ${err.message}`);
    try {
      res.write(`],"meta":{"count":0,"error":${JSON.stringify(err.message)}}}`);
      res.end();
    } catch (_) { try { res.end(); } catch (_e) { /* noop */ } }
  }
});

// ───────────────────────────────────────────────
// Start + graceful shutdown
// ───────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[SERVER] SQL Server Bridge v1.1.0 rodando na porta ${PORT} (HARD_LIMIT_ROWS=${HARD_LIMIT_ROWS})`);
});

process.on("SIGTERM", async () => {
  if (pool) await pool.close();
  process.exit(0);
});
