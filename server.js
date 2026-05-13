/**
 * SQL Server Bridge - HTTP API segura para consumo por Supabase Edge Functions.
 *
 * Endpoints:
 *   GET  /health
 *   POST /api/bi/query
 *   POST /query
 *   POST /refresh/renovacoes-inadimplencia
 *
 * Autenticacao:
 *   Header: x-api-key: <BRIDGE_API_KEY>
 */

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const sql = require("mssql");

const PORT = parseInt(process.env.PORT || "3000", 10);
const API_KEY = process.env.BRIDGE_API_KEY;
const REQUEST_TIMEOUT_MS = Math.min(
  Math.max(parseInt(process.env.SQLSERVER_REQUEST_TIMEOUT_MS || "60000", 10), 1000),
  600000,
);
const REFRESH_TIMEOUT_MS = Math.min(
  Math.max(parseInt(process.env.SQLSERVER_REFRESH_TIMEOUT_MS || "600000", 10), 1000),
  600000,
);
const BRIDGE_NAME = "sqlserver-bridge";

if (!API_KEY) {
  console.error("[FATAL] BRIDGE_API_KEY nao configurado no .env");
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
  requestTimeout: REQUEST_TIMEOUT_MS,
};

let pool;
let server;
let shuttingDown = false;
async function getPool() {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
    logStructured("info", {
      route: "startup",
      status: "connected",
      message: "SQL Server pool conectado",
    });
  }
  return pool;
}

function nowIso() {
  return new Date().toISOString();
}

function getRequestId(req) {
  return String(req.header("x-request-id") || cryptoRandom()).trim();
}

function getTraceId(req, requestId) {
  return String(req.header("x-trace-id") || requestId || cryptoRandom()).trim();
}

function getQueryName(req, fallback) {
  return String(req.header("x-query-name") || fallback || "unknown").trim();
}

function cryptoRandom() {
  return `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function logStructured(level, payload) {
  const entry = {
    timestamp: nowIso(),
    level,
    service: BRIDGE_NAME,
    ...payload,
  };
  const line = JSON.stringify(entry);
  if (level === "error") {
    console.error(line);
    return;
  }
  console.log(line);
}

function attachRequestContext(req, fallbackQueryName) {
  if (!req.bridgeContext) {
    const requestId = getRequestId(req);
    req.bridgeContext = {
      requestId,
      traceId: getTraceId(req, requestId),
      queryName: getQueryName(req, fallbackQueryName),
      startedAt: Date.now(),
    };
  } else if (fallbackQueryName && (!req.bridgeContext.queryName || req.bridgeContext.queryName === "unknown")) {
    req.bridgeContext.queryName = fallbackQueryName;
  }
  return req.bridgeContext;
}

function getContext(req, fallbackQueryName = "unknown") {
  return attachRequestContext(req, fallbackQueryName);
}

function durationSince(startedAt) {
  return Math.max(Date.now() - Number(startedAt || Date.now()), 0);
}

function parseWindows(input) {
  const raw = Array.isArray(input) ? input.join(",") : String(input || "90,180,365");
  const values = raw
    .split(",")
    .map((value) => parseInt(String(value).trim(), 10))
    .filter((value) => Number.isFinite(value))
    .filter((value) => value >= 30 && value <= 365);

  const unique = [...new Set(values)].sort((a, b) => a - b);
  return unique.length ? unique : null;
}

function classifySqlError(error) {
  const message = String(error && error.message ? error.message : "Unknown SQL error");
  const number = Number.isFinite(Number(error && error.number)) ? Number(error.number) : null;
  const code = error && error.code ? String(error.code) : "MSSQL_ERROR";
  const lower = message.toLowerCase();

  if (lower.includes("timeout") || lower.includes("request failed to complete")) {
    return { httpStatus: 504, retryable: true, code: "SQL_TIMEOUT", number, message };
  }
  if (number === 1205 || lower.includes("deadlock")) {
    return { httpStatus: 503, retryable: true, code: "SQL_DEADLOCK", number, message };
  }
  if (number === 1222 || lower.includes("lock request time out")) {
    return { httpStatus: 503, retryable: true, code: "SQL_LOCK_TIMEOUT", number, message };
  }
  if (code === "ETIMEOUT" || code === "ESOCKET" || code === "ECONNCLOSED" || code === "ENOCONN") {
    return { httpStatus: 503, retryable: true, code: "SQL_TRANSIENT", number, message };
  }
  return { httpStatus: 500, retryable: false, code, number, message };
}

function classifyAppError(error, req, upstreamTimeoutMs = REQUEST_TIMEOUT_MS) {
  const message = String(error && error.message ? error.message : "Unexpected application error");
  const code = error && error.code ? String(error.code) : "APP_ERROR";
  const lower = message.toLowerCase();

  if (error && error.type === "entity.parse.failed") {
    return { httpStatus: 400, retryable: false, code: "INVALID_JSON", number: null, message: "Invalid JSON body" };
  }
  if (error && error.type === "entity.too.large") {
    return { httpStatus: 400, retryable: false, code: "REQUEST_TOO_LARGE", number: null, message: "Request body too large" };
  }
  if (req && (req.aborted || lower.includes("request aborted") || code === "ECONNRESET")) {
    return { httpStatus: 499, retryable: true, code: "REQUEST_ABORTED", number: null, message: "Request aborted by client" };
  }
  return {
    ...classifySqlError(error),
    upstreamTimeoutMs,
  };
}

function sendError(res, {
  httpStatus,
  code,
  message,
  retryable,
  requestId,
  traceId,
  queryName,
  durationMs,
  upstreamTimeoutMs,
  sqlErrorNumber = null,
}) {
  return res.status(httpStatus).json({
    success: false,
    error: {
      code,
      message,
      retryable,
      number: sqlErrorNumber,
    },
    meta: {
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      upstream_timeout_ms: upstreamTimeoutMs,
    },
  });
}

function requireApiKey(req, res, next) {
  const { requestId, traceId, queryName, startedAt } = getContext(req, "auth");
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({
      success: false,
      error: {
        code: "UNAUTHORIZED",
        message: "x-api-key invalido",
        retryable: false,
      },
      meta: {
        request_id: requestId,
        trace_id: traceId,
        query_name: queryName,
        duration_ms: durationSince(startedAt),
      },
    });
  }
  next();
}

const ALLOWED_PREFIXES = ["vw_", "v_bi_", "v_gold_"];
const MAX_LIMIT = 5000;

function isViewAllowed(viewName) {
  if (typeof viewName !== "string") return false;
  if (!/^[A-Za-z0-9_]{1,128}$/.test(viewName)) return false;
  return ALLOWED_PREFIXES.some((prefix) => viewName.startsWith(prefix));
}

function isSqlAllowed(sqlText) {
  return typeof sqlText === "string" && /^\s*(WITH|SELECT|EXEC)\b/i.test(sqlText);
}

async function runSqlQuery(sqlText, params, timeoutMs) {
  const activePool = await getPool();
  const request = activePool.request();
  request.timeout = timeoutMs;
  for (const [key, value] of Object.entries(params || {})) {
    request.input(key, value);
  }
  return request.query(sqlText);
}

async function queryLatestRefreshLog() {
  const activePool = await getPool();
  const result = await activePool.request().query(`
    SELECT TOP 1
      Id,
      StartedAt,
      FinishedAt,
      DurationMs,
      RowsAggregate,
      RowsItens,
      Janelas,
      Status
    FROM dbo.bi_refresh_log
    WHERE Job = 'sp_refresh_renovacoes_inadimplencia'
    ORDER BY Id DESC
  `);
  return result.recordset[0] || null;
}

const app = express();
app.use(cors());
app.use((req, res, next) => {
  const ctx = attachRequestContext(req, "request");
  req.on("aborted", () => {
    logStructured("error", {
      route: req.path,
      request_id: ctx.requestId,
      trace_id: ctx.traceId,
      query_name: ctx.queryName,
      duration_ms: durationSince(ctx.startedAt),
      status: "aborted",
      error_code: "REQUEST_ABORTED",
      error_class: "operational",
      message: "Client aborted request",
    });
  });
  next();
});
app.use(express.json({ limit: "256kb" }));
app.use((error, req, res, next) => {
  if (!error) return next();
  const ctx = getContext(req, "json_parse");
  const classified = classifyAppError(error, req, REQUEST_TIMEOUT_MS);
  logStructured("error", {
    route: req.path,
    request_id: ctx.requestId,
    trace_id: ctx.traceId,
    query_name: ctx.queryName,
    duration_ms: durationSince(ctx.startedAt),
    status: "error",
    error_code: classified.code,
    error_class: "operational",
    sql_error_number: classified.number,
    message: classified.message,
  });
  if (res.headersSent || req.aborted) return;
  return sendError(res, {
    httpStatus: classified.httpStatus,
    code: classified.code,
    message: classified.message,
    retryable: classified.retryable,
    requestId: ctx.requestId,
    traceId: ctx.traceId,
    queryName: ctx.queryName,
    durationMs: durationSince(ctx.startedAt),
    upstreamTimeoutMs: classified.upstreamTimeoutMs ?? REQUEST_TIMEOUT_MS,
    sqlErrorNumber: classified.number,
  });
});

app.get("/health", async (_req, res) => {
  try {
    const activePool = await getPool();
    const result = await activePool.request().query("SELECT 1 AS ok");
    res.json({ status: "ok", db: result.recordset[0].ok === 1 });
  } catch (error) {
    res.status(500).json({
      status: "error",
      error: error.message,
    });
  }
});

app.post("/api/bi/query", requireApiKey, async (req, res) => {
  const { view, limit } = req.body || {};
  const { requestId, traceId, queryName, startedAt } = getContext(req, "api_bi_query");

  if (!isViewAllowed(view)) {
    return sendError(res, {
      httpStatus: 400,
      code: "VIEW_NOT_ALLOWED",
      message: `View nao permitida: ${String(view || "")}`,
      retryable: false,
      requestId,
      traceId,
      queryName,
      durationMs: Date.now() - startedAt,
      upstreamTimeoutMs: REQUEST_TIMEOUT_MS,
    });
  }

  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 1000, 1), MAX_LIMIT);

  try {
    const sqlText = `SELECT TOP (${safeLimit}) * FROM [${view}]`;
    const result = await runSqlQuery(sqlText, {}, REQUEST_TIMEOUT_MS);
    const durationMs = Date.now() - startedAt;
    logStructured("info", {
      route: "/api/bi/query",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows: result.recordset.length,
      status: "success",
      view,
    });
    return res.json({
      success: true,
      view,
      data: result.recordset,
      meta: {
        count: result.recordset.length,
        duration_ms: durationMs,
        request_id: requestId,
        trace_id: traceId,
        query_name: queryName,
      },
    });
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const classified = classifySqlError(error);
    logStructured("error", {
      route: "/api/bi/query",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      status: "error",
      error_code: classified.code,
      sql_error_number: classified.number,
      message: classified.message,
    });
    return sendError(res, {
      httpStatus: classified.httpStatus,
      code: classified.code,
      message: classified.message,
      retryable: classified.retryable,
      requestId,
      traceId,
      queryName,
      durationMs,
      upstreamTimeoutMs: REQUEST_TIMEOUT_MS,
      sqlErrorNumber: classified.number,
    });
  }
});

app.post("/query", requireApiKey, async (req, res) => {
  const { sql: sqlText, params = {}, timeoutMs } = req.body || {};
  const { requestId, traceId, queryName, startedAt } = getContext(req, "query");
  const effectiveTimeoutMs = Math.min(Math.max(Number(timeoutMs) || REQUEST_TIMEOUT_MS, 1000), 600000);

  if (!isSqlAllowed(sqlText)) {
    return sendError(res, {
      httpStatus: 400,
      code: "INVALID_SQL",
      message: "Only SELECT/WITH/EXEC allowed",
      retryable: false,
      requestId,
      traceId,
      queryName,
      durationMs: Date.now() - startedAt,
      upstreamTimeoutMs: effectiveTimeoutMs,
    });
  }

  try {
    const result = await runSqlQuery(sqlText, params, effectiveTimeoutMs);
    const durationMs = Date.now() - startedAt;
    logStructured("info", {
      route: "/query",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows: result.recordset.length,
      status: "success",
    });
    return res.status(200).json({
      success: true,
      rows: result.recordset,
      meta: {
        rowsAffected: result.rowsAffected,
        duration_ms: durationMs,
        request_id: requestId,
        trace_id: traceId,
        query_name: queryName,
        upstream_timeout_ms: effectiveTimeoutMs,
      },
    });
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const classified = classifySqlError(error);
    logStructured("error", {
      route: "/query",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows: 0,
      status: "error",
      error_code: classified.code,
      sql_error_number: classified.number,
      message: classified.message,
    });
    return sendError(res, {
      httpStatus: classified.httpStatus,
      code: classified.code,
      message: classified.message,
      retryable: classified.retryable,
      requestId,
      traceId,
      queryName,
      durationMs,
      upstreamTimeoutMs: effectiveTimeoutMs,
      sqlErrorNumber: classified.number,
    });
  }
});

app.post("/refresh/renovacoes-inadimplencia", requireApiKey, async (req, res) => {
  const { requestId, traceId, queryName, startedAt } = getContext(req, "refresh_renovacoes_inadimplencia");
  const windows = parseWindows(req.body && (req.body.windows || req.body.janelas));

  if (!windows) {
    return sendError(res, {
      httpStatus: 400,
      code: "INVALID_WINDOWS",
      message: "Informe janelas validas entre 30 e 365 dias",
      retryable: false,
      requestId,
      traceId,
      queryName,
      durationMs: Date.now() - startedAt,
      upstreamTimeoutMs: REFRESH_TIMEOUT_MS,
    });
  }

  try {
    await runSqlQuery(
      "EXEC dbo.sp_refresh_renovacoes_inadimplencia @Janelas = @Janelas",
      { Janelas: windows.join(",") },
      REFRESH_TIMEOUT_MS,
    );

    const latestLog = await queryLatestRefreshLog();
    const durationMs = Date.now() - startedAt;
    const refreshAt = latestLog && latestLog.FinishedAt
      ? new Date(latestLog.FinishedAt).toISOString()
      : nowIso();
    const rowsAggregate = Number(latestLog && latestLog.RowsAggregate ? latestLog.RowsAggregate : 0);
    const rowsItems = Number(latestLog && latestLog.RowsItens ? latestLog.RowsItens : 0);
    const logId = latestLog && latestLog.Id ? Number(latestLog.Id) : null;

    logStructured("info", {
      route: "/refresh/renovacoes-inadimplencia",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows_aggregate: rowsAggregate,
      rows_items: rowsItems,
      windows: windows.join(","),
      log_id: logId,
      status: "success",
    });

    return res.status(200).json({
      success: true,
      rows_aggregate: rowsAggregate,
      rows_items: rowsItems,
      refresh_at: refreshAt,
      duration_ms: durationMs,
      log_id: logId,
      windows: windows.join(","),
      request_id: requestId,
      trace_id: traceId,
      retryable: false,
      query_name: queryName,
      upstream_timeout_ms: REFRESH_TIMEOUT_MS,
    });
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const classified = classifySqlError(error);
    logStructured("error", {
      route: "/refresh/renovacoes-inadimplencia",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows_aggregate: 0,
      rows_items: 0,
      windows: windows.join(","),
      status: "error",
      error_code: classified.code,
      sql_error_number: classified.number,
      message: classified.message,
    });
    return sendError(res, {
      httpStatus: classified.httpStatus,
      code: classified.code,
      message: classified.message,
      retryable: classified.retryable,
      requestId,
      traceId,
      queryName,
      durationMs,
      upstreamTimeoutMs: REFRESH_TIMEOUT_MS,
      sqlErrorNumber: classified.number,
    });
  }
});

app.use((req, res) => {
  const { requestId, traceId, queryName, startedAt } = getContext(req, "not_found");
  return res.status(404).json({
    success: false,
    error: {
      code: "NOT_FOUND",
      message: `Cannot ${req.method} ${req.path}`,
      retryable: false,
    },
    meta: {
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationSince(startedAt),
    },
  });
});

app.use((error, req, res, _next) => {
  const { requestId, traceId, queryName, startedAt } = getContext(req, "express_error");
  const classified = classifyAppError(error, req, REQUEST_TIMEOUT_MS);
  logStructured("error", {
    route: req.path,
    request_id: requestId,
    trace_id: traceId,
    query_name: queryName,
    duration_ms: durationSince(startedAt),
    status: "error",
    error_code: classified.code,
    error_class: classified.httpStatus >= 500 ? "fatal" : "operational",
    sql_error_number: classified.number,
    message: classified.message,
  });
  if (res.headersSent || req.aborted) return;
  return sendError(res, {
    httpStatus: classified.httpStatus,
    code: classified.code,
    message: classified.message,
    retryable: classified.retryable,
    requestId,
    traceId,
    queryName,
    durationMs: durationSince(startedAt),
    upstreamTimeoutMs: classified.upstreamTimeoutMs ?? REQUEST_TIMEOUT_MS,
    sqlErrorNumber: classified.number,
  });
});

async function closePoolSafely() {
  if (!pool) return;
  try {
    await pool.close();
  } catch (error) {
    logStructured("error", {
      route: "shutdown",
      status: "error",
      error_code: "POOL_CLOSE_FAILED",
      error_class: "operational",
      message: error.message,
    });
  } finally {
    pool = null;
  }
}

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logStructured("info", {
    route: "shutdown",
    status: "started",
    signal,
  });
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
  await closePoolSafely();
  logStructured("info", {
    route: "shutdown",
    status: "completed",
    signal,
  });
  process.exit(0);
}

server = app.listen(PORT, () => {
  logStructured("info", {
    route: "startup",
    status: "listening",
    port: PORT,
  });
});

process.on("unhandledRejection", (reason) => {
  const error = reason instanceof Error ? reason : new Error(String(reason));
  const classified = classifyAppError(error, null, REQUEST_TIMEOUT_MS);
  logStructured("error", {
    route: "process",
    status: "error",
    error_code: classified.code,
    error_class: "fatal",
    sql_error_number: classified.number,
    message: classified.message,
    event: "unhandledRejection",
  });
});

process.on("uncaughtException", (error) => {
  const classified = classifyAppError(error, null, REQUEST_TIMEOUT_MS);
  logStructured("error", {
    route: "process",
    status: "error",
    error_code: classified.code,
    error_class: "fatal",
    sql_error_number: classified.number,
    message: classified.message,
    event: "uncaughtException",
  });
});

process.on("SIGTERM", () => {
  void gracefulShutdown("SIGTERM");
});

process.on("SIGINT", () => {
  void gracefulShutdown("SIGINT");
});
