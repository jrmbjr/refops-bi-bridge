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
 *   Header: x-api-key: <r5403zmeyqri80ueu77lpht4ircchcnf>
 */

require("dotenv").config();
const { AsyncLocalStorage } = require("async_hooks");
const express = require("express");
const cors = require("cors");
const sql = require("mssql");
const pLimit = require("p-limit");
const NodeCache = require("node-cache");

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
const SQLSERVER_POOL_MAX = Math.max(parseInt(process.env.SQLSERVER_POOL_MAX || "20", 10), 1);
const SQLSERVER_POOL_MIN = Math.min(SQLSERVER_POOL_MAX, Math.max(parseInt(process.env.SQLSERVER_POOL_MIN || "2", 10), 0));
const SQLSERVER_POOL_IDLE_TIMEOUT_MS = Math.max(parseInt(process.env.SQLSERVER_POOL_IDLE_TIMEOUT_MS || "30000", 10), 1000);
const SQLSERVER_POOL_ACQUIRE_TIMEOUT_MS = Math.max(parseInt(process.env.SQLSERVER_POOL_ACQUIRE_TIMEOUT_MS || "30000", 10), 1000);
const SQLSERVER_QUERY_CONCURRENCY = Math.max(parseInt(process.env.SQLSERVER_QUERY_CONCURRENCY || "5", 10), 1);
const SQLSERVER_QUERY_CACHE_TTL_SECONDS = Math.max(parseInt(process.env.SQLSERVER_QUERY_CACHE_TTL_SECONDS || "60", 10), 0);
const SQLSERVER_QUERY_CACHE_MAX_KEYS = Math.max(parseInt(process.env.SQLSERVER_QUERY_CACHE_MAX_KEYS || "300", 10), 1);
const CACHEABLE_QUERY_TOKENS = ["contratos-renovacao", "renovacoes", "inadimplencia"];

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
  pool: {
    max: SQLSERVER_POOL_MAX,
    min: SQLSERVER_POOL_MIN,
    idleTimeoutMillis: SQLSERVER_POOL_IDLE_TIMEOUT_MS,
    acquireTimeoutMillis: SQLSERVER_POOL_ACQUIRE_TIMEOUT_MS,
  },
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
const requestContextStorage = new AsyncLocalStorage();
const queryLimiter = pLimit(SQLSERVER_QUERY_CONCURRENCY);
const queryCache = new NodeCache({
  stdTTL: SQLSERVER_QUERY_CACHE_TTL_SECONDS,
  checkperiod: Math.max(Math.min(SQLSERVER_QUERY_CACHE_TTL_SECONDS, 60), 1),
  useClones: false,
  maxKeys: SQLSERVER_QUERY_CACHE_MAX_KEYS,
});
async function getPool() {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
    logStructured("info", {
      route: "startup",
      status: "connected",
      message: "SQL Server pool conectado",
      pool: getPoolStats(),
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

function getAsyncContext() {
  return requestContextStorage.getStore() || null;
}

function getPoolStats() {
  const stats = {
    configured_max: SQLSERVER_POOL_MAX,
    configured_min: SQLSERVER_POOL_MIN,
    acquire_timeout_ms: SQLSERVER_POOL_ACQUIRE_TIMEOUT_MS,
    connected: Boolean(pool && pool.connected),
  };
  const internalPool = pool && pool.pool;
  if (!internalPool) return stats;
  if (typeof internalPool.numUsed === "function") stats.used = internalPool.numUsed();
  if (typeof internalPool.numFree === "function") stats.free = internalPool.numFree();
  if (typeof internalPool.numPendingAcquires === "function") stats.pending_acquires = internalPool.numPendingAcquires();
  if (typeof internalPool.numPendingCreates === "function") stats.pending_creates = internalPool.numPendingCreates();
  return stats;
}

function getLimiterStats() {
  return {
    concurrency: SQLSERVER_QUERY_CONCURRENCY,
    active: queryLimiter.activeCount,
    pending: queryLimiter.pendingCount,
  };
}

function getStartupConfig() {
  return {
    node_env: process.env.NODE_ENV || "development",
    port: PORT,
    sqlserver_request_timeout_ms: REQUEST_TIMEOUT_MS,
    sqlserver_refresh_timeout_ms: REFRESH_TIMEOUT_MS,
    sqlserver_pool_max: SQLSERVER_POOL_MAX,
    sqlserver_pool_min: SQLSERVER_POOL_MIN,
    sqlserver_query_concurrency: SQLSERVER_QUERY_CONCURRENCY,
    sqlserver_query_cache_ttl_seconds: SQLSERVER_QUERY_CACHE_TTL_SECONDS,
    sqlserver_query_cache_max_keys: SQLSERVER_QUERY_CACHE_MAX_KEYS,
  };
}

function buildOperationalLogFields(route, queryName, durationMs, cacheStatus) {
  const poolStats = getPoolStats();
  const limiterStats = getLimiterStats();
  return {
    route,
    query_name: queryName,
    duration_ms: durationMs,
    cache: cacheStatus,
    limiter: limiterStats,
    pool: poolStats,
    "limiter.active": limiterStats.active,
    "limiter.pending": limiterStats.pending,
    "pool.connected": poolStats.connected,
    "pool.configured_max": poolStats.configured_max,
    "pool.configured_min": poolStats.configured_min,
  };
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
  metaExtras = null,
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
      ...(metaExtras || {}),
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

function isCacheableSql(sqlText) {
  return typeof sqlText === "string" && /^\s*(WITH|SELECT)\b/i.test(sqlText);
}

function shouldCacheQuery(sqlText, queryName) {
  if (!SQLSERVER_QUERY_CACHE_TTL_SECONDS || !isCacheableSql(sqlText)) return false;
  const normalized = String(queryName || "").toLowerCase();
  return CACHEABLE_QUERY_TOKENS.some((token) => normalized.includes(token));
}

function buildQueryCacheKey(sqlText, params, queryName) {
  return JSON.stringify({
    sql: sqlText,
    params: params || {},
    query_name: String(queryName || "").toLowerCase(),
  });
}

function getCacheStatus(cacheEnabled, cacheHit) {
  if (!cacheEnabled) return "bypass";
  return cacheHit ? "hit" : "miss";
}

async function runSqlQuery(sqlText, params, timeoutMs, req = null) {
  const activePool = await getPool();
  const request = activePool.request();
  request.timeout = timeoutMs;
  let bridgeTimedOut = false;
  let clientAborted = false;
  let cancelIssued = false;

  const cancelRequest = () => {
    if (cancelIssued) return;
    cancelIssued = true;
    try {
      request.cancel();
    } catch (_error) {
      // Best effort cancellation only.
    }
  };

  const onAbort = () => {
    clientAborted = true;
    cancelRequest();
  };

  const onClose = () => {
    if (req && req.aborted) onAbort();
  };

  const timeoutHandle = setTimeout(() => {
    bridgeTimedOut = true;
    cancelRequest();
  }, timeoutMs + 50);

  if (req) {
    req.on("aborted", onAbort);
    req.on("close", onClose);
  }

  for (const [key, value] of Object.entries(params || {})) {
    request.input(key, value);
  }
  try {
    return await request.query(sqlText);
  } catch (error) {
    if (bridgeTimedOut) {
      error.code = "ETIMEOUT";
      error.message = `Request failed to complete in ${timeoutMs}ms`;
    } else if (clientAborted) {
      error.code = "ECONNRESET";
      error.message = "Request aborted by client";
    }
    throw error;
  } finally {
    clearTimeout(timeoutHandle);
    if (req) {
      req.removeListener("aborted", onAbort);
      req.removeListener("close", onClose);
    }
  }
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
  const asyncContext = {
    route: req.path,
    requestId: ctx.requestId,
    traceId: ctx.traceId,
    queryName: ctx.queryName,
    startedAt: ctx.startedAt,
  };
  return requestContextStorage.run(asyncContext, () => {
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
    const result = await runSqlQuery(sqlText, {}, REQUEST_TIMEOUT_MS, req);
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
      pool: getPoolStats(),
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
    const classified = classifyAppError(error, req, REQUEST_TIMEOUT_MS);
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
      pool: getPoolStats(),
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
  const cacheEnabled = shouldCacheQuery(sqlText, queryName);
  const cacheKey = cacheEnabled ? buildQueryCacheKey(sqlText, params, queryName) : null;

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
      metaExtras: { cache: "bypass" },
    });
  }

  try {
    let result;
    let cacheStatus = "bypass";
    console.log("[query:start]", queryName, requestId);
    if (cacheEnabled) {
      const cachedResult = queryCache.get(cacheKey);
      if (cachedResult) {
        result = cachedResult;
        cacheStatus = "hit";
      } else {
        cacheStatus = "miss";
      }
    }

    if (!result) {
      result = await queryLimiter(async () => {
        if (req.aborted) {
          const abortError = new Error("Request aborted by client");
          abortError.code = "ECONNRESET";
          throw abortError;
        }
        return runSqlQuery(sqlText, params, effectiveTimeoutMs, req);
      });
      if (cacheEnabled) {
        queryCache.set(cacheKey, {
          recordset: result.recordset,
          rowsAffected: result.rowsAffected,
        });
      }
    }

    const durationMs = Date.now() - startedAt;
    logStructured("info", {
      route: "/query",
      request_id: requestId,
      trace_id: traceId,
      query_name: queryName,
      duration_ms: durationMs,
      rows: result.recordset.length,
      status: "success",
      cache: cacheStatus,
      pool: getPoolStats(),
      limiter: getLimiterStats(),
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
        cache: cacheStatus,
      },
    });
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const classified = classifyAppError(error, req, effectiveTimeoutMs);
    const cacheStatus = getCacheStatus(cacheEnabled, false);
    const operationalFields = buildOperationalLogFields("/query", queryName, durationMs, cacheStatus);
    console.log("[query:error]", queryName, requestId, durationMs, classified.code);
    logStructured("error", {
      request_id: requestId,
      trace_id: traceId,
      rows: 0,
      status: "error",
      error_code: classified.code,
      sql_error_number: classified.number,
      message: classified.message,
      ...operationalFields,
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
      metaExtras: { cache: cacheStatus },
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
      req,
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
      pool: getPoolStats(),
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
    const classified = classifyAppError(error, req, REFRESH_TIMEOUT_MS);
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
      pool: getPoolStats(),
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
    config: getStartupConfig(),
  });
  console.log("[startup] sqlserver-bridge listening", JSON.stringify(getStartupConfig()));
});

process.on("unhandledRejection", (reason) => {
  const error = reason instanceof Error ? reason : new Error(String(reason));
  const classified = classifyAppError(error, null, REQUEST_TIMEOUT_MS);
  const asyncContext = getAsyncContext();
  const durationMs = asyncContext ? durationSince(asyncContext.startedAt) : 0;
  const route = asyncContext && asyncContext.route ? asyncContext.route : "process";
  const queryName = asyncContext && asyncContext.queryName ? asyncContext.queryName : "unknown";
  const operationalFields = buildOperationalLogFields(route, queryName, durationMs, "bypass");
  logStructured("error", {
    route,
    status: "error",
    error_code: classified.code,
    error_class: "fatal",
    sql_error_number: classified.number,
    message: classified.message,
    event: "unhandledRejection",
    request_id: asyncContext ? asyncContext.requestId : undefined,
    trace_id: asyncContext ? asyncContext.traceId : undefined,
    ...operationalFields,
  });
});

process.on("uncaughtException", (error) => {
  const classified = classifyAppError(error, null, REQUEST_TIMEOUT_MS);
  const asyncContext = getAsyncContext();
  const durationMs = asyncContext ? durationSince(asyncContext.startedAt) : 0;
  const route = asyncContext && asyncContext.route ? asyncContext.route : "process";
  const queryName = asyncContext && asyncContext.queryName ? asyncContext.queryName : "unknown";
  const operationalFields = buildOperationalLogFields(route, queryName, durationMs, "bypass");
  logStructured("error", {
    route,
    status: "error",
    error_code: classified.code,
    error_class: "fatal",
    sql_error_number: classified.number,
    message: classified.message,
    event: "uncaughtException",
    request_id: asyncContext ? asyncContext.requestId : undefined,
    trace_id: asyncContext ? asyncContext.traceId : undefined,
    ...operationalFields,
  });
});

process.on("SIGTERM", () => {
  void gracefulShutdown("SIGTERM");
});

process.on("SIGINT", () => {
  void gracefulShutdown("SIGINT");
});
