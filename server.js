require("dotenv").config();

const express = require("express");
const cors = require("cors");
const sql = require("mssql");

const app = express();

// ==============================
// MIDDLEWARES
// ==============================
app.use(cors());
app.use(express.json());

// ==============================
// CONFIG SQL SERVER
// ==============================
const dbConfig = {
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

// ==============================
// CONEXÃO COM BANCO
// ==============================
async function connectDB() {
  try {
    pool = await sql.connect(dbConfig);
    console.log("✅ Conectado ao SQL Server");
  } catch (err) {
    console.error("❌ Erro ao conectar no banco:", err);
    process.exit(1);
  }
}

// ==============================
// AUTH API KEY
// ==============================
function autenticar(req, res, next) {
  const apiKey = req.headers["x-api-key"];

  if (!apiKey || apiKey !== process.env.API_KEY) {
    return res.status(401).json({
      error: "Não autorizado",
    });
  }

  next();
}

// ==============================
// HEALTH CHECK
// ==============================
app.get("/", (req, res) => {
  res.send("🚀 Bridge API rodando");
});

// ==============================
// LISTA DE VIEWS PERMITIDAS
// ==============================
const allowedViews = [
  "vw_bi_frota",
  "vw_bi_contratos",
  "vw_bi_multas",
];

// ==============================
// ENDPOINT DINÂMICO DE VIEWS
// ==============================
app.get("/views/:viewName", autenticar, async (req, res) => {
  const { viewName } = req.params;

  // segurança contra injection
  if (!allowedViews.includes(viewName)) {
    return res.status(403).json({
      error: "View não permitida",
    });
  }

  try {
    const result = await pool
      .request()
      .query(`SELECT TOP 1000 * FROM ${viewName}`);

    res.json({
      success: true,
      data: result.recordset,
      total: result.recordset.length,
    });
  } catch (err) {
    console.error("Erro na query:", err);

    res.status(500).json({
      error: "Erro ao consultar a view",
      details: err.message,
    });
  }
});

// ==============================
// START SERVER
// ==============================
const PORT = process.env.PORT || 3000;

connectDB().then(() => {
  app.listen(PORT, () => {
    console.log(`🚀 API rodando na porta ${PORT}`);
  });
});
