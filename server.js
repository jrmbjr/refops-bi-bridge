const express = require("express");
const sql = require("mssql");
const cors = require("cors");

const app = express();

app.use(cors({
  origin: "*",
  methods: ["GET", "POST"],
  allowedHeaders: ["Content-Type", "x-api-key"]
}));

app.use(express.json());

// ==============================
// API KEY (SEGURANÇA)
// ==============================
const API_KEY = process.env.BRIDGE_API_KEY;

function autenticar(req, res, next) {
  const key = req.headers["x-api-key"];

  if (!API_KEY) {
    return res.status(500).json({ error: "API KEY não configurada no servidor" });
  }

  if (!key) {
    return res.status(401).json({ error: "API KEY não enviada" });
  }

  if (key !== API_KEY) {
    return res.status(403).json({ error: "API KEY inválida" });
  }

  next();
}

// ==============================
// DEBUG DE VARIÁVEIS (TEMPORÁRIO)
// ==============================
console.log("PORT ENV:", process.env.PORT);

console.log("ENV:", {
  DB_SERVER: process.env.DB_SERVER,
  DB_USER: process.env.DB_USER,
  DB_NAME: process.env.DB_NAME
});

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
};

// ==============================
// POOL GLOBAL
// ==============================
let pool;

async function getPool() {
  if (!pool) {
    pool = await sql.connect(config);
  }
  return pool;
}

// ==============================
// ROTAS LIVRES (SEM AUTH)
// ==============================
app.get("/", (req, res) => {
  res.send("API Bridge rodando 🚀");
});

app.get("/teste-db", async (req, res) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query("SELECT GETDATE() AS data");
    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// LISTAR VIEWS (PROTEGIDO)
// ==============================
app.get("/views", autenticar, async (req, res) => {
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
// CONSULTAR VIEW DINÂMICA
// ==============================
app.get("/view/:nome", autenticar, async (req, res) => {
  try {
    const pool = await getPool();

    const nomeView = req.params.nome;

    const limit = parseInt(req.query.limit) || 100;
    const offset = parseInt(req.query.offset) || 0;

    // filtros dinâmicos
    const filtros = Object.keys(req.query)
      .filter((k) => !["limit", "offset"].includes(k))
      .map((k) => `${k} = @${k}`);

    let where = "";
    if (filtros.length > 0) {
      where = "WHERE " + filtros.join(" AND ");
    }

    const request = pool.request();

    Object.keys(req.query).forEach((key) => {
      if (!["limit", "offset"].includes(key)) {
        request.input(key, req.query[key]);
      }
    });

    const query = `
      SELECT *
      FROM ${nomeView}
      ${where}
      ORDER BY 1
      OFFSET ${offset} ROWS
      FETCH NEXT ${limit} ROWS ONLY
    `;

    const result = await request.query(query);

    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ==============================
// QUERY LIVRE (SOMENTE TESTE)
// ==============================
app.post("/query", autenticar, async (req, res) => {
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
