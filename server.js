const express = require("express");

const app = express();

app.get("/", (req, res) => {
  res.send("API funcionando");
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

const PORT = process.env.PORT;

app.listen(PORT, () => {
  console.log("rodando na porta", PORT);
});

app.get("/", (req, res) => {
  res.send("RefOps BI Bridge rodando");
});
