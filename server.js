const express = require("express");

const app = express();

app.get("/", (req, res) => {
  res.status(200).json({ status: "ok" });
});

app.get("/ping", (req, res) => {
  res.json({ pong: true });
});

const PORT = process.env.PORT || 3000;

app.listen(process.env.PORT, () => {
  console.log("Servidor rodando na porta " + process.env.PORT);
});
