const express = require("express");

const app = express();

app.get("/", (req, res) => {
  res.status(200).json({ status: "ok" });
});

app.get("/ping", (req, res) => {
  res.json({ pong: true });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando na porta " + PORT);
});
