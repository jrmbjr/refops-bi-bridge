# SQL Server Bridge — Blue Fleet BI

HTTP bridge segura para o edge function `bluefleet-bi-sync` consultar SQL Server.

## Setup local

```bash
npm install
cp .env.example .env
# editar .env com credenciais reais
npm start
```

## Deploy recomendado

- **Railway / Render / Fly.io** — deploy direto do Node.js
- **VPS própria** — `pm2 start server.js --name bridge`
- **Docker** — adicionar Dockerfile baseado em `node:20-alpine`

## Endpoints

### `GET /health`
Verifica conexão com SQL Server.

### `POST /api/bi/query`
Headers: `x-api-key: <BRIDGE_API_KEY>`
Body:
```json
{ "view": "vw_FaturamentoPendente", "limit": 1000 }
```

## Segurança

- Apenas views com prefixo `vw_`, `v_bi_`, `v_gold_` são aceitas
- Limite máximo: 5000 linhas
- Query parametrizada (sem SQL injection — view name validada por regex)
- Auth via header `x-api-key`

## Configurar no Lovable

Após deploy, adicionar os secrets no projeto Lovable:
- `SQLSERVER_BRIDGE_URL` = `https://sua-bridge.com`
- `SQLSERVER_BRIDGE_KEY` = mesmo valor de `BRIDGE_API_KEY`
