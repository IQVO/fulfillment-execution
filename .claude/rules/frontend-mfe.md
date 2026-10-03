---
paths:
  - "web/**"
---

# Frontend Micro-Frontend Remote (`web/`)

This repo also owns `web/`: `fulfillment-mfe`, a Vite + React Module
Federation **remote** consumed by the separate `warehouse-console` shell repo
(ADR-0013). It is a plain browser client of this service's own REST API
(queue-depth dashboard, task-by-orderRef lookup) — nothing in `web/` talks to
any other bounded context, and nothing in `internal/` knows `web/` exists.

- Own `package.json`, build, and dev server (`vite --port 5184`).
- Depends on `@warehouse/ui-kit` (file dependency on the sibling
  `warehouse-ui-kit` repo) and React 19 + `react-router-dom`.
- Does **not** participate in this repo's Go quality gate (`make check`) and
  is not part of the Go module.
- Lint: `oxlint` (not golangci-lint).

```bash
cd web && npm install
npm run dev      # :5184
npm run build     # tsc -b && vite build
npm run lint      # oxlint
```

CORS on the Go API (`CORS_ALLOWED_ORIGINS`, default includes
`http://localhost:5184`) is what makes local `web/` dev work against a local
`go run ./cmd/execution` — see `api-and-integration.md`.
