# PharmaCo Manufacturing — Real-Time Line Monitoring (Frontend)

React + Vite + TypeScript + TailwindCSS UI for the PharmaCo manufacturing
real-time monitoring Databricks App. **All data shown is synthetic.**

The FastAPI backend (in `app/`) serves this app's built assets from
`frontend/dist/` (`index.html` + the `/assets` mount) and exposes the JSON API
under `/api/*` plus `/health` and `/ready`.

## Stack

- Vite + React 18 + TypeScript
- TailwindCSS 3 (system font stack, everything bundled — no runtime CDN/font URLs)
- `recharts` for charts, `lucide-react` for icons
- Native `fetch`; state via React hooks (polling)

## Views

- **Plant Overview** — plant status, KPI tiles, 3 line cards
- **Line View** — line summary, throughput-by-asset chart, asset grid
- **Asset Detail** — sensor readings vs nominal bands, telemetry chart (sensor
  selector + shaded nominal band), anomaly meter, active alerts
- **Alert Console** — sortable/filterable table, acknowledge/resolve modals,
  expandable per-alert history
- **Scenario Controls** — start/stop generator, scenario picker (with a
  confirmation dialog for the high-impact `alarm_burst` / `line_slowdown`
  scenarios), rate slider, anomaly duration
- **Architecture & Observability** — pipeline flow, Delta record counts,
  latencies (ingest / pipeline / Lakebase serving shown separately), streaming
  job status, 5-min data quality

## Development

The backend must be running locally on `http://127.0.0.1:8077`.

```bash
cd app/frontend
npm install
npm run dev
```

`npm run dev` starts Vite on http://localhost:5173 and proxies `/api`,
`/health`, and `/ready` to `http://127.0.0.1:8077` (see `vite.config.ts`).

## Build

```bash
cd app/frontend
npm run build
```

Outputs to `app/frontend/dist/` (with `base: './'` so assets resolve under the
app origin). The FastAPI app auto-serves `dist/` when present. To verify the
production bundle end-to-end, build and then load the FastAPI app at
`http://127.0.0.1:8077/`.

## Notes / configuration

- API base URL is the **same origin** — all requests use relative paths
  (`/api/...`). No hostnames are hardcoded.
- Global refresh interval (2s / 5s / 10s / 30s) and operator name persist in
  `localStorage`.
- The UI distinguishes **data freshness** (age of the latest event / sync
  timestamp) from **API response latency**, and shows a stale-data banner when
  the newest event is older than ~60s.
- Permission (401/403) and service-unavailable (503 / network) responses render
  friendly panels rather than crashing.
