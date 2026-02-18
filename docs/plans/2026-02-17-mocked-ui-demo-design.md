# Mocked UI Demo for GitHub Pages

**Date:** 2026-02-17
**Status:** Approved

## Goal

Create an interactive, simulated version of the embedded web UI dashboard that runs on GitHub Pages with no backend. The demo is embedded in the existing Hugo docs site via an iframe and uses fake data with periodic simulation to feel alive.

## Architecture

### Client Swap via Vite Env Variable

The existing `ui/frontend/src/lib/client.ts` creates a Connect-RPC client. For the demo build, we conditionally export a mock client instead, based on `import.meta.env.VITE_DEMO_MODE`.

All Svelte components remain unchanged. Only the data layer is swapped.

### Mock Client (`ui/frontend/src/lib/mock-client.ts`)

Implements the same interface as the real Connect-RPC client using in-memory data.

**Mock data:**
- 3 queues: `default`, `emails`, `critical`
- ~30 jobs across all statuses (pending, running, completed, failed)
- 4 scheduled jobs with cron, daily, weekly, and interval patterns
- Realistic job types: `send-email`, `process-order`, `generate-report`, `sync-inventory`, `charge-payment`

**Simulation (every 3 seconds):**
- Transition 1-2 pending jobs to running
- Complete 1-2 running jobs
- Occasionally fail a running job (~10% chance)
- Add new pending jobs to maintain pipeline flow
- Update stats counters and generate chart data points

**Actions work locally:** retry, delete, and purge modify the in-memory mock state.

### Build Configuration

- New npm script: `"build:demo": "VITE_DEMO_MODE=true vite build --outDir demo-dist --base /simple-durable-jobs/demo/"`
- `--base` ensures asset paths work under the GitHub Pages subpath
- Output: `demo-dist/`

### Hugo Integration

- New page: `docs/content/docs/live-demo.md` with an iframe pointing to `/simple-durable-jobs/demo/`
- Banner inside the demo indicates "Interactive demo with simulated data"

### GitHub Actions (`pages.yml`)

Add steps before Hugo build:
1. Setup Node.js
2. `cd ui/frontend && npm ci && npm run build:demo`
3. `cp -r ui/frontend/demo-dist docs/static/demo`

Hugo picks up `docs/static/demo/` automatically.

## Files Changed/Created

| File | Action |
|------|--------|
| `ui/frontend/src/lib/mock-client.ts` | New: mock data + simulation engine |
| `ui/frontend/src/lib/client.ts` | Edit: conditional export for demo mode |
| `ui/frontend/package.json` | Edit: add `build:demo` script |
| `docs/content/docs/live-demo.md` | New: Hugo page with iframe |
| `.github/workflows/pages.yml` | Edit: add Node.js + demo build step |
