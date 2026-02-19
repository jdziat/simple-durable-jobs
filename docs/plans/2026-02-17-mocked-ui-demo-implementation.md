# Mocked UI Demo Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an interactive demo of the Jobs UI dashboard with simulated data, embedded in the Hugo docs site on GitHub Pages.

**Architecture:** Swap the Connect-RPC client with a mock client that serves in-memory data and simulates job activity via timers. The same Svelte components render unchanged. A separate Vite build produces static assets deployed alongside the Hugo docs.

**Tech Stack:** Svelte 5, Vite, protobuf-es message classes, Chart.js, Hugo, GitHub Actions

---

### Task 1: Create Mock Client with Static Data

**Files:**
- Create: `ui/frontend/src/lib/mock-client.ts`

**Step 1: Create the mock data module**

Create `ui/frontend/src/lib/mock-client.ts` with the full mock implementation. This file:

1. Defines in-memory arrays of mock jobs, queues, and scheduled jobs
2. Exports a `mockJobsClient` object with the same method signatures the Svelte components call on `jobsClient`
3. Includes a simulation engine that periodically mutates the mock data

The methods used by components (from grep):
- `getStats({})` -> `GetStatsResponse`
- `getStatsHistory({ period, queue })` -> `GetStatsHistoryResponse`
- `listJobs({ status, queue, type, search, page, limit })` -> `ListJobsResponse`
- `getJob({ id })` -> `GetJobResponse`
- `retryJob({ id })` -> `RetryJobResponse`
- `deleteJob({ id })` -> `DeleteJobResponse`
- `listQueues({})` -> `ListQueuesResponse`
- `purgeQueue({ name, status })` -> `PurgeQueueResponse`
- `listScheduledJobs({})` -> `ListScheduledJobsResponse`

```typescript
import { Timestamp } from '@bufbuild/protobuf'
import {
  GetStatsResponse, GetStatsHistoryResponse, ListJobsResponse,
  GetJobResponse, RetryJobResponse, DeleteJobResponse,
  ListQueuesResponse, PurgeQueueResponse, ListScheduledJobsResponse,
  QueueStats, Job, Checkpoint, DataPoint, ScheduledJobInfo,
} from './gen/jobs/v1/jobs_pb'
import { protoInt64 } from '@bufbuild/protobuf'

// --- Helpers ---

function uuid(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16)
  })
}

function tsFromDate(d: Date): Timestamp {
  return Timestamp.fromDate(d)
}

function minutesAgo(n: number): Date {
  return new Date(Date.now() - n * 60_000)
}

function randomPick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

function jsonBytes(obj: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(obj))
}

// --- Mock Data ---

const JOB_TYPES = [
  { type: 'send-email', queue: 'emails', args: { to: 'user@example.com', subject: 'Welcome!' } },
  { type: 'process-order', queue: 'default', args: { orderId: 'ORD-1042', items: 3 } },
  { type: 'generate-report', queue: 'default', args: { reportType: 'monthly', format: 'pdf' } },
  { type: 'sync-inventory', queue: 'critical', args: { warehouseId: 'WH-03', sku: 'WIDGET-A' } },
  { type: 'charge-payment', queue: 'critical', args: { amount: 49.99, currency: 'USD' } },
  { type: 'send-notification', queue: 'emails', args: { channel: 'push', userId: 'u_8f3k' } },
  { type: 'resize-image', queue: 'default', args: { width: 800, height: 600, format: 'webp' } },
  { type: 'cleanup-expired', queue: 'default', args: { olderThanDays: 30 } },
]

type MockJob = {
  id: string; type: string; queue: string; status: string
  priority: number; attempt: number; maxRetries: number
  args: Uint8Array; lastError: string
  createdAt: Date; startedAt: Date | null; completedAt: Date | null
}

const mockJobs: MockJob[] = []
const statsHistory: { timestamp: Date; completed: number; failed: number; queue: string }[] = []

// Seed initial jobs
function seedJobs() {
  const statuses = ['completed', 'completed', 'completed', 'completed', 'completed',
                    'running', 'running', 'pending', 'pending', 'pending', 'failed']
  for (let i = 0; i < 30; i++) {
    const template = randomPick(JOB_TYPES)
    const status = randomPick(statuses)
    const createdAt = minutesAgo(Math.floor(Math.random() * 120))
    const startedAt = status !== 'pending' ? new Date(createdAt.getTime() + Math.random() * 30_000) : null
    const completedAt = status === 'completed' ? new Date((startedAt?.getTime() ?? createdAt.getTime()) + Math.random() * 60_000) : null
    mockJobs.push({
      id: uuid(),
      type: template.type,
      queue: template.queue,
      status,
      priority: Math.floor(Math.random() * 100),
      attempt: status === 'failed' ? 3 : (status === 'running' ? 1 : (status === 'completed' ? 1 : 0)),
      maxRetries: 3,
      args: jsonBytes(template.args),
      lastError: status === 'failed' ? `Error: ${randomPick(['connection timeout', 'rate limit exceeded', 'invalid payload', 'upstream service unavailable'])}` : '',
      createdAt,
      startedAt,
      completedAt,
    })
  }
}

// Seed stats history (last 2 hours, per minute)
function seedStatsHistory() {
  const queues = ['default', 'emails', 'critical']
  for (let m = 120; m >= 0; m--) {
    const ts = minutesAgo(m)
    for (const q of queues) {
      statsHistory.push({
        timestamp: ts,
        completed: Math.floor(Math.random() * 8) + 1,
        failed: Math.random() < 0.15 ? Math.floor(Math.random() * 3) + 1 : 0,
        queue: q,
      })
    }
  }
}

seedJobs()
seedStatsHistory()

// --- Simulation Engine ---

let simulationStarted = false

function startSimulation() {
  if (simulationStarted) return
  simulationStarted = true

  setInterval(() => {
    // Move 1-2 pending -> running
    const pending = mockJobs.filter(j => j.status === 'pending')
    const toStart = pending.slice(0, Math.floor(Math.random() * 2) + 1)
    for (const j of toStart) {
      j.status = 'running'
      j.startedAt = new Date()
      j.attempt = 1
    }

    // Complete 1-2 running jobs
    const running = mockJobs.filter(j => j.status === 'running')
    const toComplete = running.slice(0, Math.floor(Math.random() * 2) + 1)
    for (const j of toComplete) {
      if (Math.random() < 0.1) {
        // Fail ~10%
        j.status = 'failed'
        j.lastError = `Error: ${randomPick(['connection timeout', 'rate limit exceeded', 'invalid payload', 'upstream service unavailable'])}`
        j.attempt = j.maxRetries
      } else {
        j.status = 'completed'
        j.completedAt = new Date()
      }
    }

    // Add 1-2 new pending jobs to keep pipeline flowing
    const newCount = Math.floor(Math.random() * 2) + 1
    for (let i = 0; i < newCount; i++) {
      const template = randomPick(JOB_TYPES)
      mockJobs.push({
        id: uuid(),
        type: template.type,
        queue: template.queue,
        status: 'pending',
        priority: Math.floor(Math.random() * 100),
        attempt: 0,
        maxRetries: 3,
        args: jsonBytes(template.args),
        lastError: '',
        createdAt: new Date(),
        startedAt: null,
        completedAt: null,
      })
    }

    // Keep array from growing unbounded (remove old completed jobs beyond 100)
    while (mockJobs.length > 100) {
      const idx = mockJobs.findIndex(j => j.status === 'completed')
      if (idx >= 0) mockJobs.splice(idx, 1)
      else break
    }

    // Add stats history data point
    const queues = ['default', 'emails', 'critical']
    for (const q of queues) {
      statsHistory.push({
        timestamp: new Date(),
        completed: toComplete.filter(j => j.status === 'completed' && j.queue === q).length + Math.floor(Math.random() * 3),
        failed: toComplete.filter(j => j.status === 'failed' && j.queue === q).length,
        queue: q,
      })
    }

    // Prune old history (keep last 8 hours)
    const cutoff = Date.now() - 8 * 60 * 60_000
    while (statsHistory.length > 0 && statsHistory[0].timestamp.getTime() < cutoff) {
      statsHistory.shift()
    }
  }, 3000)
}

// --- Mock Client Methods ---

function toProtoJob(j: MockJob): Job {
  return new Job({
    id: j.id,
    type: j.type,
    queue: j.queue,
    status: j.status,
    priority: j.priority,
    attempt: j.attempt,
    maxRetries: j.maxRetries,
    args: j.args,
    lastError: j.lastError,
    createdAt: tsFromDate(j.createdAt),
    startedAt: j.startedAt ? tsFromDate(j.startedAt) : undefined,
    completedAt: j.completedAt ? tsFromDate(j.completedAt) : undefined,
  })
}

function computeQueueStats(queueName: string): { pending: number; running: number; completed: number; failed: number } {
  const jobs = mockJobs.filter(j => j.queue === queueName)
  return {
    pending: jobs.filter(j => j.status === 'pending').length,
    running: jobs.filter(j => j.status === 'running').length,
    completed: jobs.filter(j => j.status === 'completed').length,
    failed: jobs.filter(j => j.status === 'failed').length,
  }
}

export const mockJobsClient = {
  async getStats(_req: unknown) {
    startSimulation()
    const queueNames = [...new Set(mockJobs.map(j => j.queue))]
    const queues = queueNames.map(name => {
      const s = computeQueueStats(name)
      return new QueueStats({
        name,
        pending: protoInt64.parse(s.pending),
        running: protoInt64.parse(s.running),
        completed: protoInt64.parse(s.completed),
        failed: protoInt64.parse(s.failed),
      })
    })
    const totals = { pending: 0, running: 0, completed: 0, failed: 0 }
    for (const q of queues) {
      totals.pending += Number(q.pending)
      totals.running += Number(q.running)
      totals.completed += Number(q.completed)
      totals.failed += Number(q.failed)
    }
    return new GetStatsResponse({
      queues,
      totalPending: protoInt64.parse(totals.pending),
      totalRunning: protoInt64.parse(totals.running),
      totalCompleted: protoInt64.parse(totals.completed),
      totalFailed: protoInt64.parse(totals.failed),
    })
  },

  async getStatsHistory(req: { period?: string; queue?: string }) {
    startSimulation()
    const periodMs = req.period === '7d' ? 7 * 24 * 60 * 60_000
      : req.period === '24h' ? 24 * 60 * 60_000
      : 60 * 60_000
    const since = Date.now() - periodMs
    const filtered = statsHistory.filter(s =>
      s.timestamp.getTime() >= since && (!req.queue || s.queue === req.queue)
    )

    // Bucket by minute
    const buckets = new Map<number, { completed: number; failed: number }>()
    for (const s of filtered) {
      const key = Math.floor(s.timestamp.getTime() / 60_000) * 60_000
      const existing = buckets.get(key) ?? { completed: 0, failed: 0 }
      existing.completed += s.completed
      existing.failed += s.failed
      buckets.set(key, existing)
    }

    const sorted = [...buckets.entries()].sort((a, b) => a[0] - b[0])
    return new GetStatsHistoryResponse({
      completed: sorted.map(([ts, v]) => new DataPoint({
        timestamp: tsFromDate(new Date(ts)),
        value: protoInt64.parse(v.completed),
      })),
      failed: sorted.map(([ts, v]) => new DataPoint({
        timestamp: tsFromDate(new Date(ts)),
        value: protoInt64.parse(v.failed),
      })),
    })
  },

  async listJobs(req: { status?: string; queue?: string; type?: string; search?: string; page?: number; limit?: number }) {
    startSimulation()
    let filtered = [...mockJobs]
    if (req.status) filtered = filtered.filter(j => j.status === req.status)
    if (req.queue) filtered = filtered.filter(j => j.queue === req.queue)
    if (req.type) filtered = filtered.filter(j => j.type === req.type)
    if (req.search) filtered = filtered.filter(j => j.id.includes(req.search!))

    filtered.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())

    const page = req.page || 1
    const limit = req.limit || 20
    const start = (page - 1) * limit
    const paged = filtered.slice(start, start + limit)

    return new ListJobsResponse({
      jobs: paged.map(toProtoJob),
      total: protoInt64.parse(filtered.length),
      page,
    })
  },

  async getJob(req: { id: string }) {
    startSimulation()
    const job = mockJobs.find(j => j.id === req.id)
    if (!job) return new GetJobResponse({})

    // Generate mock checkpoints for completed/failed workflow jobs
    const checkpoints: Checkpoint[] = []
    if (job.type === 'process-order' && (job.status === 'completed' || job.status === 'failed')) {
      checkpoints.push(new Checkpoint({
        id: uuid(),
        jobId: job.id,
        callIndex: 0,
        callType: 'charge-payment',
        result: jsonBytes({ receipt: 'rcpt_' + job.id.slice(0, 8) }),
        createdAt: job.startedAt ? tsFromDate(job.startedAt) : undefined,
      }))
      if (job.status === 'completed') {
        checkpoints.push(new Checkpoint({
          id: uuid(),
          jobId: job.id,
          callIndex: 1,
          callType: 'ship-order',
          result: jsonBytes({ trackingId: 'TRK-' + Math.floor(Math.random() * 10000) }),
          createdAt: job.completedAt ? tsFromDate(job.completedAt) : undefined,
        }))
      }
    }

    return new GetJobResponse({
      job: toProtoJob(job),
      checkpoints,
    })
  },

  async retryJob(req: { id: string }) {
    const job = mockJobs.find(j => j.id === req.id)
    if (job) {
      job.status = 'pending'
      job.attempt = 0
      job.lastError = ''
      job.completedAt = null
      job.startedAt = null
    }
    return new RetryJobResponse({ job: job ? toProtoJob(job) : undefined })
  },

  async deleteJob(req: { id: string }) {
    const idx = mockJobs.findIndex(j => j.id === req.id)
    if (idx >= 0) mockJobs.splice(idx, 1)
    return new DeleteJobResponse({})
  },

  async listQueues(_req: unknown) {
    startSimulation()
    const queueNames = [...new Set(mockJobs.map(j => j.queue))]
    return new ListQueuesResponse({
      queues: queueNames.map(name => {
        const s = computeQueueStats(name)
        return new QueueStats({
          name,
          pending: protoInt64.parse(s.pending),
          running: protoInt64.parse(s.running),
          completed: protoInt64.parse(s.completed),
          failed: protoInt64.parse(s.failed),
        })
      }),
    })
  },

  async purgeQueue(req: { name: string; status: string }) {
    const before = mockJobs.length
    const toRemove = mockJobs.filter(j => j.queue === req.name && j.status === req.status)
    for (const j of toRemove) {
      const idx = mockJobs.indexOf(j)
      if (idx >= 0) mockJobs.splice(idx, 1)
    }
    return new PurgeQueueResponse({ deleted: protoInt64.parse(before - mockJobs.length) })
  },

  async listScheduledJobs(_req: unknown) {
    const now = new Date()
    return new ListScheduledJobsResponse({
      jobs: [
        new ScheduledJobInfo({
          name: 'cleanup-expired',
          schedule: '0 * * * *',
          queue: 'default',
          nextRun: tsFromDate(new Date(now.getTime() + 42 * 60_000)),
          lastRun: tsFromDate(new Date(now.getTime() - 18 * 60_000)),
        }),
        new ScheduledJobInfo({
          name: 'daily-report',
          schedule: 'daily@09:00',
          queue: 'default',
          nextRun: tsFromDate(new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1, 9, 0)),
          lastRun: tsFromDate(new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 0)),
        }),
        new ScheduledJobInfo({
          name: 'weekly-backup',
          schedule: 'weekly@Sun/02:00',
          queue: 'critical',
          nextRun: tsFromDate(new Date(now.getTime() + 3 * 24 * 60 * 60_000)),
          lastRun: tsFromDate(new Date(now.getTime() - 4 * 24 * 60 * 60_000)),
        }),
        new ScheduledJobInfo({
          name: 'sync-inventory',
          schedule: 'every 5m',
          queue: 'critical',
          nextRun: tsFromDate(new Date(now.getTime() + 3 * 60_000)),
          lastRun: tsFromDate(new Date(now.getTime() - 2 * 60_000)),
        }),
      ],
    })
  },
}
```

**Step 2: Verify TypeScript compiles**

Run: `cd ui/frontend && npx tsc --noEmit src/lib/mock-client.ts`
Expected: No errors (or only errors from missing tsconfig resolution which is fine — Vite handles it)

**Step 3: Commit**

```bash
git add ui/frontend/src/lib/mock-client.ts
git commit -m "feat: add mock client with simulated job data for demo"
```

---

### Task 2: Wire Up Client Swap via Environment Variable

**Files:**
- Modify: `ui/frontend/src/lib/client.ts`

**Step 1: Update client.ts to conditionally export mock or real client**

Replace `ui/frontend/src/lib/client.ts` with:

```typescript
import { mockJobsClient } from './mock-client'

let jobsClient: typeof mockJobsClient

if (import.meta.env.VITE_DEMO_MODE === 'true') {
  jobsClient = mockJobsClient
} else {
  // Dynamic import avoided — use static import with tree-shaking
  const { createClient } = await import('@connectrpc/connect')
  const { createConnectTransport } = await import('@connectrpc/connect-web')
  const { JobsService } = await import('./gen/jobs/v1/jobs_connect')

  const transport = createConnectTransport({
    baseUrl: window.location.origin,
  })

  jobsClient = createClient(JobsService, transport) as typeof mockJobsClient
}

export { jobsClient }

// Re-export types for convenience
export type { Job, QueueStats, Checkpoint } from './gen/jobs/v1/jobs_pb'
```

**NOTE:** The top-level await works because the project targets ESM (`"type": "module"` in package.json) and Vite supports top-level await. However, if this causes issues, an alternative approach is to use Vite's `define` or `resolve.alias` to swap the entire module at build time. If the dynamic import approach causes issues at build time, fall back to the alias approach in `vite.config.ts`:

```typescript
// Alternative in vite.config.ts if top-level await doesn't work:
resolve: {
  alias: process.env.VITE_DEMO_MODE === 'true'
    ? { './lib/client': './lib/mock-client' }
    : {}
}
```

**Step 2: Verify the production build still works (non-demo)**

Run: `cd ui/frontend && npx vite build`
Expected: Build succeeds, outputs to `dist/`

**Step 3: Commit**

```bash
git add ui/frontend/src/lib/client.ts
git commit -m "feat: wire client swap for demo mode via VITE_DEMO_MODE env"
```

---

### Task 3: Add Demo Build Script

**Files:**
- Modify: `ui/frontend/package.json`

**Step 1: Add `build:demo` script**

Add to the `"scripts"` section in `package.json`:

```json
"build:demo": "VITE_DEMO_MODE=true vite build --outDir demo-dist --base /simple-durable-jobs/demo/"
```

**Step 2: Run the demo build**

Run: `cd ui/frontend && npm run build:demo`
Expected: Build succeeds, outputs to `ui/frontend/demo-dist/`

**Step 3: Verify the demo build outputs are correct**

Run: `ls ui/frontend/demo-dist/`
Expected: `index.html`, `assets/` directory with JS and CSS bundles

**Step 4: Add `demo-dist` to `.gitignore`**

Check if there's a `.gitignore` in `ui/frontend/` or at root and add `ui/frontend/demo-dist/` to it.

**Step 5: Commit**

```bash
git add ui/frontend/package.json .gitignore
git commit -m "feat: add build:demo script for GitHub Pages demo build"
```

---

### Task 4: Create Hugo Docs Page with Iframe

**Files:**
- Create: `docs/content/docs/live-demo.md`

**Step 1: Create the Hugo page**

Create `docs/content/docs/live-demo.md`:

```markdown
---
title: "Live Demo"
weight: 5
---

# Live Demo

Try the embedded web UI dashboard below. This is an interactive demo running entirely in your browser with simulated job data — no backend required.

Jobs move through states automatically (pending → running → completed/failed), and you can interact with all views: click jobs, filter, retry failed jobs, and browse queues.

<div style="border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; margin: 24px 0;">
  <div style="background: #1e293b; color: #94a3b8; padding: 8px 16px; font-size: 13px; display: flex; align-items: center; gap: 8px;">
    <span style="display: inline-block; width: 8px; height: 8px; background: #22c55e; border-radius: 50;"></span>
    Interactive Demo — Simulated Data
  </div>
  <iframe
    src="../demo/"
    style="width: 100%; height: 700px; border: none; display: block;"
    title="Jobs UI Demo"
    loading="lazy"
  ></iframe>
</div>

{{< hint info >}}
**Note:** This demo uses simulated data that updates every few seconds. In a real deployment, the dashboard connects to your Go application's backend via Connect-RPC. See [Embedded Web UI]({{< relref "embedded-ui" >}}) for setup instructions.
{{< /hint >}}
```

**Step 2: Verify Hugo renders it locally (optional)**

Run: `cd docs && hugo server`
Expected: New "Live Demo" page appears in the sidebar

**Step 3: Commit**

```bash
git add docs/content/docs/live-demo.md
git commit -m "docs: add live demo page with iframe for interactive UI demo"
```

---

### Task 5: Update GitHub Actions Workflow

**Files:**
- Modify: `.github/workflows/pages.yml`

**Step 1: Add Node.js setup and demo build steps**

Insert these steps in the `build` job, after the "Checkout" step and before "Build with Hugo":

```yaml
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
          cache-dependency-path: ui/frontend/package-lock.json

      - name: Build UI Demo
        working-directory: ui/frontend
        run: |
          npm ci
          npm run build:demo

      - name: Copy demo to docs static
        run: cp -r ui/frontend/demo-dist docs/static/demo
```

Also update the `paths` trigger to include `ui/frontend/**`:

```yaml
on:
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'README.md'
      - 'ui/frontend/**'
  workflow_dispatch:
```

**Step 2: Verify the workflow YAML is valid**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/pages.yml'))"`
If python3/yaml not available, just visually verify indentation.

**Step 3: Commit**

```bash
git add .github/workflows/pages.yml
git commit -m "ci: add demo build step to GitHub Pages workflow"
```

---

### Task 6: Test the Full Demo Locally

**Step 1: Build the demo**

Run: `cd ui/frontend && npm run build:demo`
Expected: `demo-dist/` created with `index.html` and `assets/`

**Step 2: Serve and test the demo in a browser**

Run: `cd ui/frontend && npx vite preview --outDir demo-dist --base /simple-durable-jobs/demo/`
Expected: Opens at `http://localhost:4173/simple-durable-jobs/demo/`

Verify manually:
- Dashboard shows 4 stats cards with numbers
- Stats update every few seconds (numbers change)
- Chart shows completed/failed lines
- Queue table shows 3 queues (default, emails, critical)
- Jobs page lists ~20 jobs with status badges
- Clicking a job shows detail view with metadata
- Queues page shows per-queue breakdowns
- Scheduled page shows 4 scheduled jobs
- Retry/delete actions work (modify local data)

**Step 3: Test Hugo integration locally**

Run:
```bash
cp -r ui/frontend/demo-dist docs/static/demo
cd docs && hugo server
```
Expected: "Live Demo" page shows iframe with working demo

**Step 4: Clean up and commit any fixes**

If any issues found, fix them and commit.

---

### Task 7: Final Commit and Cleanup

**Step 1: Verify all files are tracked**

Run: `git status`
Expected: All new/modified files staged or committed

**Step 2: Ensure demo-dist is gitignored**

Verify `ui/frontend/demo-dist/` and `docs/static/demo/` are in `.gitignore` (they are build artifacts, not source).

**Step 3: Clean up docs/static/demo if present**

Run: `rm -rf docs/static/demo` (this is only needed during local testing)

**Step 4: Final commit if needed**

```bash
git add -A
git commit -m "chore: finalize demo build setup and gitignore"
```
