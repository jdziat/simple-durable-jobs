---
title: Documentation
weight: 1
toc: false
---

Welcome to the **Simple Durable Jobs** documentation — a Go library for durable job queues with checkpointed workflows, fan-out/fan-in, crash recovery, and an embedded web dashboard.

{{< callout type="info" >}}
**New here?** Start with [Getting Started]({{< relref "getting-started" >}}) — you'll have a worker processing jobs in about five minutes.
{{< /callout >}}

## Guides

{{< cards >}}
  {{< card link="getting-started/" title="Getting Started" icon="play" subtitle="Install the library, wire up storage, register handlers, enqueue jobs, and run a worker." >}}
  {{< card link="examples/" title="Examples" icon="code" subtitle="Copy-paste-runnable examples: workflows, fan-out/fan-in, scheduling, priorities, error handling, and more." >}}
  {{< card link="embedded-ui/" title="Embedded Web UI" icon="chart-bar" subtitle="Mount the real-time monitoring dashboard into your own HTTP server." >}}
  {{< card link="live-demo/" title="Live Demo" icon="play" subtitle="Try the dashboard in your browser on simulated data — no backend required." >}}
{{< /cards >}}

## Reference

{{< cards >}}
  {{< card link="api-reference/" title="API Reference" icon="book-open" subtitle="Complete type and method reference: Queue, Worker, durable Calls, fan-out, options, events, and storage." >}}
  {{< card link="advanced/" title="Advanced Topics" icon="cog" subtitle="Production features: transactional enqueue, dead-letter queue, retention/GC, rate limiting, concurrency caps, metrics, workflow versioning, payload codec, dashboard authorization, testing utilities, and tuning knobs." >}}
{{< /cards >}}
