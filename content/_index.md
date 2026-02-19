---
title: Simple Durable Jobs
layout: hextra-home
description: "A Go library for durable job queues with checkpointed workflows, fan-out/fan-in, crash recovery, and an embedded web dashboard."
---

<div class="hx:mt-8 hx:mb-8">
{{< hextra/hero-headline >}}
  Simple Durable Jobs
{{< /hextra/hero-headline >}}
</div>

<div class="hx:mb-10">
{{< hextra/hero-subtitle >}}
  A Go library for durable job queues with checkpointed workflows.&nbsp;<br class="hx:hidden sm:hx:block" />Background jobs, fan-out/fan-in, crash recovery, and an embedded web dashboard.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx:mb-16">
{{< hextra/hero-button text="Get Started" link="docs/getting-started/" >}}
</div>

{{< hextra/feature-grid >}}
  {{< hextra/feature-card
    title="Durable Workflows"
    subtitle="Multi-step workflows with automatic checkpointing. If a workflow fails, it resumes from the last successful step."
  >}}
  {{< hextra/feature-card
    title="Fan-Out/Fan-In"
    subtitle="Spawn parallel sub-jobs, wait for results, and aggregate. Supports fail-fast, collect-all, and threshold strategies."
  >}}
  {{< hextra/feature-card
    title="Crash Recovery"
    subtitle="Jobs persist to a database and survive crashes. Stale lock reaper automatically reclaims stuck jobs."
  >}}
  {{< hextra/feature-card
    title="Embedded Web UI"
    subtitle="Real-time monitoring dashboard with stats, historical charts, live event streaming, and job management."
  >}}
  {{< hextra/feature-card
    title="Scheduled Jobs"
    subtitle="Cron expressions, daily, weekly, and interval-based scheduling with a built-in scheduler."
  >}}
  {{< hextra/feature-card
    title="Simple API"
    subtitle="Minimal boilerplate, type-safe handlers, and a clean facade. Import one package and start processing jobs."
  >}}
{{< /hextra/feature-grid >}}
