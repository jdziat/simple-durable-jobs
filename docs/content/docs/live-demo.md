---
title: "Live Demo"
weight: 5
---

# Live Demo

Try the embedded web UI dashboard below. This is an interactive demo running entirely in your browser with simulated job data -- no backend required.

Jobs move through states automatically (pending, running, completed, failed), and you can interact with all views: click jobs, filter, retry failed jobs, and browse queues.

<div style="border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; margin: 24px 0;">
  <div style="background: #1e293b; color: #94a3b8; padding: 8px 16px; font-size: 13px; display: flex; align-items: center; gap: 8px;">
    <span style="display: inline-block; width: 8px; height: 8px; background: #22c55e; border-radius: 50%;"></span>
    Interactive Demo — Simulated Data
  </div>
  <iframe
    src="/simple-durable-jobs/demo/"
    style="width: 100%; height: 700px; border: none; display: block;"
    title="Jobs UI Demo"
    loading="lazy"
  ></iframe>
</div>

{{< hint info >}}
**Note:** This demo uses simulated data that updates every few seconds. In a real deployment, the dashboard connects to your Go application's backend via Connect-RPC. See [Embedded Web UI]({{< relref "embedded-ui" >}}) for setup instructions.
{{< /hint >}}
