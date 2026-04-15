---
title: "Live Demo"
weight: 5
---

Try the embedded web UI dashboard below. This is an interactive demo running entirely in your browser with simulated job data — no backend required. Jobs move through their lifecycle automatically (pending → running → completed/failed), and every panel is interactive: filter, retry failed jobs, browse queues, inspect events.

<style>
  /* Break out of the Hextra content column so the dashboard has room
     to breathe. The default doc column caps around ~768px, which
     squashes a 700px-wide UI into a cramped scroll box. */
  .sdj-demo-breakout {
    position: relative;
    left: 50%;
    right: 50%;
    margin-left: -50vw;
    margin-right: -50vw;
    width: 100vw;
    max-width: none;
  }
  .sdj-demo-inner {
    max-width: 1600px;
    margin: 0 auto;
    padding: 0 16px;
  }
  .sdj-demo-frame {
    border: 1px solid rgba(148, 163, 184, 0.35);
    border-radius: 8px;
    overflow: hidden;
    background: #0f172a;
  }
  .sdj-demo-chrome {
    background: #1e293b;
    color: #94a3b8;
    padding: 8px 16px;
    font-size: 13px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }
  .sdj-demo-chrome .sdj-demo-status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  .sdj-demo-chrome .sdj-demo-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    background: #22c55e;
    border-radius: 50%;
  }
  .sdj-demo-chrome a {
    color: #e2e8f0;
    text-decoration: none;
    font-weight: 500;
    padding: 4px 10px;
    border-radius: 5px;
    border: 1px solid rgba(148, 163, 184, 0.35);
    background: rgba(15, 23, 42, 0.35);
  }
  .sdj-demo-chrome a:hover {
    background: rgba(15, 23, 42, 0.75);
  }
  .sdj-demo-iframe {
    width: 100%;
    height: min(900px, 85vh);
    border: none;
    display: block;
    background: #fff;
  }
</style>

<div class="sdj-demo-breakout">
  <div class="sdj-demo-inner">
    <div class="sdj-demo-frame">
      <div class="sdj-demo-chrome">
        <span class="sdj-demo-status">
          <span class="sdj-demo-dot"></span>
          Interactive Demo — Simulated Data
        </span>
        <a href="/simple-durable-jobs/demo/" target="_blank" rel="noopener">Open full-screen ↗</a>
      </div>
      <iframe
        class="sdj-demo-iframe"
        src="/simple-durable-jobs/demo/"
        title="Simple Durable Jobs UI Demo"
        loading="lazy"
      ></iframe>
    </div>
  </div>
</div>

{{< callout type="info" >}}
**Note:** This demo uses simulated data that updates every few seconds. In a real deployment, the dashboard connects to your Go application's backend via Connect-RPC. See [Embedded Web UI]({{< relref "embedded-ui" >}}) for setup instructions.
{{< /callout >}}
