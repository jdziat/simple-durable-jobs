---
title: "Live Demo"
layout: fullscreen
toc: false
sitemap:
  disable: true
---

<style>
  /* The fullscreen layout strips the Hextra max-width wrapper, so this
     iframe fills the viewport below the navbar. */
  .sdj-fullscreen-demo {
    display: block;
    width: 100vw;
    height: calc(100vh - var(--navbar-height, 64px));
    border: 0;
    margin: 0;
  }
  .sdj-fullscreen-backlink {
    position: fixed;
    top: calc(var(--navbar-height, 64px) + 12px);
    right: 16px;
    z-index: 10;
    padding: 6px 12px;
    background: rgba(15, 23, 42, 0.85);
    color: #f1f5f9;
    font-size: 13px;
    font-family: ui-sans-serif, system-ui, sans-serif;
    text-decoration: none;
    border-radius: 6px;
    border: 1px solid rgba(148, 163, 184, 0.25);
  }
  .sdj-fullscreen-backlink:hover {
    background: rgba(15, 23, 42, 0.95);
  }
</style>

<a class="sdj-fullscreen-backlink" href="{{< relref "/docs/live-demo" >}}">← back to docs</a>

<iframe
  class="sdj-fullscreen-demo"
  src="/simple-durable-jobs/demo/"
  title="Simple Durable Jobs — Live Dashboard"
  loading="lazy"
></iframe>
