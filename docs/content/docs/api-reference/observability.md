---
title: "Observability"
weight: 10
---

## OpenTelemetry Tracing

### `otel.Instrument(q *Queue, opts ...otel.Option)`

Imports:

```go
import jobsotel "github.com/jdziat/simple-durable-jobs/v3/pkg/otel"
```

Attaches tracing to a queue with no hand-rolled hook wiring. By default the instrumentation uses the global `TracerProvider`; pass `jobsotel.WithTracerProvider(tp)` to override.

```go
jobsotel.Instrument(queue) // uses otel.GetTracerProvider()
// or
jobsotel.Instrument(queue, jobsotel.WithTracerProvider(tp))
```

Spans emitted:

| Span | When |
|---|---|
| `job.enqueue` | `Queue.Enqueue` — producer span; trace context is persisted on the job so the consumer picks it up. |
| `job.process {type}` | Worker handler execution — child of `job.enqueue`. |
| `job.retry` | Span event added to `job.process` when a job is retried. |

Every span carries `job.id`, `job.type`, `job.queue`, `job.attempt`, and `job.priority` attributes.
